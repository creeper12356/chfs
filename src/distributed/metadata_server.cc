#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
  server_->bind("set_file_size",
                [this](inode_id_t id, u64 size) { return this->set_file_size(id, size); });
  server_->bind("get_block_size", [this]() {
    return this->operation_->block_manager_->block_size();
  });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

auto MetadataServer::poll_allocate_block(inode_id_t id) -> BlockInfo
{
  for(auto client_mapping: clients_) {
    auto allocated_block_info = client_mapping.second->call("alloc_block").unwrap()->as<std::pair<block_id_t, version_t>>();
    if(allocated_block_info.first == KInvalidBlockID) {
      continue;
    }

    return BlockInfo(allocated_block_info.first, client_mapping.first, allocated_block_info.second);
  }

  return {};
}

auto MetadataServer::handle_last_direct_block_not_full(BlockInfoStruct *last_direct_block_info_arr, BlockInfoStruct block_info_struct, u8 *last_direct_block_data, block_id_t last_direct_block_id) -> bool
{
  // 未满
  auto next_block_info_idx = 0;
  while(last_direct_block_info_arr[next_block_info_idx].block_id != KInvalidBlockID) {
    ++ next_block_info_idx;
  }
  // 直接将返回的信息写入到next_block_info_idx对应的位置
  last_direct_block_info_arr[next_block_info_idx] = block_info_struct;
  auto write_last_direct_block_res = operation_->block_manager_->write_block(last_direct_block_id, last_direct_block_data);
  if(write_last_direct_block_res.is_err()) {
    return false;
  }

  return true;
}

auto MetadataServer::handle_last_direct_block_full(BlockInfoStruct block_info_struct, Inode *inode_p, u8* inode_data, block_id_t inode_bid, usize last_direct_block_id_idx,bool need_indirect) -> bool
{
  auto block_size = operation_->block_manager_->block_size();
  auto inode_nblocks = inode_p->get_nblocks();

  // 分配一个新的direct block
  auto allocate_next_direct_block_res = operation_->block_allocator_->allocate();
  if(allocate_next_direct_block_res.is_err()) {
    return false;
  }
  auto next_direct_block_id = allocate_next_direct_block_res.unwrap();

  // 写入新的block信息
  std::vector<u8> next_direct_block(block_size, 0);
  reinterpret_cast<BlockInfoStruct *>(next_direct_block.data())[0] = block_info_struct;
  auto write_next_direct_block_res = operation_->block_manager_->write_block(next_direct_block_id, next_direct_block.data());
  if(write_next_direct_block_res.is_err()) {
    return false;
  }

  // if(last_direct_block_id_idx == inode_nblocks - 2) {
    if(need_indirect) {
    // inode中所有的direct block对应的空间都已满，需要分配indirect block
    // 分配next direct block，类似上一种情况的逻辑
    auto allocate_next_direct_block_res = operation_->block_allocator_->allocate();
    if(allocate_next_direct_block_res.is_err()) {
      return false;
    }
    auto next_direct_block_id = allocate_next_direct_block_res.unwrap();

    // 写入新的block信息
    std::vector<u8> next_direct_block(block_size, 0);
    reinterpret_cast<BlockInfoStruct *>(next_direct_block.data())[0] = block_info_struct;
    auto write_next_direct_block_res = operation_->block_manager_->write_block(next_direct_block_id, next_direct_block.data());
    if(write_next_direct_block_res.is_err()) {
      return false;
    }

    // 分配一个indirect block
    auto allocate_indirect_block_res = inode_p->get_or_insert_indirect_block(operation_->block_allocator_);
    if(allocate_indirect_block_res.is_err()) {
      return false;
    }
    auto indirect_block_id = allocate_indirect_block_res.unwrap();

    // 将next direct block的block id写入到indirect block中
    std::vector<u8> indirect_block(block_size);
    reinterpret_cast<block_id_t *>(indirect_block.data())[0] = next_direct_block_id;
    auto write_indirect_block_res = operation_->block_manager_->write_block(indirect_block_id, indirect_block.data());
    if(write_indirect_block_res.is_err()) {
      return false;
    }

    // 将新分配的indirect block的block id写入到inode中
    inode_p->blocks[inode_nblocks - 1] = indirect_block_id;
    auto write_inode_res = operation_->block_manager_->write_block(inode_bid, inode_data);
    if(write_inode_res.is_err()) {
      return false;
    }

  } else {
    // 写入一个新的direct block中，只需要修改inode中的指针
    inode_p->blocks[last_direct_block_id_idx + 1] = next_direct_block_id;
    auto write_inode_res = operation_->block_manager_->write_block(inode_bid, inode_data);
    if(write_inode_res.is_err()) {
      return false;
    }
  }

  return true;
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled)
{
    server_ = std::make_unique<RpcServer>(port);
    init_fs(data_path);
    if (is_log_enabled_)
    {
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                 is_checkpoint_enabled);
    }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  std::lock_guard<std::mutex> lock(global_lock_);

  if(is_log_enabled_) {
    operation_->block_manager_->set_log_enabled(true);
    operation_->block_manager_->set_txn_id(operation_->block_manager_->get_txn_id() + 1);
  }

  auto res = mknode_atomic(type, parent, name);

  if(is_log_enabled_) {
    operation_->block_manager_->set_log_enabled(false);
    auto recover_res = commit_log->recover_with_ret();
    if(recover_res) {
      // 只有成功落盘之后才清空日志
      commit_log->clean();
    } else {
      res = KInvalidInodeID;
    }
  }

  return res;
}


auto MetadataServer::mknode_atomic(u8 type, inode_id_t parent, const std::string &name) 
    -> inode_id_t {
  auto mk_res = operation_->mk_helper(parent, name.c_str(), static_cast<InodeType>(type));
  auto res = mk_res.is_err() ? KInvalidInodeID : mk_res.unwrap();
  return res;
}
// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
    std::lock_guard<std::mutex> lock(global_lock_);

    if(is_log_enabled_) {
      operation_->block_manager_->set_log_enabled(true);
      operation_->block_manager_->set_txn_id(operation_->block_manager_->get_txn_id() + 1);
    }

    auto res = unlink_atomic(parent, name);

    if(is_log_enabled_) {
      operation_->block_manager_->set_log_enabled(false);
      auto recover_res = commit_log->recover_with_ret();
      if(recover_res) {
        // 只有成功落盘之后才清空日志
        commit_log->clean();
      } else {
        res = KInvalidInodeID;
      }
    }

    return res;
}

auto MetadataServer::unlink_atomic(inode_id_t parent, const std::string &name) -> bool {
   // NOTE: logic similar to FileOperation::unlink, but different
    // 1. Remove the file, you can use the function `remove_file`
    auto child = lookup(parent, name);
    if (child == KInvalidInodeID)
    {
      // file with name `name` do not exist
      std::cout << "File with name " << name << " do not exist." << std::endl;
      return false;
    }
    remove_file(child);

    // 2. Remove the entry from the directory.
    auto read_dir_file_res = operation_->read_file(parent);
    if (read_dir_file_res.is_err())
    {
      return false;
    }
    auto buffer = read_dir_file_res.unwrap();
    auto dir_src = std::string(buffer.begin(), buffer.end());
    dir_src = rm_from_directory(dir_src, name);

    auto write_dir_file_res = operation_->write_file(parent, std::vector<u8>(dir_src.begin(), dir_src.end()));
    if (write_dir_file_res.is_err())
    {
      return false;
    }
    return true;
}

auto MetadataServer::remove_file(inode_id_t id) -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  std::vector<block_id_t> free_set;
  block_id_t inode_bid;
  auto cal_free_set_res = operation_->cal_free_set(id, free_set, inode_bid);
  if(cal_free_set_res.is_err()) {
    // Error encountered when calculating free set
    return cal_free_set_res;
  }

  // 计算block map并释放远程block
  auto block_map = get_block_map(id);
  for(auto block_info: block_map) {
    auto res = clients_[std::get<1>(block_info)]->call("free_block", std::get<0>(block_info));
    if(res.is_err()) {
      error_code = res.unwrap_error();
      return ChfsNullResult(error_code);
    }
  }

  // free inode first
  auto res = operation_->inode_manager_->free_inode(id);
    if (res.is_err()) {
      error_code = res.unwrap_error();
      goto err_ret;
  }

  // free inode block 
  res = operation_->block_allocator_->deallocate(inode_bid);
  if(res.is_err()) {
    error_code = res.unwrap_error();
    goto err_ret;
  }

  // free the blocks in free_set (rpc)
  for (auto mac_block_id : free_set) {
    // TODO: update mac_id and bid
    auto mac_id = mac_block_id;
    auto bid = mac_block_id;
    auto res = clients_[mac_id]->call("free_block", bid);
    if (res.is_err()) {
      error_code = res.unwrap_error();
      goto err_ret;
    }

    // TODO: replace this replace statement
    assert(res.unwrap() -> as<bool>());
  }
  return KNullOk;

err_ret:
  return ChfsNullResult(error_code);
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  auto lookup_res = operation_->lookup(parent, name.c_str());
  if(lookup_res.is_err()) {
    return KInvalidInodeID;
  }
  return lookup_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  auto block_size = operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());

  // 获取inode对应的block id
  auto inode_bid_res = operation_->inode_manager_->get(id);
  if(inode_bid_res.is_err()) {
    return {};
  }
  auto inode_bid = inode_bid_res.unwrap();

  // 读取inode block
  auto read_inode_res = operation_->block_manager_->read_block(inode_bid, inode.data());
  if(read_inode_res.is_err()) {
    return {};
  }

  std::vector<BlockInfo> block_map;
  auto inode_nblocks = inode_p->get_nblocks();
  auto block_buffer = std::vector<u8> (block_size);
  auto block_info_arr = reinterpret_cast<BlockInfoStruct *>(block_buffer.data());
  auto block_info_arr_size = block_size / sizeof(BlockInfoStruct);

  // direct block part
  for(usize i = 0; i < inode_nblocks - 1; ++i) {
    if(inode_p->blocks[i] == KInvalidBlockID) {
      continue;
    }

    auto read_block_res = operation_->block_manager_->read_block(inode_p->blocks[i], block_buffer.data());
    if(read_block_res.is_err()) {
      return {};
    }

    for(usize j = 0; j < block_info_arr_size; ++j) {
      if(block_info_arr[j].block_id == KInvalidBlockID) {
        continue;
      }
      block_map.push_back({block_info_arr[j].block_id, block_info_arr[j].mac_id, block_info_arr[j].version});
    }
  }

  // indirect block part
  if(inode_p->blocks[inode_nblocks - 1] == KInvalidBlockID) {
    return block_map;
  }
  auto indirect_block_id = inode_p->get_indirect_block_id();
  indirect_block.resize(block_size);
  auto read_indirect_block_res = operation_->block_manager_->read_block(indirect_block_id, indirect_block.data());
  if(read_indirect_block_res.is_err()) {
    return {};
  }
  // 重新解释indirect block的内容，读取存储的block id
  auto indirect_block_p = reinterpret_cast<block_id_t *>(indirect_block.data());
  // 每个indirect block存储的block id数量
  auto indirect_block_size = block_size / sizeof(block_id_t);
  for(usize i = 0;i < indirect_block_size; ++i) {
    if(indirect_block_p[i] == KInvalidBlockID) {
      continue;
    }
    // NOTE: 此处逻辑和direct block部分相同
    // 考虑到代码重复，可以将这部分逻辑提取为一个函数
    auto read_block_res = operation_->block_manager_->read_block(indirect_block_p[i], block_buffer.data());
    if(read_block_res.is_err()) {
      return {};
    }
    for(usize j = 0; j < block_info_arr_size; ++j) {
      if(block_info_arr[j].block_id == KInvalidBlockID) {
        continue;
      }
      block_map.push_back({block_info_arr[j].block_id, block_info_arr[j].mac_id, block_info_arr[j].version});
    }
  }

  return block_map;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  std::lock_guard<std::mutex> lock(global_lock_);

  // 向所有的data server发送请求，分配一个新的block
  auto poll_allocate_block_info = poll_allocate_block(id);
  if(std::get<0>(poll_allocate_block_info) == KInvalidBlockID) {
    return poll_allocate_block_info;
  }
  
  // 便于可读性
  auto poll_allocate_block_info_struct = BlockInfoStruct {
    std::get<0>(poll_allocate_block_info),
    std::get<1>(poll_allocate_block_info),
    std::get<2>(poll_allocate_block_info)
  };

  // 读取inode block
  auto block_size = operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto inode_bid_res = operation_->inode_manager_->get(id);
  if(inode_bid_res.is_err()) {
    return {};
  }
  auto inode_bid = inode_bid_res.unwrap();
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto read_inode_res = operation_->block_manager_->read_block(inode_bid, inode.data());
  if(read_inode_res.is_err()) {
    return {};
  }

  auto inode_nblocks = inode_p->get_nblocks();
  // 分成5种情况进行讨论：
  // - indirect block未使用，最后一个direct block指向的block未满
  // - indirect block未使用，最后一个direct block指向的block已满，且已经没有direct block可以使用
  // - indirect block未使用，最后一个direct block指向的block已满，但还有direct block可以使用
  // - indirect block已使用，indirect block中最后一个direct block指向的block未满
  // - indirect block已使用，indirect block中最后一个direct block指向的block已满
  if(inode_p->blocks[inode_nblocks - 1] == KInvalidBlockID) {
    // indirect block未使用
    // 读取最后一个direct block
    auto last_direct_block_id_idx = 0;
    while(inode_p->blocks[last_direct_block_id_idx] != KInvalidBlockID) {
      ++ last_direct_block_id_idx;
    }
    -- last_direct_block_id_idx;
    if(last_direct_block_id_idx == -1) {
      // 没有direct block可以使用，相当于
      // 已满的情况，需要分配一个新的direct block
      auto res = handle_last_direct_block_full(
        poll_allocate_block_info_struct,
        inode_p,
        inode.data(),
        inode_bid,
        last_direct_block_id_idx,
        false
      );
      if(!res) {
        return {};
      }

    } else {
      // 读取当前最后一个direct block,
      // 判断最后一个direct block指向的block是否已满
      auto last_direct_block_id = inode_p->blocks[last_direct_block_id_idx];
      std::vector<u8> last_direct_block(block_size);
      auto read_last_direct_block_res = operation_->block_manager_->read_block(last_direct_block_id, last_direct_block.data());
      if(read_last_direct_block_res.is_err()) {
        return {};
      }

      auto last_direct_block_info_arr = reinterpret_cast<BlockInfoStruct *>(last_direct_block.data());
      auto last_direct_block_info_arr_size = block_size / sizeof(BlockInfoStruct);
      if(last_direct_block_info_arr[last_direct_block_info_arr_size - 1].block_id == KInvalidBlockID) {
        // 未满
        auto res = handle_last_direct_block_not_full(
          last_direct_block_info_arr,
          poll_allocate_block_info_struct,
          last_direct_block.data(),
          last_direct_block_id
        );
        if(!res) {
          return {};
        }

      } else {
        // 已满
        auto res = handle_last_direct_block_full(
          poll_allocate_block_info_struct,
          inode_p,
          inode.data(),
          inode_bid,
          last_direct_block_id_idx,
          last_direct_block_id_idx == inode_nblocks - 2
        );
        if(!res) {
          return {};
        }
      }
    }
    
  } else {
    // indirect block已使用
    // 读取indirect block
    std::vector<u8> indirect_block(block_size);
    auto indirect_block_id = inode_p->get_indirect_block_id();
    auto read_indirect_block_res = operation_->block_manager_->read_block(indirect_block_id, indirect_block.data());
    if(read_indirect_block_res.is_err()) {
      return {};
    }

    // 在indirect block中找到最后一个使用的direct block
    auto indirect_block_p = reinterpret_cast<block_id_t *>(indirect_block.data());
    auto indirect_block_size = block_size / sizeof(block_id_t);
    auto last_direct_block_id_idx = 0;
    while(indirect_block_p[last_direct_block_id_idx] != KInvalidBlockID) {
      ++ last_direct_block_id_idx;
    }
    -- last_direct_block_id_idx;

    auto last_direct_block_id = indirect_block_p[last_direct_block_id_idx];
    auto last_direct_block = std::vector<u8>(block_size);

    // 判断最后一个direct block指向的block是否已满，
    // NOTE: 逻辑与direct block部分相同
    auto last_direct_block_info_arr = reinterpret_cast<BlockInfoStruct *>(last_direct_block.data());
    auto last_direct_block_info_arr_size = block_size / sizeof(BlockInfoStruct);
    if(last_direct_block_info_arr[last_direct_block_info_arr_size - 1].block_id == KInvalidBlockID) {
      auto res = handle_last_direct_block_not_full(
        last_direct_block_info_arr,
        poll_allocate_block_info_struct,
        last_direct_block.data(),
        last_direct_block_id
      );
      if(!res) {
        return {};
      }
      
    } else {
      // 已满
      if(last_direct_block_id_idx == indirect_block_size - 1) {
        // 达到最大文件大小，直接失败
        return {};
      }

      // 分配一个新的direct block
      auto allocate_next_direct_block_res = operation_->block_allocator_->allocate();
      if(allocate_next_direct_block_res.is_err()) {
        return {};
      }
      auto next_direct_block_id = allocate_next_direct_block_res.unwrap();

      // 写入新的block信息
      std::vector<u8> next_direct_block(block_size, 0);
      reinterpret_cast<BlockInfoStruct *>(next_direct_block.data())[0] = poll_allocate_block_info_struct;
      auto write_next_direct_block_res = operation_->block_manager_->write_block(next_direct_block_id, next_direct_block.data());
      if(write_next_direct_block_res.is_err()) {
        return {};
      }

      // 写入一个新的direct block中，只需要修改indirect block中的指针
      // NOTE: 上面情况是修改inode中的指针，类似但是不同
      indirect_block_p[last_direct_block_id_idx + 1] = next_direct_block_id;
      auto write_indirect_block_res = operation_->block_manager_->write_block(indirect_block_id, indirect_block.data());
      if(write_indirect_block_res.is_err()) {
        return {};
      }
    }

  }
  return poll_allocate_block_info;
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  std::lock_guard<std::mutex> lock(global_lock_);
                          
  // 调用对应的data server释放block
  auto res = clients_[machine_id]->call("free_block", block_id);
  if(!res.unwrap()->as<bool>()) {
    return false;
  }

  // NOTE: naive实现，只保证正确性，不保证碎片
  // 读取inode block
  auto block_size = operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto inode_bid_res = operation_->inode_manager_->get(id);
  if(inode_bid_res.is_err()) {
    return false;
  }
  auto inode_bid = inode_bid_res.unwrap();
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto read_inode_res = operation_->block_manager_->read_block(inode_bid, inode.data());
  if(read_inode_res.is_err()) {
    return false;
  }

  // 在direct block id指向的block中查找<block_id, mac_id> 
  auto inode_nblocks = inode_p->get_nblocks();
  for(usize i = 0;i < inode_nblocks - 1; ++i) {
    if(inode_p->blocks[i] == KInvalidBlockID) {
      continue;
    }

    // 读取direct block内容
    auto direct_block_id = inode_p->blocks[i];
    auto direct_block = std::vector<u8>(block_size);
    auto read_direct_block_res = operation_->block_manager_->read_block(direct_block_id, direct_block.data());
    if(read_direct_block_res.is_err()) {
      return false;
    }

    // 查找<block_id, mac_id>
    auto direct_block_info_arr = reinterpret_cast<BlockInfoStruct *>(direct_block.data());
    auto direct_block_info_arr_size = block_size / sizeof(BlockInfoStruct);
    for(usize j = 0;j < direct_block_info_arr_size; ++j) {
      if(direct_block_info_arr[j].block_id == block_id && 
        direct_block_info_arr[j].mac_id == machine_id) {
          // 找到需要释放的block记录
          // 将该记录置为无效
          direct_block_info_arr[j] = {KInvalidBlockID, 0, 0};
          auto write_direct_block_res = operation_->block_manager_->write_block(direct_block_id, direct_block.data());
          if(write_direct_block_res.is_err()) {
            return false;
          }
          return true;
        }
    }
  }

  // 在indirect block中查找<block_id, mac_id>
  if(inode_p->blocks[inode_nblocks - 1] == KInvalidBlockID) {
    return false;
  }
  auto indirect_block_id = inode_p->get_indirect_block_id();
  auto indirect_block = std::vector<u8>(block_size);
  auto read_indirect_block_res = operation_->block_manager_->read_block(indirect_block_id, indirect_block.data());
  if(read_indirect_block_res.is_err()) {
    return false;
  }
  auto indirect_block_p = reinterpret_cast<block_id_t *>(indirect_block.data());
  auto indirect_block_size = block_size / sizeof(block_id_t);

  for(usize i = 0;i < indirect_block_size; ++i) {
    if(indirect_block_p[i] == KInvalidBlockID) {
      continue;
    }

    auto direct_block_id = indirect_block_p[i];
    auto direct_block = std::vector<u8>(block_size);
    auto read_direct_block_res = operation_->block_manager_->read_block(direct_block_id, direct_block.data());
    if(read_direct_block_res.is_err()) {
      return false;
    }

    // 查找<block_id, mac_id>
    // NOTE: 逻辑与direct block部分相同
    auto direct_block_info_arr = reinterpret_cast<BlockInfoStruct *>(direct_block.data());
    auto direct_block_info_arr_size = block_size / sizeof(BlockInfoStruct);
    for(usize j = 0; j < direct_block_info_arr_size; ++j) {
      if(direct_block_info_arr[j].block_id == block_id && 
        direct_block_info_arr[j].mac_id == machine_id) {
          // 找到需要释放的block记录
          // 将该记录置为无效
          direct_block_info_arr[j] = {KInvalidBlockID, 0, 0};
          auto write_direct_block_res = operation_->block_manager_->write_block(direct_block_id, direct_block.data());
          if(write_direct_block_res.is_err()) {
            return false;
          }
          return true;
        }
    }
  }

  return false;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  auto read_res = operation_->read_file(node);
  if(read_res.is_err()) {
    return {};
  }
  
  auto file_content = read_res.unwrap();
  auto dir_content = std::string(file_content.begin(), file_content.end());
  std::list<DirectoryEntry> dir_entry_list;
  std::vector<std::pair<std::string, inode_id_t>> dir_entry_vec;
  parse_directory(dir_content, dir_entry_list);
  for(auto dir_entry: dir_entry_list) {
    dir_entry_vec.push_back({dir_entry.name, dir_entry.id});
  }

  return dir_entry_vec;
}

// {Your code here}
//  @return: a tuple of <size, atime, mtime, ctime, type>

auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  auto inode_bid_res = operation_->inode_manager_->get(id);
  if(inode_bid_res.is_err()) {
    std::cerr << "Cannot get inode block id." << std::endl;
    return {};
  }
  
  auto inode_bid = inode_bid_res.unwrap();
  auto block_size = operation_->block_manager_->block_size();

  std::vector<u8> inode_buffer(block_size);
  auto read_inode_res = operation_->block_manager_->read_block(inode_bid, inode_buffer.data());
  if(read_inode_res.is_err()) {
    std::cerr << "Cannot read inode block." << std::endl;
    return {};
  }

  // NOTE: 理论上应该在创建和写文件时更新inode_attr.size
  // 但是此处为了方便，直接计算文件大小并返回 :(
  auto inode_p = reinterpret_cast<Inode *>(inode_buffer.data());
  auto inode_attr = inode_p->get_attr();
  // auto inode_attr_size = inode_p->get_size();

  // inode_attr_size = get_block_map(id).size() * block_size;

  // NOTE: lab4又变成直接返回文件大小，可能导致之前的测试无法通过
  return std::make_tuple(
    inode_attr.size,
    inode_attr.atime,
    inode_attr.mtime,
    inode_attr.ctime,
    static_cast<u8>(inode_p->get_type())
  );
}

auto MetadataServer::set_file_size(inode_id_t id, u64 size) -> bool {
  auto inode_bid_res = operation_->inode_manager_->get(id);
  if(inode_bid_res.is_err()) {
    return false;
  }
  auto inode_bid = inode_bid_res.unwrap();
  auto block_size = operation_->block_manager_->block_size();

  std::vector<u8> inode_buffer(block_size);
  auto read_inode_res = operation_->block_manager_->read_block(inode_bid, inode_buffer.data());
  if(read_inode_res.is_err()) {
    return false;
  }

  auto inode_p = reinterpret_cast<Inode *>(inode_buffer.data());
  inode_p->set_size(size);

  auto write_inode_res = operation_->block_manager_->write_block(inode_bid, inode_buffer.data());
  if(write_inode_res.is_err()) {
    return false;
  }

  return true;
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs