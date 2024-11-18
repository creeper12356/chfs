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

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
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
  auto mk_res = operation_->mk_helper(parent, name.c_str(), static_cast<InodeType>(type));
  if(mk_res.is_err()) {
    return KInvalidInodeID;
  }

  return mk_res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
    // NOTE: logic similar to FileOperation::unlink, but different
    // 1. Remove the file, you can use the function `remove_file`
    auto child = lookup(parent, name);
    if (child == KInvalidInodeID)
    {
      // file with name `name` do not exist
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

  return {};
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  UNIMPLEMENTED();

  return {};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  UNIMPLEMENTED();

  return false;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  UNIMPLEMENTED();

  return {};
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  UNIMPLEMENTED();

  return {};
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