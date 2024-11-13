#include <ctime>

#include "filesystem/operations.h"

namespace chfs
{

  // {Your code here}
  auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t>
  {
    inode_id_t inode_id = static_cast<inode_id_t>(0);
    auto inode_res = ChfsResult<inode_id_t>(inode_id);

    // TODO:
    // 1. Allocate a block for the inode.
    auto allocate_block_res = block_allocator_->allocate();
    if (allocate_block_res.is_err())
    {
      return ChfsResult<inode_id_t>(allocate_block_res.unwrap_error());
    }
    block_id_t bid = allocate_block_res.unwrap();

    // 2. Allocate an inode.
    inode_res = inode_manager_->allocate_inode(type, bid);
    if (inode_res.is_err())
    {
      return ChfsResult<inode_id_t>(inode_res.unwrap_error());
    }

    // 3. Initialize the inode block
    //    and write the block back to block manager.
    // 读取存储inode的block到buffer
    std::vector<u8> buffer(this->block_manager_->block_size());
    auto read_block_res = this->block_manager_->read_block(bid, buffer.data());
    if (read_block_res.is_err())
    {
      return ChfsResult<inode_id_t>(read_block_res.unwrap_error());
    }

    Inode *inode_p = (Inode *)buffer.data();
    auto nblocks = inode_p->get_nblocks();
    for (usize idx = 0; idx < nblocks; ++idx)
    {
      inode_p->set_block_direct(idx, KInvalidBlockID);
    }

    // 将buffer写回inode
    auto write_block_res = this->block_manager_->write_block(bid, buffer.data());
    if (write_block_res.is_err())
    {
      return ChfsResult<inode_id_t>(write_block_res.unwrap_error());
    }

    return inode_res;
  }

  auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr>
  {
    return this->inode_manager_->get_attr(id);
  }

  auto FileOperation::get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>
  {
    return this->inode_manager_->get_type_attr(id);
  }

  auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType>
  {
    return this->inode_manager_->get_type(id);
  }

  auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64
  {
    return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
  }

  auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                       u64 offset) -> ChfsResult<u64>
  {
    auto read_res = this->read_file(id);
    if (read_res.is_err())
    {
      return ChfsResult<u64>(read_res.unwrap_error());
    }

    auto content = read_res.unwrap();
    if (offset + sz > content.size())
    {
      content.resize(offset + sz);
    }
    memcpy(content.data() + offset, data, sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err())
    {
      return ChfsResult<u64>(write_res.unwrap_error());
    }
    return ChfsResult<u64>(sz);
  }

  // {Your code here}
  auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
      -> ChfsNullResult
  {
    auto error_code = ErrorType::DONE;
    const auto block_size = this->block_manager_->block_size();
    usize old_block_num = 0;
    usize new_block_num = 0;
    u64 original_file_sz = 0;

    // 1. read the inode
    std::vector<u8> inode(block_size);
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    auto inlined_blocks_num = 0; // direct block的数量（nblocks - 1）

    // 将旧的inode信息读入inode buffer
    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err())
    {
      error_code = inode_res.unwrap_error();
      // I know goto is bad, but we have no choice
      goto err_ret;
    }
    else
    {
      inlined_blocks_num = inode_p->get_direct_block_num();
    }

    if (content.size() > inode_p->max_file_sz_supported())
    {
      std::cerr << "file size too large: " << content.size() << " vs. "
                << inode_p->max_file_sz_supported() << std::endl;
      error_code = ErrorType::OUT_OF_RESOURCE;
      goto err_ret;
    }

    // 2. make sure whether we need to allocate more blocks
    original_file_sz = inode_p->get_size();
    old_block_num = calculate_block_sz(original_file_sz, block_size);
    new_block_num = calculate_block_sz(content.size(), block_size);

    if (new_block_num > old_block_num)
    {
      // If we need to allocate more blocks.
      for (usize idx = old_block_num; idx < new_block_num; ++idx)
      {

        // TODO: Implement the case of allocating more blocks.
        // 1. Allocate a block.
        auto allocate_bid_res = block_allocator_->allocate();
        if (allocate_bid_res.is_err())
        {
          error_code = allocate_bid_res.unwrap_error();
          goto err_ret;
        }
        // 2. Fill the allocated block id to the inode.
        //    You should pay attention to the case of indirect block.
        //    You may use function `get_or_insert_indirect_block`
        //    in the case of indirect block.
        if (inode_p->is_direct_block(idx))
        {
          inode_p->set_block_direct(idx, allocate_bid_res.unwrap());
        }
        else
        {
          // idx位于indirect block指向的范围

          if (indirect_block.size() == 0)
          {
            indirect_block.resize(block_size, 0);
            auto indirect_bid_res = inode_p->get_or_insert_indirect_block(block_allocator_);
            if (indirect_bid_res.is_err())
            {
              error_code = indirect_bid_res.unwrap_error();
              goto err_ret;
            }
            this->block_manager_->read_block(indirect_bid_res.unwrap(), indirect_block.data());
          }

          // 将分配的bid写入indirect_block缓存
          reinterpret_cast<block_id_t *>(indirect_block.data())[idx - inlined_blocks_num] = allocate_bid_res.unwrap();
        }
      }
    }
    else
    {
      // new_block_num <= old_block_num
      
      // We need to free the extra blocks.
      if (!inode_p->is_direct_block(old_block_num - 1) && indirect_block.size() == 0)
      {
        // 需要将indirect_block写入缓存
        indirect_block.resize(block_size);
        auto read_block_res = this->block_manager_->read_block(inode_p->get_indirect_block_id(), indirect_block.data());
        if (read_block_res.is_err())
        {
          error_code = read_block_res.unwrap_error();
          goto err_ret;
        }
      }
      for (usize idx = new_block_num; idx < old_block_num; ++idx)
      {
        if (inode_p->is_direct_block(idx))
        {
          // TODO: Free the direct extra block.
          // 使用block manager释放idx对应的direct block资源
          block_id_t deallocate_bid = (*inode_p)[idx];
          auto deallocate_res = block_allocator_->deallocate(deallocate_bid);
          if (deallocate_res.is_err())
          {
            error_code = deallocate_res.unwrap_error();
            goto err_ret;
          }

          // 在inode中清除idx对应的block id指向
          inode_p->set_block_direct(idx, KInvalidBlockID);
        }
        else
        {
          // TODO: Free the indirect extra block.

          // 在indirect_block buffer中清除idx对应的block id指向
          // 后续需要使用Inode.write_direct_block写回block

          block_id_t &bid_ref = reinterpret_cast<block_id_t *>(indirect_block.data())[idx - inlined_blocks_num];
          auto deallocate_res = block_allocator_->deallocate(bid_ref);
          if (deallocate_res.is_err())
          {
            error_code = deallocate_res.unwrap_error();
            goto err_ret;
          }
          bid_ref = KInvalidBlockID;
        }
      }

      // If there are no more indirect blocks.
      if (old_block_num > inlined_blocks_num &&
          new_block_num <= inlined_blocks_num && true)
      {

        auto res =
            this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
        if (res.is_err())
        {
          error_code = res.unwrap_error();
          goto err_ret;
        }
        indirect_block.clear();
        inode_p->invalid_indirect_block_id();
      }
    }

    // 3. write the contents
    inode_p->inner_attr.size = content.size();
    inode_p->inner_attr.mtime = time(0);

    {
      auto block_idx = 0;
      u64 write_sz = 0;
      block_id_t cur_block_id = KInvalidBlockID;

      while (write_sz < content.size())
      {
        auto sz = ((content.size() - write_sz) > block_size)
                      ? block_size
                      : (content.size() - write_sz);
        std::vector<u8> buffer(block_size);
        memcpy(buffer.data(), content.data() + write_sz, sz);

        if (inode_p->is_direct_block(block_idx))
        {

          // TODO: Implement getting block id of current direct block.
          cur_block_id = (*inode_p)[block_idx];
        }
        else
        {

          // TODO: Implement getting block id of current indirect block.
          cur_block_id = reinterpret_cast<block_id_t *>(indirect_block.data())[block_idx - inlined_blocks_num];
        }

        // TODO: Write to current block.
        auto write_block_res = this->block_manager_->write_block(cur_block_id, buffer.data());
        if (write_block_res.is_err())
        {
          error_code = write_block_res.unwrap_error();
          goto err_ret;
        }

        write_sz += sz;
        block_idx += 1;
      }
    }

    // finally, update the inode
    {
      inode_p->inner_attr.set_all_time(time(0));

      auto write_res =
          this->block_manager_->write_block(inode_res.unwrap(), inode.data());
      if (write_res.is_err())
      {
        error_code = write_res.unwrap_error();
        goto err_ret;
      }
      if (indirect_block.size() != 0)
      {
        write_res =
            inode_p->write_indirect_block(this->block_manager_, indirect_block);
        if (write_res.is_err())
        {
          error_code = write_res.unwrap_error();
          goto err_ret;
        }
      }
    }

    return KNullOk;

  err_ret:
    // std::cerr << "write file return error: " << (int)error_code << std::endl;
    return ChfsNullResult(error_code);
  }

  // {Your code here}
  auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>>
  {
    auto error_code = ErrorType::DONE;
    std::vector<u8> content;

    const auto block_size = this->block_manager_->block_size();

    // 1. read the inode
    std::vector<u8> inode(block_size); // inode buffer
    std::vector<u8> indirect_block(0);
    indirect_block.reserve(block_size);

    auto inode_p = reinterpret_cast<Inode *>(inode.data());
    u64 file_sz = 0;
    u64 read_sz = 0;

    auto inode_res = this->inode_manager_->read_inode(id, inode);
    if (inode_res.is_err())
    {
      error_code = inode_res.unwrap_error();
      // I know goto is bad, but we have no choice
      goto err_ret;
    }

    file_sz = inode_p->get_size();
    content.reserve(file_sz);

    // Now read the file
    while (read_sz < file_sz)
    {
      auto sz = ((inode_p->get_size() - read_sz) > block_size)
                    ? block_size
                    : (inode_p->get_size() - read_sz);
      std::vector<u8> buffer(block_size);

      block_id_t bid = KInvalidBlockID;
      // Get current block id.
      if (inode_p->is_direct_block(read_sz / block_size))
      {
        // TODO: Implement the case of direct block.
        // read content of direct block to buffer
        bid = (*inode_p)[read_sz / block_size];
      }
      else
      {
        // TODO: Implement the case of indirect block.
        // read content of indirect block to var `indirect_block`
        if (indirect_block.size() == 0)
        {
          // indirect block的缓存为空

          auto indirect_bid = inode_p->get_indirect_block_id();
          indirect_block.resize(block_size);
          auto read_block_res = this->block_manager_->read_block(indirect_bid, indirect_block.data());
          if (read_block_res.is_err())
          {
            error_code = read_block_res.unwrap_error();
            goto err_ret;
          }
        }

        bid = reinterpret_cast<block_id_t *>(indirect_block.data())[read_sz / block_size - inode_p->get_direct_block_num()];
      }

      // TODO: Read from current block and store to `content`.
      auto read_block_res = this->block_manager_->read_block(bid, buffer.data());
      if (read_block_res.is_err())
      {
        error_code = read_block_res.unwrap_error();
        goto err_ret;
      }
      content.insert(content.end(), buffer.begin(), buffer.begin() + sz);

      read_sz += sz;
    }

    return ChfsResult<std::vector<u8>>(std::move(content));

  err_ret:
    return ChfsResult<std::vector<u8>>(error_code);
  }

  auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
      -> ChfsResult<std::vector<u8>>
  {
    auto res = read_file(id);
    if (res.is_err())
    {
      return res;
    }

    auto content = res.unwrap();
    return ChfsResult<std::vector<u8>>(
        std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
  }

  auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr>
  {
    auto attr_res = this->getattr(id);
    if (attr_res.is_err())
    {
      return ChfsResult<FileAttr>(attr_res.unwrap_error());
    }

    auto attr = attr_res.unwrap();
    auto file_content = this->read_file(id);
    if (file_content.is_err())
    {
      return ChfsResult<FileAttr>(file_content.unwrap_error());
    }

    auto content = file_content.unwrap();

    if (content.size() != sz)
    {
      content.resize(sz);

      auto write_res = this->write_file(id, content);
      if (write_res.is_err())
      {
        return ChfsResult<FileAttr>(write_res.unwrap_error());
      }
    }

    attr.size = sz;
    return ChfsResult<FileAttr>(attr);
  }

} // namespace chfs
