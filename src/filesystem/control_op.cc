#include "filesystem/operations.h"
#include "metadata/superblock.h"

namespace chfs {

FileOperation::FileOperation(std::shared_ptr<BlockManager> bm,
                             u64 max_inode_supported)
    : block_manager_(bm), inode_manager_(std::shared_ptr<InodeManager>(
                              new InodeManager(bm, max_inode_supported))),
      block_allocator_(std::shared_ptr<BlockAllocator>(
          new BlockAllocator(bm, inode_manager_->get_reserved_blocks()))) {
  // now initialize the superblock
  SuperBlock(bm, inode_manager_->get_max_inode_supported()).flush(0).unwrap();
}

auto FileOperation::create_from_raw(std::shared_ptr<BlockManager> bm)
    -> ChfsResult<std::shared_ptr<FileOperation>> {
  // 1. get the metadata from the super block
  auto superblock_res = SuperBlock::create_from_existing(bm, 0);
  if (superblock_res.is_err()) {
    return ChfsResult<std::shared_ptr<FileOperation>>(
        superblock_res.unwrap_error());
  }

  // 2. create the innode manager
  auto inode_manager_res = InodeManager::create_from_block_manager(
      bm, superblock_res.unwrap()->get_ninodes());
  if (inode_manager_res.is_err()) {
    return ChfsResult<std::shared_ptr<FileOperation>>(
        inode_manager_res.unwrap_error());
  }

  auto reserved_block_num = inode_manager_res.unwrap().get_reserved_blocks();
  return ChfsResult<std::shared_ptr<FileOperation>>(
      std::shared_ptr<FileOperation>(new FileOperation(
          bm, InodeManager::to_shared_ptr(inode_manager_res.unwrap()),
          std::shared_ptr<BlockAllocator>(
              new BlockAllocator(bm, reserved_block_num, false)))));
}

auto FileOperation::get_free_inode_num() const -> ChfsResult<u64> {
  return inode_manager_->free_inode_cnt();
}

auto FileOperation::get_free_blocks_num() const -> ChfsResult<u64> {
  return ChfsResult<u64>(block_allocator_->free_block_cnt());
}

auto FileOperation::remove_file(inode_id_t id) -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  std::vector<block_id_t> free_set;
  block_id_t inode_bid;
  auto cal_free_set_res = cal_free_set(id, free_set, inode_bid);
  if(cal_free_set_res.is_err()) {
    // Error encountered when calculating free set
    return cal_free_set_res;
  }

  // In this function, simply add inode_bid to free_set
  free_set.push_back(inode_bid);

  // free inode first
  auto res = this->inode_manager_->free_inode(id);
    if (res.is_err()) {
      error_code = res.unwrap_error();
      goto err_ret;
  }

  // free the blocks in free_set
  for (auto bid : free_set) {
    auto res = this->block_allocator_->deallocate(bid);
    if (res.is_err()) {
      return res;
    }
  }
  return KNullOk;

err_ret:
  return ChfsNullResult(error_code);
}

auto FileOperation::cal_free_set(inode_id_t id, std::vector<block_id_t> &free_set, block_id_t& free_inode_bid) -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();

  std::vector<u8> inode(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  }
  free_inode_bid = inode_res.unwrap();

  for (uint i = 0; i < inode_p->get_direct_block_num(); ++i) {
    if (inode_p->blocks[i] == KInvalidBlockID) {
      break;
    }
    free_set.push_back(inode_p->blocks[i]);
  }

  if (inode_p->blocks[inode_p->get_direct_block_num()] != KInvalidBlockID) {
    // we still need to release the indirect block
    std::vector<u8> indirect_block;
    auto read_res = this->block_manager_->read_block(
        inode_p->blocks[inode_p->get_direct_block_num()],
        indirect_block.data());
    if (read_res.is_err()) {
      error_code = read_res.unwrap_error();
      goto err_ret;
    }

    auto block_p = reinterpret_cast<block_id_t *>(indirect_block.data());
    for (uint i = 0;
         i < this->block_manager_->block_size() / sizeof(block_id_t); ++i) {
      if (block_p[i] == KInvalidBlockID) {
        break;
      } else {
        free_set.push_back(block_p[i]);
      }
    }
  }
  return KNullOk;
err_ret:
  return ChfsNullResult(error_code);
}

} // namespace chfs
