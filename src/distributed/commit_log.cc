#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
    bm->set_commit_log(this);
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return 0;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  std::lock_guard<std::mutex> lock(log_mutex_);

  auto log_head_ptr = bm_->get_log_head_ptr();
  auto log_content_len = *reinterpret_cast<u32 *>(log_head_ptr);

  // 单个op日志结构
  // <u32 txn_id, u64 block_id, std::vector<u8> block_state>
  // 假定所有block_size都相同
  for(auto &op: ops) {
    auto log_entry_len = sizeof(txn_id_t) + sizeof(block_id_t) + op->new_block_state_.size();
    auto log_entry_content = std::vector<u8>(log_entry_len);
    auto log_entry_content_ptr = log_entry_content.data();

    // 写单个op日志
    memcpy(log_entry_content_ptr, &txn_id, sizeof(txn_id_t));
    log_entry_content_ptr += sizeof(txn_id_t);
    memcpy(log_entry_content_ptr, &op->block_id_, sizeof(block_id_t));
    log_entry_content_ptr += sizeof(block_id_t);
    memcpy(log_entry_content_ptr, op->new_block_state_.data(), bm_->block_size());

    // 追加到log中
    memcpy(log_head_ptr + sizeof(u32) + log_content_len, log_entry_content.data(), log_entry_len);
    log_content_len += log_entry_len;
  }
  // 写回log头部
  memcpy(log_head_ptr, &log_content_len, sizeof(u32));
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}


auto CommitLog::read_log_ops() const -> std::vector<std::shared_ptr<BlockOperation>> {
  auto log_head_ptr = bm_->get_log_head_ptr();
  auto log_content_len = *reinterpret_cast<u32 *>(log_head_ptr);

  auto ops = std::vector<std::shared_ptr<BlockOperation>>();

  // 单个日志结构
  // <u32 txn_id, u64 block_id, std::vector<u8> block_state>
  auto log_content_ptr = log_head_ptr + sizeof(u32);
  while(log_content_ptr != log_head_ptr + sizeof(u32) + log_content_len) {
    txn_id_t txn_id;
    block_id_t block_id;

    memcpy(&txn_id, log_content_ptr, sizeof(txn_id_t));
    log_content_ptr += sizeof(txn_id_t);
    memcpy(&block_id, log_content_ptr, sizeof(block_id_t));
    log_content_ptr += sizeof(block_id_t);
    auto block_state = std::vector<u8>(bm_->block_size());
    memcpy(block_state.data(), log_content_ptr, bm_->block_size());
    log_content_ptr += bm_->block_size();

    ops.push_back(std::make_shared<BlockOperation>(block_id, block_state));
  }

  return ops;
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  recover_with_ret();
}

auto CommitLog::recover_with_ret() -> bool {
  std::lock_guard<std::mutex> lock(log_mutex_);

  auto ops = read_log_ops();

  // 重放日志
  for(auto &op: ops) {
    auto res = bm_->write_block(op->block_id_, op->new_block_state_.data());
    if(res.is_err()) {
      return false;
    }
  }

  return true;
}

auto CommitLog::clean() -> void {
  std::lock_guard<std::mutex> lock(log_mutex_);

  auto log_head_ptr = bm_->get_log_head_ptr();
  reinterpret_cast<u32 *>(log_head_ptr)[0] = 0;
}
}; // namespace chfs