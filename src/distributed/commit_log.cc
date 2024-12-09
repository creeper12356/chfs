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
  return log_entry_num_;
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
  // 使用一条空的日志来标记事务的提交
  // NOTE: 可读性很差
  // <txn_id, 0, empty_block_state> 
  auto commit_ops = std::vector<std::shared_ptr<BlockOperation>>();
  commit_ops.push_back(std::make_shared<BlockOperation>(0, std::vector<u8>(bm_->block_size(), 0)));
  this->append_log(txn_id, commit_ops);
}


auto CommitLog::read_log_ops() const -> std::vector<std::shared_ptr<BlockOperation>> {
  auto entries = read_log_entries([](txn_id_t txn_id) { return true; });
  auto ops = std::vector<std::shared_ptr<BlockOperation>>();
  for(auto &entry: entries) {
    ops.push_back(std::make_shared<BlockOperation>(entry->block_id, entry->block_state));
  }
  return ops;
}

auto CommitLog::read_log_entries(std::function<bool(txn_id_t)> filter) const 
  -> std::vector<std::shared_ptr<RedoLogEntry>> {
  auto log_head_ptr = bm_->get_log_head_ptr();
  auto log_content_len = *reinterpret_cast<u32 *>(log_head_ptr);

  auto log_entries = std::vector<std::shared_ptr<RedoLogEntry>>();

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
    if(filter(txn_id)) {
      log_entries.push_back(
        std::make_shared<RedoLogEntry>(txn_id, block_id, block_state)
      );
    }
  }

  return log_entries;
}



// {Your code here}
auto CommitLog::checkpoint() -> void {
  auto entries = read_log_entries([](txn_id_t txn_id) { return true; });
  auto commited_txn_ids = std::vector<txn_id_t>();
  for(auto &entry: entries) {
    if(entry->block_id == 0) {
      commited_txn_ids.push_back(entry->txn_id);
    }
  }

  
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
    if(op->block_id_ == 0) {
      // 提交日志，不需要处理
      continue;
    }

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