//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <functional> 

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
class BlockOperation {
public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

struct RedoLogEntry {
  txn_id_t txn_id;
  block_id_t block_id;
  std::vector<u8> block_state;

public :
  RedoLogEntry(txn_id_t txn_id, block_id_t block_id, std::vector<u8> block_state)
    : txn_id(txn_id), block_id(block_id), block_state(block_state) {}
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;

  /**
   * @brief 读取日志中的所有操作
   */
  auto read_log_ops() const -> std::vector<std::shared_ptr<BlockOperation>>;

  /**
   * @brief 筛选并读取日志中的操作
   * @param filter 筛选函数
   */
  auto read_log_entries(std::function<bool(txn_id_t)> filter) const
      -> std::vector<std::shared_ptr<RedoLogEntry>>;
  
  auto checkpoint() -> void;
  auto recover() -> void;
  auto recover_with_ret() -> bool;
  auto get_log_entry_num() -> usize;

  auto clean() -> void;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */

private:
  std::mutex log_mutex_;
  usize log_entry_num_ = 0U;
};

} // namespace chfs