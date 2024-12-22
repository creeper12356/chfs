#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "protocol.h"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
    enum class Mode {
        LOG,
        SNAPSHOT
    };

public:
    RaftLog(std::shared_ptr<BlockManager> bm, Mode mode);
    ~RaftLog();

    /* Lab3: Your code here */
    // LOG mode only
    auto load_current_term() const -> int;
    auto load_voted_for() const -> int;
    auto load_log_entries() const -> std::vector<LogEntry<Command>>;

    auto store_current_term(int term) -> void;
    auto store_voted_for(int voted_for) -> void;
    auto store_log_entries(const std::vector<LogEntry<Command>> &entries) -> void;

    // SNAPSHOT mode only
    auto load_snapshot() const -> std::tuple<int, int, std::vector<u8>>;
    auto store_snapshot(int last_included_index, int last_included_term, const std::vector<u8> &data) -> void;

    
private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    Mode mode_;

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, Mode mode)
{
    /* Lab3: Your code here */
    bm_ = bm;
    mode_ = mode;
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */
template <typename Command>
auto RaftLog<Command>::load_current_term() const -> int
{
    assert(mode_ == Mode::LOG);
    auto block_data = bm_->unsafe_get_block_ptr();
    return reinterpret_cast<int *>(block_data)[0];
}
template <typename Command>
auto RaftLog<Command>::load_voted_for() const -> int
{
    assert(mode_ == Mode::LOG);
    auto block_data = bm_->unsafe_get_block_ptr();
    return reinterpret_cast<int *>(block_data)[1];
}
template <typename Command>
auto RaftLog<Command>::load_log_entries() const -> std::vector<LogEntry<Command>>
{
    assert(mode_ == Mode::LOG);
    auto block_data = bm_->unsafe_get_block_ptr();
    auto base_ptr = block_data + 2 * sizeof(int);
    auto cur_ptr = base_ptr;
    std::vector<LogEntry<Command>> entries;

    while(true) {
        int term = reinterpret_cast<int *>(cur_ptr)[0];
        if(term == -1) {
            // 遇到-1表示结束
            break;
        }
        cur_ptr += sizeof(int);
        int index = reinterpret_cast<int *>(cur_ptr)[0];
        cur_ptr += sizeof(int);
        int entry_cmd_size = reinterpret_cast<int *>(cur_ptr)[0];
        cur_ptr += sizeof(int);

        Command cmd;
        auto entry_cmd_data = std::vector<u8>(cur_ptr, cur_ptr + entry_cmd_size);
        cmd.deserialize(entry_cmd_data, entry_cmd_size);
        entries.push_back({term, index, cmd});
        cur_ptr += entry_cmd_size;
    }

    return entries;
}
template <typename Command>
auto RaftLog<Command>::store_current_term(int term) -> void
{
    assert(mode_ == Mode::LOG);
    auto block_data = bm_->unsafe_get_block_ptr();
    reinterpret_cast<int *>(block_data)[0] = term;
}

template <typename Command>
auto RaftLog<Command>::store_voted_for(int voted_for) -> void
{
    assert(mode_ == Mode::LOG);
    auto block_data = bm_->unsafe_get_block_ptr();
    reinterpret_cast<int *>(block_data)[1] = voted_for;
}

template <typename Command>
auto RaftLog<Command>::store_log_entries(const std::vector<LogEntry<Command>> &entries) -> void
{
    assert(mode_ == Mode::LOG);
    auto block_data = bm_->unsafe_get_block_ptr();
    auto base_ptr = block_data + 2 * sizeof(int);
    auto cur_ptr = base_ptr;

    for(const auto &log_entry: entries) {
        reinterpret_cast<int *>(cur_ptr)[0] = log_entry.term;
        cur_ptr += sizeof(int);
        reinterpret_cast<int *>(cur_ptr)[0] = log_entry.index;
        cur_ptr += sizeof(int);
        auto entry_cmd_size = log_entry.command.size();
        reinterpret_cast<int *>(cur_ptr)[0] = entry_cmd_size;
        cur_ptr += sizeof(int);

        auto entry_cmd_data = log_entry.command.serialize(entry_cmd_size);
        memcpy(cur_ptr, entry_cmd_data.data(), entry_cmd_size);
        cur_ptr += entry_cmd_size;
    }
    // 末尾加一个-1表示结束
    reinterpret_cast<int *>(cur_ptr)[0] = -1;
}

template <typename Command>
auto RaftLog<Command>::store_snapshot(int last_included_index, int last_included_term, const std::vector<u8> &data) -> void
{
    assert(mode_ == Mode::SNAPSHOT);
    auto block_data = bm_->unsafe_get_block_ptr();
    reinterpret_cast<int *>(block_data)[0] = last_included_index;
    reinterpret_cast<int *>(block_data)[1] = last_included_term;
    auto snapshot_data_size = data.size();
    reinterpret_cast<int *>(block_data)[2] = snapshot_data_size;
    auto snapshot_data_ptr = block_data + 3 * sizeof(int);
    memcpy(snapshot_data_ptr, data.data(), snapshot_data_size);
}

template <typename Command>
auto RaftLog<Command>::load_snapshot() const -> std::tuple<int, int, std::vector<u8>> 
{
    assert(mode_ == Mode::SNAPSHOT);
    auto block_data = bm_->unsafe_get_block_ptr();
    auto last_included_index = reinterpret_cast<int *>(block_data)[0];
    auto last_included_term = reinterpret_cast<int *>(block_data)[1];
    auto snapshot_data_size = reinterpret_cast<int *>(block_data)[2];
    auto snapshot_data_ptr = block_data + 3 * sizeof(int);
    auto snapshot_data = std::vector<u8>(snapshot_data_ptr, snapshot_data_ptr + snapshot_data_size);

    return std::make_tuple(last_included_index, last_included_term, snapshot_data);
}

} /* namespace chfs */
