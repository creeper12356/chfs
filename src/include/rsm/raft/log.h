#pragma once

#include "common/macros.h"
#include "block/manager.h"
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
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here */
    auto load_current_term() const -> int;
    auto load_voted_for() const -> int;
    auto load_log_entries() const -> std::vector<std::pair<int, Command>>;

    auto store_current_term(int term) -> void;
    auto store_voted_for(int voted_for) -> void;
    auto store_log_entries(const std::vector<std::pair<int, Command>> &entries) -> void;

    
private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */

};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    /* Lab3: Your code here */
    
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
    auto block_data = bm_->unsafe_get_block_ptr();
    return reinterpret_cast<int *>(block_data)[0];
}
template <typename Command>
auto RaftLog<Command>::load_voted_for() const -> int
{
    auto block_data = bm_->unsafe_get_block_ptr();
    return reinterpret_cast<int *>(block_data)[1];
}
template <typename Command>
auto RaftLog<Command>::load_log_entries() const -> std::vector<std::pair<int, Command>>
{
    auto block_data = bm_->unsafe_get_block_ptr();
    auto base_ptr = block_data + 2 * sizeof(int);
    auto cur_ptr = base_ptr;
    std::vector<std::pair<int, Command>> entries;

    while(true) {
        int term = reinterpret_cast<int *>(cur_ptr)[0];
        if(term == -1) {
            // 遇到-1表示结束
            break;
        }
        cur_ptr += sizeof(int);
        int entry_cmd_size = reinterpret_cast<int *>(cur_ptr)[0];
        cur_ptr += sizeof(int);

        Command cmd;
        auto entry_cmd_data = std::vector<u8>(cur_ptr, cur_ptr + entry_cmd_size);
        cmd.deserialize(entry_cmd_data, entry_cmd_size);
        entries.push_back(std::make_pair(term, cmd));
        cur_ptr += entry_cmd_size;
    }

    return entries;
}
template <typename Command>
auto RaftLog<Command>::store_current_term(int term) -> void
{
    auto block_data = bm_->unsafe_get_block_ptr();
    reinterpret_cast<int *>(block_data)[0] = term;
}

template <typename Command>
auto RaftLog<Command>::store_voted_for(int voted_for) -> void
{
    auto block_data = bm_->unsafe_get_block_ptr();
    reinterpret_cast<int *>(block_data)[1] = voted_for;
}

template <typename Command>
auto RaftLog<Command>::store_log_entries(const std::vector<std::pair<int, Command>> &entries) -> void
{
    auto block_data = bm_->unsafe_get_block_ptr();
    auto base_ptr = block_data + 2 * sizeof(int);
    auto cur_ptr = base_ptr;

    for(const auto &log_entry: entries) {
        reinterpret_cast<int *>(cur_ptr)[0] = log_entry.first;
        cur_ptr += sizeof(int);
        auto entry_cmd_size = log_entry.second.size();
        reinterpret_cast<int *>(cur_ptr)[0] = entry_cmd_size;
        cur_ptr += sizeof(int);

        auto entry_cmd_data = log_entry.second.serialize(entry_cmd_size);
        memcpy(cur_ptr, entry_cmd_data.data(), entry_cmd_size);
        cur_ptr += entry_cmd_size;
    }
    // 末尾加一个-1表示结束
    reinterpret_cast<int *>(cur_ptr)[0] = -1;
}

} /* namespace chfs */
