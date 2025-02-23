#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";


template<typename Command>
struct LogEntry{
    int term;
    int index;
    Command command;

    static LogEntry DummyEntry() {
        return LogEntry{0, 0, Command()};
    }
};


struct RequestVoteArgs {
    /* Lab3: Your code here */
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
    MSGPACK_DEFINE(
        term,
        candidate_id,
        last_log_index,
        last_log_term
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term;
    int vote_granted;
    MSGPACK_DEFINE(
        term,
        vote_granted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<LogEntry<Command>> entries;
    int leader_commit;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    // term, index, command
    std::vector<std::tuple<int, int, std::vector<u8>>> entries;
    int leader_commit;
    MSGPACK_DEFINE(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    auto rpc_arg = RpcAppendEntriesArgs();
    rpc_arg.term = arg.term;
    rpc_arg.leader_id = arg.leader_id;
    rpc_arg.prev_log_index = arg.prev_log_index;
    rpc_arg.prev_log_term = arg.prev_log_term;

    for (const auto &entry: arg.entries) {
        auto rpc_entry = std::make_tuple(entry.term, entry.index, entry.command.serialize(entry.command.size()));
        rpc_arg.entries.push_back(rpc_entry);
    }

    rpc_arg.leader_commit = arg.leader_commit;
    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    auto arg = AppendEntriesArgs<Command>();
    arg.term = rpc_arg.term;
    arg.leader_id = rpc_arg.leader_id;
    arg.prev_log_index = rpc_arg.prev_log_index;
    arg.prev_log_term = rpc_arg.prev_log_term;

    for (const auto &rpc_entry: rpc_arg.entries) {
        Command cmd;
        cmd.deserialize(std::get<2>(rpc_entry), std::get<2>(rpc_entry).size());
        arg.entries.push_back(LogEntry<Command>({std::get<0>(rpc_entry), std::get<1>(rpc_entry), cmd}));
    }

    arg.leader_commit = rpc_arg.leader_commit;
    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    bool success;
    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int term;
    int leader_id;
    int last_included_index;
    int last_included_term;
    int offset;
    std::vector<u8> data;
    bool done;
    MSGPACK_DEFINE(
        term,
        leader_id,
        last_included_index,
        last_included_term,
        offset,
        data,
        done
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int term;
    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */