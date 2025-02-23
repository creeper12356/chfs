#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */

    std::unique_ptr<ThreadPool> thread_pool; // thread-safe itself
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<RaftLog<Command>> snapshot_storage; 
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role; 
    int current_term; 
    int leader_id; 

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int voted_for = -1; 

    int commit_index = 0; 
    int last_applied = 0;  // logical index

    int vote_count = 0; 

    std::atomic<std::chrono::time_point<std::chrono::system_clock>> last_heartbeat_time = std::chrono::system_clock::now();

    // 内存中的日志列表，每个日志条目包含<term, command> 

    std::vector<LogEntry<Command>> log_entries; 

    // snapshot 相关
    int snapshot_last_index; // logical
    int snapshot_last_term;

    std::vector<u8> snapshot_data;

    int logical_to_physical(int logical_index) const {
        return logical_index - snapshot_last_index;
    }

    int physical_to_logical(int physical_index) const {
        return physical_index + snapshot_last_index;
    }

    // leaders-only member
    std::vector<int> next_index;
    std::vector<int> match_index;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

    /* Lab3: Your code here */ 
    // NOTE: do not hardcoded the thread number
    thread_pool = std::make_unique<ThreadPool>(100);
    state = std::make_unique<StateMachine>();


    // 初始化日志持久化存储
    auto node_log_filename = "/tmp/raft_log/" + std::to_string(my_id) + ".log";

    if(is_file_exist(node_log_filename)) {
        // 从持久化存储中加载数据
        auto bm = std::make_shared<BlockManager>(node_log_filename);
        log_storage = std::make_unique<RaftLog<Command>>(bm, RaftLog<Command>::Mode::LOG);
        
        current_term = log_storage->load_current_term();
        voted_for = log_storage->load_voted_for();
        log_entries = log_storage->load_log_entries();
    } else {
        // 初始化持久化存储
        auto bm = std::make_shared<BlockManager>(node_log_filename);
        log_storage = std::make_unique<RaftLog<Command>>(bm, RaftLog<Command>::Mode::LOG);

        assert(current_term == 0);
        log_storage->store_current_term(current_term);
        assert(voted_for == -1);
        log_storage->store_voted_for(voted_for);

        // NOTE: 由于日志index从1开始，所以这里插入一个空的log entry
        log_entries.push_back(LogEntry<Command>::DummyEntry());
        log_storage->store_log_entries(log_entries);
    }

    auto node_snapshot_filename = "/tmp/raft_log/" + std::to_string(my_id) + ".snapshot";
    if(is_file_exist(node_snapshot_filename)) {
        auto bm = std::make_shared<BlockManager>(node_snapshot_filename);
        snapshot_storage = std::make_unique<RaftLog<Command>>(bm, RaftLog<Command>::Mode::SNAPSHOT);

        std::tie(snapshot_last_index, snapshot_last_term, snapshot_data) = snapshot_storage->load_snapshot();
        state->apply_snapshot(snapshot_data);
        last_applied = snapshot_last_index;
        commit_index = snapshot_last_index;
    } else {
        auto bm = std::make_shared<BlockManager>(node_snapshot_filename);
        snapshot_storage = std::make_unique<RaftLog<Command>>(bm, RaftLog<Command>::Mode::SNAPSHOT);

        snapshot_last_index = 0;
        snapshot_last_term = 0;
        snapshot_data = state->snapshot();

        snapshot_storage->store_snapshot(snapshot_last_index, snapshot_last_term, snapshot_data);
    }


    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    stopped = false;

    for(auto &config: node_configs) {
        rpc_clients_map[config.node_id] = std::make_unique<RpcClient>(config.ip_address, config.port, true);
    }

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped = true;
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    return std::make_tuple(role == RaftRole::Leader, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    RAFT_LOG("new_cmd\tarrival with cmdsize: %d", cmd_size);
    std::unique_lock<std::mutex> lock(mtx);
    if(role != RaftRole::Leader) {
        RAFT_LOG("new_cmd\treject cuz not leader");
        return std::make_tuple(false, current_term, -1);
    }

    // 同步地给所有follower发送一次心跳
    RAFT_LOG("new_cmd\tping to make sure self are newest leader");
    AppendEntriesArgs<Command> append_entries_args;
    append_entries_args.term = current_term;
    append_entries_args.leader_id = my_id;
    // NOTE: 心跳不带有prev_log_index和prev_log_term,以及entries
    append_entries_args.leader_commit = commit_index;

    lock.unlock();
    std::vector<std::future<void>> futures;
    for(auto &pair: rpc_clients_map) {
        if(pair.first == my_id) {
            // 不需要给自己发心跳
            continue;
        }
        futures.push_back(thread_pool->enqueue(&RaftNode::send_append_entries, this, pair.first, append_entries_args));
    }
    // 显式等待所有的心跳发送并响应
    for(auto &future: futures) {
        future.get();
    }
    lock.lock();

    if(role != RaftRole::Leader) {
        // 发送心跳后发现自己不再是leader
        RAFT_LOG("new_cmd\treject cuz not leader any more");
        return std::make_tuple(false, current_term, -1);
    }
    
    auto log_entry = LogEntry<Command>({current_term, physical_to_logical(log_entries.size()), Command()});
    log_entry.command.deserialize(cmd_data, cmd_size);

    log_entries.push_back(std::move(log_entry));
    log_storage->store_log_entries(log_entries);

    RAFT_LOG("new_cmd\tlog entry added, log index: %d", static_cast<int>(physical_to_logical(log_entries.size() - 1)));

    return std::make_tuple(true, current_term, physical_to_logical(log_entries.size() - 1));
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    auto new_snapshot_last_index = last_applied;
    snapshot_last_term = log_entries[logical_to_physical(last_applied)].term;
    snapshot_data = state->snapshot();

    auto new_log_entries = std::vector<LogEntry<Command>>(log_entries.begin() + logical_to_physical(last_applied + 1), log_entries.end());
    new_log_entries.insert(new_log_entries.begin(), LogEntry<Command>::DummyEntry()); // dummy entry at index 0
    log_entries = new_log_entries;

    // NOTE: 必须最后更新snapshot_last_index!
    snapshot_last_index = new_snapshot_last_index;
    log_storage->store_log_entries(log_entries);
    snapshot_storage->store_snapshot(snapshot_last_index, snapshot_last_term, snapshot_data);

    RAFT_LOG("save_snap\tlast_applied: %d, snapshot_last_index: %d, snapshot_last_term: %d", last_applied, snapshot_last_index, snapshot_last_term);
    RAFT_LOG("savve_snap\tnew log_entries size: %d", static_cast<int>(log_entries.size()));

    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    std::unique_lock<std::mutex> lock(mtx);
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    // RAFT_LOG("req_vote\ttid: %lu", std::hash<std::thread::id>{}(std::this_thread::get_id()));

    if(args.term < current_term) {
        RAFT_LOG("req_vote\tterm too old");
        return RequestVoteReply{current_term, false};
    }
    if(args.term > current_term) {
        log_storage->store_current_term(args.term);
        current_term = args.term;

        role = RaftRole::Follower;
        leader_id = -1;

        log_storage->store_voted_for(-1);
        voted_for = -1;
    }

    if(voted_for == -1 || voted_for == args.candidate_id) {
        last_heartbeat_time = std::chrono::system_clock::now();

        log_storage->store_voted_for(args.candidate_id);
        voted_for = args.candidate_id;
        // 判断candidate的LOG是否比自己新
        // NOTE: 需要考虑snapshot的情况
        auto last_log_entry_term = log_entries.size() == 1 ? snapshot_last_term : log_entries.back().term;
        auto last_log_entry_index = log_entries.size() == 1 ? snapshot_last_index : physical_to_logical(log_entries.size() - 1);

        if(args.last_log_term > last_log_entry_term
         || (args.last_log_term == last_log_entry_term && args.last_log_index >= last_log_entry_index) ){
            RAFT_LOG("req_vote\tvote granted for %d", args.candidate_id);
            return RequestVoteReply{current_term, true};
        } else {
            RAFT_LOG("req_vote\tvote denied. candidate's log is not up-to-date");
            RAFT_LOG("req_vote\tlast_log_term %d, last_log_index %d, args.last_log_term %d, args.last_log_index %d", last_log_entry_term, last_log_entry_index, args.last_log_term, args.last_log_index);
        }
    }
    RAFT_LOG("req_vote\tvote denied.");
    return RequestVoteReply{current_term, false};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    if(reply.term > current_term) {
        log_storage->store_current_term(reply.term);
        current_term = reply.term;

        role = RaftRole::Follower;
        leader_id = -1;

        log_storage->store_voted_for(-1);
        voted_for = -1;
        return ;
    }

    // 收集投票结果
    if(role == RaftRole::Candidate && reply.vote_granted) {
        std::lock_guard<std::mutex> clients_lock(clients_mtx);

        ++ vote_count;
        // NOTE: 可以将这个逻辑移到run_background_election中
        if(vote_count > rpc_clients_map.size() / 2) {
            RAFT_LOG("hdl_req_vote\tbecome leader");
            // 新的leader上任(come to power)
            role = RaftRole::Leader;
            leader_id = my_id;

            // 重新初始化next_index
            next_index.resize(rpc_clients_map.size());
            std::fill(next_index.begin(), next_index.end(), physical_to_logical(log_entries.size()));

            // 重新初始化match_index
            match_index.resize(rpc_clients_map.size());
            std::fill(match_index.begin(), match_index.end(), 0);
        }
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    auto arg = transform_rpc_append_entries_args<Command>(rpc_arg);
    // RAFT_LOG("app_ent\ttid: %lu", std::hash<std::thread::id>{}(std::this_thread::get_id()));
    if(arg.term < current_term) {
        // 旧的任期，忽略
        RAFT_LOG("app_ent\tterm too old");
        // RAFT_LOG("app_ent\tcurrent_term %d, rpc_arg.term %d", current_term, rpc_arg.term);
        return AppendEntriesReply{current_term, false};
    }
    if(arg.term > current_term) {
        // 任期更新
        RAFT_LOG("app_ent\tterm update and become follower");

        log_storage->store_current_term(arg.term);
        current_term = arg.term;

        role = RaftRole::Follower;
        leader_id = arg.leader_id;

        log_storage->store_voted_for(-1);
        voted_for = -1;
    }

    if(role == RaftRole::Candidate) {
        RAFT_LOG("app_ent\tcandidate -> follower");
        // 当前是candidate，
        // 收到心跳或者追加日志请求，
        // 转为follower
        role = RaftRole::Follower;
        leader_id = arg.leader_id;
    }

    if(arg.entries.size() == 0) {
        // 接收leader心跳
        RAFT_LOG("app_ent\tupd last heartbeat");
        last_heartbeat_time = std::chrono::system_clock::now();
    } else {
        // 接收leader的追加日志请求
        RAFT_LOG("app_ent\treceive append entries from %d", arg.leader_id);
        RAFT_LOG("app_ent\targ.prev_log_index: %d, logical_bound: %d", arg.prev_log_index, physical_to_logical(log_entries.size()));
        if(arg.prev_log_index >= physical_to_logical(log_entries.size())) {
            // 来自prev_log_index位置不存在log entry
            RAFT_LOG("app_ent\treject cuz no such log entry at %d", arg.prev_log_index);
            return AppendEntriesReply{current_term, false};
        }
        
        auto this_compared_term = arg.prev_log_index == snapshot_last_index ? snapshot_last_term : log_entries[logical_to_physical(arg.prev_log_index)].term;
        if(arg.prev_log_term != this_compared_term) {
            // 来自leader的prev_log_index位置的term与自己的term不一致
            RAFT_LOG("app_ent\treject cuz term not match: arg.prev_log_term %d, log_entries[prev_log_index].term %d", arg.prev_log_term, this_compared_term);
            return AppendEntriesReply{current_term, false};
        }

        // 重写arg.prev_log_index之后的日志
        log_entries.resize(logical_to_physical(arg.prev_log_index) + 1);
        auto arg_entries_size = arg.entries.size();
        bool is_appending = false;
        for(int phy_idx = 0; phy_idx < arg_entries_size; ++ phy_idx) {
            if(arg.entries[phy_idx].index == arg.prev_log_index + 1) {
                is_appending = true;
            }

            if(is_appending) {
                log_entries.push_back(arg.entries[phy_idx]);
            }
        }

        log_storage->store_log_entries(log_entries);
    }

    // 更新 commit index
    if(arg.leader_commit > commit_index) {
        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) // TODO: check again
        commit_index = std::min<int>(arg.leader_commit, physical_to_logical(log_entries.size() - 1));
        RAFT_LOG("app_ent\tupd commit index to %d", commit_index);
    }

    return AppendEntriesReply{current_term, true};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.term > current_term) {
        RAFT_LOG("send_app_ent\tterm update and become follower");
        role = RaftRole::Follower;
        leader_id = -1;

        log_storage->store_current_term(reply.term);
        current_term = reply.term;

        log_storage->store_voted_for(-1);
        voted_for = -1;
        return;
    }

    if(arg.entries.size() == 0) {
        // 心跳
        return ;
    }

    if(reply.success) {
        // AppendEntries成功，更新next_index和match_index
        // NOTE: 此处更新next_index不需要精确
        // 只需要保证next_index[node_id] >= arg.entries.size()
        // ref: https://groups.google.com/g/raft-dev/c/2-ReA6bLJTk?pli=1
        next_index[node_id] = physical_to_logical(arg.entries.size());
        match_index[node_id] = next_index[node_id] - 1;
        RAFT_LOG("send_app_ent\tappend success, upd next_index to %d, match_index to %d", next_index[node_id], match_index[node_id]);
    } else {
        // AppendEntries失败，减小next_index，重试
        RAFT_LOG("send_app_ent\tappend fail, retry");
        RAFT_LOG("send_app_ent\tnext_index[%d] %d, snapshot_last_index %d", node_id, next_index[node_id], snapshot_last_index);
        RAFT_LOG("send_app_ent\tso choose %s", next_index[node_id] <= snapshot_last_index ? "install snapshot" : "append entries");
        if(next_index[node_id] <= snapshot_last_index) {
            InstallSnapshotArgs install_snapshot_args;
            install_snapshot_args.term = current_term;
            install_snapshot_args.leader_id = my_id;
            install_snapshot_args.last_included_index = snapshot_last_index;
            install_snapshot_args.last_included_term = snapshot_last_term;
            install_snapshot_args.data = snapshot_data;

            RAFT_LOG("send_app_ent\tsend install snapshot to %d", node_id);
            RAFT_LOG("send_app_ent\targs: term %d, leader_id %d, last_included_index %d, last_included_term %d, data size %lu", install_snapshot_args.term, install_snapshot_args.leader_id, install_snapshot_args.last_included_index, install_snapshot_args.last_included_term, install_snapshot_args.data.size());

            thread_pool->enqueue(&RaftNode::send_install_snapshot, this, node_id, install_snapshot_args);
        } else {
            auto append_entries_args = arg;
            -- next_index[node_id];
            append_entries_args.prev_log_index = next_index[node_id] - 1;
            if(append_entries_args.prev_log_index == snapshot_last_index)
            {
                append_entries_args.prev_log_term = snapshot_last_term;
            } else {
                append_entries_args.prev_log_term = log_entries[logical_to_physical(append_entries_args.prev_log_index)].term;
            }

            RAFT_LOG("send_app_ent\tsend append entries to %d", node_id);
            RAFT_LOG("send_app_ent\targs: term %d, leader_id %d, prev_log_index %d, prev_log_term %d, entries size %lu, leader_commit %d", append_entries_args.term, append_entries_args.leader_id, append_entries_args.prev_log_index, append_entries_args.prev_log_term, append_entries_args.entries.size(), append_entries_args.leader_commit);

            thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, append_entries_args);
        }
        
    }
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    std::unique_lock<std::mutex> lock(mtx);
    if(args.term < current_term) {
        RAFT_LOG("install_snap\tterm too old");
        return InstallSnapshotReply{current_term};
    }
    if(args.term > current_term) {
        RAFT_LOG("install_snap\tterm update and become follower");

        log_storage->store_current_term(args.term);
        current_term = args.term;

        role = RaftRole::Follower;
        leader_id = args.leader_id;

        log_storage->store_voted_for(-1);
        voted_for = -1;
    }

    if(args.last_included_index <= snapshot_last_index) {
        RAFT_LOG("install_snap\treject cuz already installed");
        return InstallSnapshotReply{current_term};
    }

    // 检查日志中是否包含snapshot的最后一个entry
    bool log_has_snapshot_last_entry = false;
    auto log_entries_size = log_entries.size();
    for(int i = 1; i < log_entries_size; ++ i) {
        if(physical_to_logical(i) == args.last_included_index && log_entries[i].term == args.last_included_term) {
            log_has_snapshot_last_entry = true;
            break;
        }
    }

    if(log_has_snapshot_last_entry) {
        // 截断日志，只保留last_included_index之后的日志
        auto new_log_entries = std::vector<LogEntry<Command>>(log_entries.begin() + logical_to_physical(args.last_included_index) + 1, log_entries.end());
        new_log_entries.insert(new_log_entries.begin(), LogEntry<Command>::DummyEntry()); // dummy entry at index 0
        log_entries = new_log_entries;
        commit_index = std::max<int>(commit_index, args.last_included_index);

    } else {
        // 清空所有日志
        log_entries.clear();
        log_entries.push_back(LogEntry<Command>::DummyEntry()); // dummy entry at index 0
        commit_index = args.last_included_index;
    }
    
    // 更新snapshot
    snapshot_last_index = args.last_included_index;
    snapshot_last_term = args.last_included_term;
    snapshot_data = args.data;
    snapshot_storage->store_snapshot(snapshot_last_index, snapshot_last_term, snapshot_data);

    // apply immediately to rsm
    state->apply_snapshot(snapshot_data);
    RAFT_LOG("install_snap\tupd last_applied to %d", args.last_included_index);
    last_applied = args.last_included_index;

    // persist log entries
    log_storage->store_log_entries(log_entries);

    return InstallSnapshotReply{current_term};
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    if(reply.term > current_term) {
        RAFT_LOG("hdl_ins_snap\tterm update and become follower");
        role = RaftRole::Follower;
        leader_id = -1;

        log_storage->store_current_term(reply.term);
        current_term = reply.term;

        log_storage->store_voted_for(-1);
        voted_for = -1;
        return;
    }

    match_index[node_id] = arg.last_included_index;
    next_index[node_id] = arg.last_included_index + 1;
    RAFT_LOG("hdl_ins_snap\tmatch_index to %d, next_index to %d", match_index[node_id], next_index[node_id]);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    // RAFT_LOG("send_req_vote\ttid: %lu", std::hash<std::thread::id>{}(std::this_thread::get_id()));
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        RAFT_LOG("send_req_vote\tnot connected to %d", target_id);
        return ;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
        RAFT_LOG("send_req_vote\trpc fails");
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    // RAFT_LOG("send_app_ent\ttarget_id %d", target_id);
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        RAFT_LOG("send_app_ent\tnot connected to %d", target_id);
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        // RAFT_LOG("send_app_ent\trpc success");
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
        // NOTE: 暂时
        RAFT_LOG("send_app_ent\trpc fails");
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        RAFT_LOG("send_ins_snap\tnot connected to %d", target_id);
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    while (true) {
        {
            // RAFT_LOG("bg_ele\ttid: %lu", std::hash<std::thread::id>{}(std::this_thread::get_id()));
            if (is_stopped()) {
                return;
            }
            RaftRole role_local;
            {
                std::unique_lock<std::mutex> lock(mtx);
                role_local = role;
            }
            if(role_local != RaftRole::Follower) {
                // 只有follower才需要检查leader的心跳
                // NOTE: 可以直接sleep，等变成follower再唤醒这个线程
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            /* Lab3: Your code here */
            // follower定期检查来自leader的心跳
            // 使用随机的选举超时时间
            int election_timeout = RandomNumberGenerator().rand(300, 500);
            RAFT_LOG("bg_ele\tgen timeout %d, sleep to wait", election_timeout);
            std::this_thread::sleep_for(std::chrono::milliseconds(election_timeout));
            if(last_heartbeat_time.load() + std::chrono::milliseconds(election_timeout) < std::chrono::system_clock::now()) {
                // 收到leader心跳超时，发起选举
                RAFT_LOG("bg_ele\tnew election start");
                RequestVoteArgs request_vote_args;
                {
                    std::unique_lock<std::mutex> lock(mtx);

                    ++ current_term;

                    log_storage->store_voted_for(-1);
                    voted_for = -1; //NOTE: voted_for = my_id;也可以
                    leader_id = -1;
                    role = RaftRole::Candidate;
                    request_vote_args.term = current_term;
                    request_vote_args.candidate_id = my_id;
                    // NOTE: 需要考虑snapshot的情况
                    RAFT_LOG("req_vote\tlog_entries size %lu", log_entries.size());
                    if(log_entries.size() == 1) {
                        RAFT_LOG("req_vote\tchoose snapshot_last_index %d, snapshot_last_term %d", snapshot_last_index, snapshot_last_term);
                    } else {
                        RAFT_LOG("req_vote\tchoose last log index %d, last log term %d", physical_to_logical(log_entries.size() - 1), log_entries.back().term);
                    }
                    request_vote_args.last_log_index = log_entries.size() == 1 ? snapshot_last_index : physical_to_logical(log_entries.size() - 1);
                    request_vote_args.last_log_term = log_entries.size() == 1 ? snapshot_last_term : log_entries.back().term;
                }

                std::vector<std::future<void>> futures;
                {
                    std::unique_lock<std::mutex> lock(clients_mtx);
                    for(auto &pair: rpc_clients_map) {
                        futures.push_back(thread_pool->enqueue(&RaftNode::send_request_vote, this, pair.first, request_vote_args));
                    }
                }

                // 等待投票结果
                for(auto &future: futures) {
                    future.get();
                }

                {
                    std::unique_lock<std::mutex> lock(mtx);
                    if(role == RaftRole::Candidate) {
                        // 选举未成功（失败或者没有leader）
                        RAFT_LOG("bg_ele\telection not success");
                        role = RaftRole::Follower;
                    } else {
                        RAFT_LOG("bg_ele\telection success");
                    }
                    vote_count = 0;
                }
            }
            
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            // NOTE: 此处需要立即放锁，否则sleep拿锁会导致死锁
            RaftRole role_local;
            {
                std::unique_lock<std::mutex> lock(mtx);
                role_local = role;
            }

            if (role_local == RaftRole::Leader) {
                std::vector<int> node_ids = {};
                {
                    // 从rpc_clients_map中获取所有节点的id
                    std::unique_lock<std::mutex> lock(clients_mtx);
                    for(auto &pair: rpc_clients_map) {
                        node_ids.push_back(pair.first);
                    }
                }

                for(auto &node_id: node_ids) {
                    std::unique_lock<std::mutex> lock(mtx);
                    if(node_id == my_id) {
                        continue;
                    }

                    if(physical_to_logical(log_entries.size() - 1) < next_index[node_id]) {
                        continue;
                    }
                    RAFT_LOG("bg_commit\tnext_index[%d] %d, snapshot_last_index %d", node_id, next_index[node_id], snapshot_last_index);
                    RAFT_LOG("bg_commit\tso choose %s", next_index[node_id] <= snapshot_last_index ? "install snapshot" : "append entries");

                    if(next_index[node_id] <= snapshot_last_index) {
                        InstallSnapshotArgs install_snapshot_args;
                        install_snapshot_args.term = current_term;
                        install_snapshot_args.leader_id = my_id;
                        install_snapshot_args.last_included_index = snapshot_last_index;
                        install_snapshot_args.last_included_term = snapshot_last_term;
                        install_snapshot_args.data = snapshot_data;

                        RAFT_LOG("bg_commit\tsend install snapshot to %d", node_id);
                        RAFT_LOG("bg_commit\targs: term %d, leader_id %d, last_included_index %d, last_included_term %d, data size %lu", install_snapshot_args.term, install_snapshot_args.leader_id, install_snapshot_args.last_included_index, install_snapshot_args.last_included_term, install_snapshot_args.data.size());

                        thread_pool->enqueue(&RaftNode::send_install_snapshot, this, node_id, install_snapshot_args);
                    } else {
                        AppendEntriesArgs<Command> append_entries_args;
                        append_entries_args.term = current_term;
                        append_entries_args.leader_id = my_id;
                        append_entries_args.prev_log_index = next_index[node_id] - 1;
                        if(append_entries_args.prev_log_index == snapshot_last_index) {
                            append_entries_args.prev_log_term = snapshot_last_term;
                        } else {
                            append_entries_args.prev_log_term = log_entries[logical_to_physical(append_entries_args.prev_log_index)].term;
                        }
                        append_entries_args.entries = log_entries;
                        append_entries_args.leader_commit = commit_index;

                        RAFT_LOG("bg_commit\tsend append entries to %d", node_id);
                        RAFT_LOG("bg_commit\targs: term %d, leader_id %d, prev_log_index %d, prev_log_term %d, entries size %lu, leader_commit %d", append_entries_args.term, append_entries_args.leader_id, append_entries_args.prev_log_index, append_entries_args.prev_log_term, append_entries_args.entries.size(), append_entries_args.leader_commit);

                        thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, append_entries_args);
                    }
                }
                
                RAFT_LOG("bg_commit\tsleep to wait");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                
                // 更新leader的commit index
                {
                    RAFT_LOG("bg_commit\ttry upd leader commit index");
                    std::unique_lock<std::mutex> lock(mtx);
                    //NOTE: 需要更新match_index[leader_id]
                    match_index[my_id] = physical_to_logical(log_entries.size() - 1);

                    auto sorted_match_index = match_index;
                    std::sort(sorted_match_index.begin(), sorted_match_index.end());
                    auto sorted_match_index_size = sorted_match_index.size();

                    int max_commit_index = sorted_match_index[
                        sorted_match_index_size % 2 ? sorted_match_index_size / 2 : sorted_match_index_size / 2 - 1
                    ];

                    if(commit_index >= max_commit_index) {
                        RAFT_LOG("bg_commit\tleader commit index is up-to-date, commit index %d, max_commit_index %d", commit_index, max_commit_index);
                        continue;
                    }
                    for(auto ci = max_commit_index; ci > commit_index; -- ci) {
                        if(log_entries[logical_to_physical(ci)].term == current_term) {
                            // 大多数节点都已经复制了这个日志，更新commit index
                            RAFT_LOG("bg_commit\tupdate leader commit index to %d", ci);
                            commit_index = ci;
                            break;
                        }
                    }
                }
            }
        }
        // NOTE: do not hardcoded this value
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(mtx);
            RAFT_LOG("bg_apply\tcommit index %d, last_applied %d", commit_index, last_applied);
            if(commit_index > last_applied) {
                RAFT_LOG("bg_apply\tso need to apply log entries");
                for(int i = last_applied + 1; i <= commit_index; ++ i) {
                    state->apply_log(log_entries[logical_to_physical(i)].command);
                }
                last_applied = commit_index;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            // RAFT_LOG("bg_ping\trunning bg_ping");
            /* Lab3: Your code here */
            {
                std::unique_lock<std::mutex> lock(mtx);
                if(role == RaftRole::Leader) {
                    RAFT_LOG("bg_ping\tping...");
                    AppendEntriesArgs<Command> append_entries_args;
                    append_entries_args.term = current_term;
                    append_entries_args.leader_id = my_id;
                    // NOTE: 心跳不带有prev_log_index和prev_log_term,以及entries
                    append_entries_args.leader_commit = commit_index;
                    for(auto &pair: rpc_clients_map) {
                        if(pair.first == my_id) {
                            // 不需要给自己发心跳
                            continue;
                        }
                        thread_pool->enqueue(&RaftNode::send_append_entries, this, pair.first, append_entries_args);
                    }
                }
            }
            // NOTE: do not hardcoded this value
            std::this_thread::sleep_for(std::chrono::milliseconds(80));
        }
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}