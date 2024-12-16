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
    int last_applied = 0; 

    int vote_count = 0; 

    std::atomic<std::chrono::time_point<std::chrono::system_clock>> last_heartbeat_time = std::chrono::system_clock::now();

    // 内存中的日志列表，每个日志条目包含<term, command> 

    std::vector<std::pair<int, Command>> log_entries; 

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
    auto node_log_filename = "/tmp/raft_log/" + std::to_string(my_id);

    if(is_file_exist(node_log_filename)) {
        // 从持久化存储中加载数据
        auto bm = std::make_shared<BlockManager>(node_log_filename);
        log_storage = std::make_unique<RaftLog<Command>>(bm);
        
        current_term = log_storage->load_current_term();
        voted_for = log_storage->load_voted_for();
        log_entries = log_storage->load_log_entries();
    } else {
        // 初始化持久化存储
        auto bm = std::make_shared<BlockManager>(node_log_filename);
        log_storage = std::make_unique<RaftLog<Command>>(bm);

        assert(current_term == 0);
        log_storage->store_current_term(current_term);
        assert(voted_for == -1);
        log_storage->store_voted_for(voted_for);

        // NOTE: 由于日志index从1开始，所以这里插入一个空的log entry
        log_entries.push_back(std::make_pair(0, Command()));
        log_storage->store_log_entries(log_entries);
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
    
    auto log_entry = std::make_pair(current_term, Command());
    log_entry.second.deserialize(cmd_data, cmd_size);

    log_entries.push_back(std::move(log_entry));
    log_storage->store_log_entries(log_entries);

    RAFT_LOG("new_cmd\tlog entry added, log index: %d", static_cast<int>(log_entries.size() - 1));

    return std::make_tuple(true, current_term, log_entries.size() - 1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return std::vector<u8>();
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

        if(args.last_log_term > log_entries.back().first
         || (args.last_log_term == log_entries.back().first && args.last_log_index >= log_entries.size() - 1) ){
            RAFT_LOG("req_vote\tvote granted for %d", args.candidate_id);
            return RequestVoteReply{current_term, true};
        } else {
            RAFT_LOG("req_vote\tvote denied. candidate's log is not up-to-date");
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
            std::fill(next_index.begin(), next_index.end(), log_entries.size());

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
    // RAFT_LOG("app_ent\ttid: %lu", std::hash<std::thread::id>{}(std::this_thread::get_id()));
    if(rpc_arg.term < current_term) {
        // 旧的任期，忽略
        RAFT_LOG("app_ent\tterm too old");
        // RAFT_LOG("app_ent\tcurrent_term %d, rpc_arg.term %d", current_term, rpc_arg.term);
        return AppendEntriesReply{current_term, false};
    }
    if(rpc_arg.term > current_term) {
        // 任期更新
        RAFT_LOG("app_ent\tterm update and become follower");

        log_storage->store_current_term(rpc_arg.term);
        current_term = rpc_arg.term;

        role = RaftRole::Follower;
        leader_id = rpc_arg.leader_id;

        log_storage->store_voted_for(-1);
        voted_for = -1;
    }

    if(role == RaftRole::Candidate) {
        RAFT_LOG("app_ent\tcandidate -> follower");
        // 当前是candidate，
        // 收到心跳或者追加日志请求，
        // 转为follower
        role = RaftRole::Follower;
        leader_id = rpc_arg.leader_id;
    }

    

    if(rpc_arg.entries.size() == 0) {
        // 接收leader心跳
        RAFT_LOG("app_ent\tupd last heartbeat");
        last_heartbeat_time = std::chrono::system_clock::now();
    } else {
        // 接收leader的追加日志请求
        RAFT_LOG("app_ent\treceive append entries from %d", rpc_arg.leader_id);
        if(rpc_arg.prev_log_index >= log_entries.size()) {
            // 来自的prev_log_index位置不存在log entry
            RAFT_LOG("app_ent\treject cuz no such log entry at %d", rpc_arg.prev_log_index);
            return AppendEntriesReply{current_term, false};
        }

        if(log_entries[rpc_arg.prev_log_index].first != rpc_arg.prev_log_term) {
            // 来自leader的prev_log_index位置的term与自己的term不一致
            RAFT_LOG("app_ent\treject cuz term not match, %d != %d", log_entries[rpc_arg.prev_log_index].first, rpc_arg.prev_log_term);
            return AppendEntriesReply{current_term, false};
        }

        // 直接覆盖prev_log_index之后的log entry
        int log_entries_final_size = rpc_arg.entries.size();
        log_entries.resize(log_entries_final_size);
        for(int i = rpc_arg.prev_log_index + 1; i < log_entries_final_size; ++ i) {
            auto &rpc_entry = rpc_arg.entries[i];
            auto entry = std::make_pair(rpc_entry.first, Command());
            entry.second.deserialize(rpc_entry.second, rpc_entry.second.size());
            log_entries[i] = std::move(entry);
        }
        log_storage->store_log_entries(log_entries);
    }

    // 更新 commit index
    if(rpc_arg.leader_commit > commit_index) {
        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) 
        commit_index = std::min<int>(rpc_arg.leader_commit, log_entries.size() - 1);
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
        next_index[node_id] = arg.entries.size();
        match_index[node_id] = next_index[node_id] - 1;
        RAFT_LOG("send_app_ent\tappend success, upd next_index to %d, match_index to %d", next_index[node_id], match_index[node_id]);
    } else {
        // AppendEntries失败，减小next_index，重试
        RAFT_LOG("send_app_ent\tappend fail, retry");
        auto new_arg = arg;
        -- next_index[node_id];
        new_arg.prev_log_index = next_index[node_id] - 1;
        new_arg.prev_log_term = log_entries[new_arg.prev_log_index].first;
        thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, new_arg);
    }
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    return;
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
                // TODO: 直接sleep，等变成follower再唤醒这个线程
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
                    request_vote_args.last_log_index = log_entries.size() - 1;
                    request_vote_args.last_log_term = log_entries.back().first;
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
                
                AppendEntriesArgs<Command> args;
                int last_entry_index;
                {
                    std::unique_lock<std::mutex> lock(mtx);

                    args.term = current_term;
                    args.leader_id = my_id;
                    args.entries = log_entries;
                    args.leader_commit = commit_index;

                    last_entry_index = log_entries.size() - 1;
                }
                {
                    // RAFT_LOG("bg_commit\ttry sync log entries");
                    // 发送给除了自己以外的所有节点(follwer and candidate)
                    for(auto &node_id: node_ids) {
                        if(node_id == my_id) {
                            continue;
                        }
                        {
                            std::unique_lock<std::mutex> lock(mtx);
                            if(last_entry_index < next_index[node_id]) {
                                continue;
                            }
                            args.prev_log_index = next_index[node_id] - 1;
                            args.prev_log_term = log_entries[args.prev_log_index].first;
                        }

                        RAFT_LOG("bg_commit\tsend append entries to %d", node_id);
                        RAFT_LOG("bg_commit\targs: term %d, leader_id %d, prev_log_index %d, prev_log_term %d, entries size %lu, leader_commit %d", args.term, args.leader_id, args.prev_log_index, args.prev_log_term, args.entries.size(), args.leader_commit);
                        
                        thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, args);
                    }
                }
                
                RAFT_LOG("bg_commit\tsleep to wait");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                
                // 更新leader的commit index
                {
                    RAFT_LOG("bg_commit\ttry upd leader commit index");
                    std::unique_lock<std::mutex> lock(mtx);
                    //NOTE: 需要更新match_index[leader_id]
                    match_index[my_id] = log_entries.size() - 1;

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
                        if(log_entries[ci].first == current_term) {
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
                    state->apply_log(log_entries[i].second);
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