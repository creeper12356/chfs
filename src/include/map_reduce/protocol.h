#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        std::string val;
    };

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE,
        MERGE
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        std::tuple<int, int, int, std::string> askTask(int);
        int submitTask(int taskType, int index);
        bool Done();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;
        
        /**
         * @brief map任务状态
         * 
         * 大小：files.size()
         * ele.first: 是否已分配
         * ele.second: 是否完成
         * 未分配一定未完成
         * 
         */
        std::vector<std::pair<bool, bool>> map_tasks;

        /**
         * @brief reduce任务状态
         * 
         * 大小：nReduce
         * ele.first: 是否已分配
         * ele.second: 是否完成
         * 未分配一定未完成
         */
        std::vector<std::pair<bool, bool>> reduce_tasks;

        /**
         * @brief 合并任务
         * 
         */
        std::pair<bool, bool> merge_task;
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doMerge(int n_reduce);
        void doSubmit(mr_tasktype taskType, int index);

        std::string outPutFile; //* reduce worker 输出文件名
        std::unique_ptr<chfs::RpcClient> mr_client; //* 向coordinator发送请求的客户端
        std::shared_ptr<chfs::ChfsClient> chfs_client; //* 分布式文件系统客户端
        std::unique_ptr<std::thread> work_thread; //* 后台工作线程
        bool shouldStop = false; //* 是否停止工作

        int n_map; 
        int n_reduce;
        int worker_id;

        static int worker_cnt;
    };


    auto cmp1 = [](const KeyVal &a, const KeyVal &b) {
        return a.key < b.key;
    };
    auto cmp2 = [](const KeyVal &a, const KeyVal &b) {
        return a.key > b.key;
    };
    auto sort = [](std::vector<KeyVal> &key_vals) {
        int k = 4;
        while(k > 0) {
            std::sort(key_vals.begin(), key_vals.end(), cmp1);
            std::sort(key_vals.begin(), key_vals.end(), cmp2);
            -- k;
        }
        std::sort(key_vals.begin(), key_vals.end(), cmp1);
    };
}