#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, int, std::string> Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        std::unique_lock<std::mutex> lock(mtx);
        int n_map = map_tasks.size();
        for(int i = 0; i < n_map; ++i) {
            if(!map_tasks[i].first) {
                // 存在未分配的map task，
                // 优先分配map task
                // MAP map_index n_reduce file_name
                map_tasks[i].first = true;
                return std::make_tuple(MAP, i, reduce_tasks.size(), files[i]);
            }
        }

        // 所有map task都分配完
        for(int i = 0;i < n_map; ++i) {
            if(!map_tasks[i].second) {
                // 存在未完成的map task,
                // 暂停分配
                return std::make_tuple(NONE, -1, -1, "");
            }
        }

        // 所有map task 都完成
        int n_reduce = reduce_tasks.size();
        for(int i = 0; i < n_reduce; ++i) {
            if(!reduce_tasks[i].first) {
                // 存在未分配的reduce task，
                // 分配reduce task
                // REDUCE reduce_index n_files ""
                reduce_tasks[i].first = true;
                return std::make_tuple(REDUCE, i, files.size(), "");
            }
        }

        // 没有任务可以分配
        return std::make_tuple(NONE, -1, n_reduce, "");
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        std::unique_lock<std::mutex> lock(mtx);
        if(taskType == MAP) {
            map_tasks[index].second = true;
        } else if(taskType == REDUCE) {
            reduce_tasks[index].second = true;
        }

        // 检查任务列表，并更新isFinished
        isFinished = true;
        for(const auto &map_task: map_tasks) {
            if(!map_task.second) {
                isFinished = false;
                break;
            }
        }
        for(const auto &reduce_task: reduce_tasks) {
            if(!reduce_task.second) {
                isFinished = false;
                break;
            }
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        map_tasks = std::vector<std::pair<bool, bool>>(files.size(), std::make_pair(false, false));
        reduce_tasks = std::vector<std::pair<bool, bool>>(nReduce, std::make_pair(false, false));
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}