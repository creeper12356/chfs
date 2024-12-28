#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

#define MR_WK_LOG(fmt, args...)                                                                                   \
    {auto now =                                                                                               \
        std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
            std::chrono::system_clock::now().time_since_epoch())                                             \
            .count();                                                                                        \
    printf("[%ld][%s:%d][%d] " fmt "\n", now, __FILE__, __LINE__, worker_id, ##args);}
namespace mapReduce {
    int Worker::worker_cnt = 0;
    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
        worker_id = worker_cnt;
        ++ worker_cnt;
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        MR_WK_LOG("doMap index: %d filename: %s", index, filename.c_str());
        auto file_inode_id = chfs_client->lookup(1, filename).unwrap();
        auto file_type_attr = chfs_client->get_type_attr(file_inode_id).unwrap();
        auto content_byte_arr = chfs_client->read_file(file_inode_id, 0, file_type_attr.second.size).unwrap();

        auto content = std::string(content_byte_arr.begin(), content_byte_arr.end());
        auto key_vals = Map(content);

        // 划分键值对
        auto partitioned_key_vals = std::vector<std::vector<KeyVal>>(n_reduce);
        for (const auto &key_val: key_vals) {
            auto hash = std::hash<std::string>{}(key_val.key);
            auto reduce_index = hash % n_reduce;

            partitioned_key_vals[reduce_index].push_back(key_val);
        }

        // 写入中间文件，中间文件名mr-X-Y
        // X: Map task number
        // Y: Reduce task number
        for(int reduce_index = 0; reduce_index < n_reduce; ++ reduce_index) {
            std::string if_name = "mr-" + std::to_string(index) + "-" + std::to_string(reduce_index);
            auto if_inode_id = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, if_name).unwrap();
            std::string if_content;
            // 每个中间文件格式：
            // key1 val1
            // key2 val2
            // ...
            for(const auto &key_val: partitioned_key_vals[reduce_index]) {
                if_content += key_val.key + " " + key_val.val + "\n";
            }

            MR_WK_LOG("write if name: %s, if size: %lu", if_name.c_str(), if_content.size());
            chfs_client->write_file(if_inode_id, 0, std::vector<chfs::u8>(if_content.begin(), if_content.end())).unwrap();
        }
        
    }

    void Worker::doReduce(int index, int nfiles) {
        // TODO: sort ?
        MR_WK_LOG("doReduce index: %d nfiles: %d", index, nfiles); 
        std::unordered_map<std::string, std::vector<std::string>> key_vals;
        for(int map_index = 0; map_index < nfiles; ++ map_index) {
            std::string if_name = "mr-" + std::to_string(map_index) + "-" + std::to_string(index);
            auto if_inode_id = chfs_client->lookup(1, if_name).unwrap();
            auto if_type_attr = chfs_client->get_type_attr(if_inode_id).unwrap();
            MR_WK_LOG("read if name: %s, if size: %lu", if_name.c_str(), if_type_attr.second.size);
            auto if_content_byte_arr = chfs_client->read_file(if_inode_id, 0, if_type_attr.second.size).unwrap();
            auto if_content = std::string(if_content_byte_arr.begin(), if_content_byte_arr.end());

            std::istringstream if_stream(if_content);
            std::string line;
            while(std::getline(if_stream, line)) {
                std::istringstream line_stream(line);
                std::string key, val;
                line_stream >> key >> val;
                MR_WK_LOG("key: %s val: %s", key.c_str(), val.c_str());
                key_vals[key].push_back(val);
            }
        }

        std::string work_res;
        for(const auto &key_val: key_vals) {
            MR_WK_LOG("key: %s", key_val.first.c_str());
            MR_WK_LOG("val cnt: %lu", key_val.second.size());
            auto reduce_res = Reduce(key_val.first, key_val.second);
            work_res += reduce_res;
        }

        auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        auto append_file_res = chfs_client->append_file(output_file_inode_id, std::vector<chfs::u8>(work_res.begin(), work_res.end()));
        if(append_file_res.is_err()) {
            MR_WK_LOG("append file error");
        }
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        MR_WK_LOG("finish %s job and submit ", taskType == MAP ? "MAP" : "REDUCE");
        mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            auto asked_task = mr_client->call(ASK_TASK, 0).unwrap()->as<std::tuple<int, int, int, std::string>>();
            auto task_type = static_cast<mr_tasktype>(std::get<0>(asked_task));
            if(task_type == NONE) {
                continue;
            }
            if(task_type == MAP) {
                auto map_index = std::get<1>(asked_task);
                n_reduce = std::get<2>(asked_task);
                auto file_name = std::get<3>(asked_task);
                doMap(map_index, file_name);
                doSubmit(MAP, map_index);
            } else {
                // task_type == REDUCE
                auto reduce_index = std::get<1>(asked_task);
                n_map = std::get<2>(asked_task);
                doReduce(reduce_index, n_map);
                doSubmit(REDUCE, reduce_index);
            }
        }
    }
}