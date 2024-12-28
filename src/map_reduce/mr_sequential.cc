#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> key_vals;
        for (const auto &file : files) {
            auto file_inode_id = chfs_client->lookup(1, file).unwrap();
            auto file_type_attr = chfs_client->get_type_attr(file_inode_id).unwrap();
            auto content_byte_arr = chfs_client->read_file(file_inode_id, 0, file_type_attr.second.size).unwrap();

            auto content = std::string(content_byte_arr.begin(), content_byte_arr.end());

            std::vector<KeyVal> kvs = Map(content);
            key_vals.insert(key_vals.end(), kvs.begin(), kvs.end());
        }

        std::sort(key_vals.begin(), key_vals.end(), [](const KeyVal &a, const KeyVal &b) {
            return a.key < b.key;
        });
        // 冒泡排序
        // int key_vals_size = keyVals.size();
        // for(int i = 0; i < key_vals_size; ++i) {
        //     for(int j = 0; j < key_vals_size - i - 1; ++j) {
        //         if(keyVals[j].key > keyVals[j + 1].key) {
        //             std::swap(keyVals[j], keyVals[j + 1]);
        //         }
        //     }
        // }

        std::string last_key;
        std::vector<std::string> values;
        std::string work_res = "";
        for (const auto &kv : key_vals) {
            if (kv.key != last_key) {
                if (!last_key.empty()) {
                    std::string reduce_res = Reduce(last_key, values);
                    work_res += reduce_res;
                }
                last_key = kv.key;
                values.clear();
            }
            values.push_back(kv.val);
        }
        if (!last_key.empty()) {
            std::string res = Reduce(last_key, values);
            work_res += res;
        }

        auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_file_inode_id, 0, std::vector<chfs::u8>(work_res.begin(), work_res.end())).unwrap();

    }
}