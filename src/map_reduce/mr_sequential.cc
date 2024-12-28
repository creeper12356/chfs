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
        for (auto file : files) {
            auto file_inode_id = chfs_client->lookup(1, file).unwrap();
            auto file_type_attr = chfs_client->get_type_attr(file_inode_id).unwrap();
            auto content_byte_arr = chfs_client->read_file(file_inode_id, 0, file_type_attr.second.size).unwrap();

            auto content = std::string(content_byte_arr.begin(), content_byte_arr.end());

            std::vector<KeyVal> kvs = Map(content);
            for(int i = 0;i < kvs.size(); ++i) {
                key_vals.push_back(kvs[i]);
            }
        }
        
        mapReduce::sort(key_vals);

        std::map<std::string, std::vector<std::string>> key_vals_map;
        for(int i = 0; i < key_vals.size(); ++i) {
            auto key_val = key_vals[i];
            if(key_vals_map.find(key_val.key) == key_vals_map.end()) {
                key_vals_map[key_val.key] = std::vector<std::string>({key_val.val});
            } else {
                key_vals_map[key_val.key].resize(key_vals_map[key_val.key].size() + 1);
                key_vals_map[key_val.key][key_vals_map[key_val.key].size() - 1] = key_val.val;
            }
        }

        std::string work_res = "";
        for(auto it = key_vals_map.begin(); it != key_vals_map.end(); ++it) {
            auto key = it->first;
            std::string reduce_res = Reduce(key, key_vals_map[key]);
            work_res += reduce_res;
        }

        auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_file_inode_id, 0, std::vector<chfs::u8>(work_res.begin(), work_res.end())).unwrap();

    }
}