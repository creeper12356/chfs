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
        std::vector<KeyVal> keyVals;
        for (const auto &file : files) {
            auto file_inode_id = chfs_client->lookup(1, file).unwrap();
            auto file_type_attr = chfs_client->get_type_attr(file_inode_id).unwrap();
            auto content_byte_arr = chfs_client->read_file(file_inode_id, 0, file_type_attr.second.size).unwrap();

            auto content = std::string(content_byte_arr.begin(), content_byte_arr.end());

            std::vector<KeyVal> kvs = Map(content);
            keyVals.insert(keyVals.end(), kvs.begin(), kvs.end());
        }

        std::sort(keyVals.begin(), keyVals.end(), [](const KeyVal &a, const KeyVal &b) {
            return a.key < b.key;
        });

        std::string lastKey;
        std::vector<std::string> values;
        std::string work_res = "";
        for (const auto &kv : keyVals) {
            if (kv.key != lastKey) {
                if (!lastKey.empty()) {
                    std::string reduce_res = Reduce(lastKey, values);
                    work_res += reduce_res;
                }
                lastKey = kv.key;
                values.clear();
            }
            values.push_back(kv.val);
        }
        if (!lastKey.empty()) {
            std::string res = Reduce(lastKey, values);
            work_res += res;
        }

        auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_file_inode_id, 0, std::vector<chfs::u8>(work_res.begin(), work_res.end())).unwrap();

    }
}