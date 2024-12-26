#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  auto inode_id = res.unwrap()->as<inode_id_t>();
  if(inode_id == KInvalidInodeID) {
    return ChfsResult<inode_id_t>(ErrorType::BadResponse);
  } else {
    return ChfsResult<inode_id_t>(inode_id);
  }
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto res = metadata_server_->call("unlink", parent, name);
  if(res.unwrap()->as<bool>()) {
    return KNullOk;
  } else {
    return ChfsNullResult(ErrorType::BadResponse);
  }
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto res = metadata_server_->call("lookup", parent, name);
  auto inode_id = res.unwrap()->as<inode_id_t>();
  if(inode_id == KInvalidBlockID) {
    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  } else {
    return ChfsResult<inode_id_t>(inode_id);
  }
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto res = metadata_server_->call("readdir", id);
  auto dir_list = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return dir_list;
}

// {Your code here}
// a tuple of <size, atime, mtime, ctime, type>
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto res = metadata_server_->call("get_type_attr", id)
    .unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();


  // std::cout << "atime: " << std::get<1>(res) << std::endl;
  // std::cout << "mtime: " << std::get<2>(res) << std::endl;
  // std::cout << "ctime: " << std::get<3>(res) << std::endl;
  // std::cout << "size: " << std::get<0>(res) << std::endl;

  return std::make_pair(static_cast<InodeType>(std::get<4>(res)), 
                        FileAttr{
                          .atime = std::get<1>(res),
                          .mtime = std::get<2>(res),
                          .ctime = std::get<3>(res),
                          .size = std::get<0>(res),
                        });
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  auto block_size = metadata_server_->call("get_block_size")
    .unwrap()->as<usize>();
  auto block_map = metadata_server_->call("get_block_map", id)
    .unwrap()->as<std::vector<BlockInfo>>();
  std::vector<u8> data;
  data.reserve(size);

  auto read_set = cal_nonempty_data_sequence(offset, size, block_size);

  // print read_set
  // std::cout << "read_set: " << std::endl;
  // for(auto &read_op: read_set) {
  //   std::cout << std::get<0>(read_op) << " " << std::get<1>(read_op) << " " << std::get<2>(read_op) << std::endl;
  // }

  for(auto &read_op: read_set) {
    auto block_info = block_map[std::get<0>(read_op)];
    auto block_id = std::get<0>(block_info);
    auto mac_id = std::get<1>(block_info);
    auto version = std::get<2>(block_info);
    
    auto res = data_servers_[mac_id]->call("read_data", block_id, std::get<1>(read_op), std::get<2>(read_op), version);
    auto block_data = res.unwrap()->as<std::vector<u8>>();
    if(block_data.empty()) {
      return ChfsResult<std::vector<u8>>(ErrorType::BadResponse);
    }

    data.insert(data.end(), block_data.begin(), block_data.end());
  }

  return ChfsResult<std::vector<u8>>(data);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  assert(data.size() > 0);

  auto block_size = metadata_server_->call("get_block_size")
    .unwrap()->as<usize>();
  auto size = data.size();
  auto block_map = metadata_server_->call("get_block_map", id)
    .unwrap()->as<std::vector<BlockInfo>>();
  auto block_map_size = block_map.size();

  // 使用一个data cursor将data分段写入
  auto data_cursor = data.begin();

  auto write_set = cal_nonempty_data_sequence(offset, size, block_size);

  // print write_set
  // std::cout << "write_set: " << std::endl;
  for(auto &write_op: write_set) {
    std::cout << std::get<0>(write_op) << " " << std::get<1>(write_op) << " " << std::get<2>(write_op) << std::endl;
  }

  for(auto &write_op: write_set) {
    // 如果block不存在，向metadata server申请一个block
    if(std::get<0>(write_op) >= block_map_size) {
      auto res = metadata_server_->call("alloc_block", id);
      if(res.is_err()) {
        return ChfsNullResult(ErrorType::BadResponse);
      }
      block_map.push_back(res.unwrap()->as<BlockInfo>());
    }

    auto block_info = block_map[std::get<0>(write_op)];
    auto block_id = std::get<0>(block_info);
    auto mac_id = std::get<1>(block_info);

    // 写入数据
    auto res = data_servers_[mac_id]->call("write_data", 
                                            block_id,
                                            std::get<1>(write_op),
                                            std::vector<u8>(data_cursor, data_cursor + std::get<2>(write_op)));
    if(!res.unwrap()->as<bool>()) {
      std::cerr << "cannot write to data server" << std::endl;
      return ChfsNullResult(ErrorType::BadResponse);
    }

    data_cursor += std::get<2>(write_op);
  }

  // 更新文件大小
  // TODO: 将更新文件大小的逻辑移到外面，防止影响lab2测试
  auto set_size_res = metadata_server_->call("set_file_size", id, offset + size);
  if(set_size_res.is_err()) {
    return ChfsNullResult(ErrorType::BadResponse);
  }

  // std::cout << "client successfully write data" << std::endl;
  return KNullOk;
}


// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto res = metadata_server_->call("free_file_block", id, block_id, mac_id);
  if(res.unwrap()->as<bool>()) {
    return KNullOk;
  } else {
    return ChfsNullResult(ErrorType::BadResponse);
  }
}

auto ChfsClient::cal_nonempty_data_sequence(usize offset, usize size, usize block_size) -> std::vector<std::tuple<usize, usize, usize>>
{
  auto offset_left = offset % block_size;
  auto idx_left = offset / block_size;
  auto offset_right = (offset + size) % block_size;
  auto idx_right = (offset + size) / block_size;

  std::vector<std::tuple<usize, usize, usize>> res;

  if(idx_left == idx_right) {
    // 单个block范围之内
    res.push_back(std::make_tuple(idx_left, offset_left, size));
  } else {
    // 第一个block
    assert(block_size - offset_left > 0);
    res.push_back(std::make_tuple(idx_left, offset_left, block_size - offset_left));

    // 读取中间完整的block(optional)
    for(usize i = idx_left + 1; i < idx_right; ++i) {
      res.push_back(std::make_tuple(i, 0, block_size));
    }

    // 读取最后一个block(optional)
    if(offset_right > 0) {
      res.push_back(std::make_tuple(idx_right, 0, offset_right));
    }
  }

  return res;
}
} // namespace chfs