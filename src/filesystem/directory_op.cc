#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs
{

  /**
   * Some helper functions
   */
  auto string_to_inode_id(std::string &data) -> inode_id_t
  {
    std::stringstream ss(data);
    inode_id_t inode;
    ss >> inode;
    return inode;
  }

  auto inode_id_to_string(inode_id_t id) -> std::string
  {
    std::stringstream ss;
    ss << id;
    return ss.str();
  }

  // {Your code here}
  auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
      -> std::string
  {
    std::ostringstream oss;
    usize cnt = 0;
    for (const auto &entry : entries)
    {
      oss << entry.name << ':' << entry.id;
      if (cnt < entries.size() - 1)
      {
        oss << '/';
      }
      cnt += 1;
    }
    return oss.str();
  }

  // {Your code here}
  auto append_to_directory(std::string src, std::string filename, inode_id_t id)
      -> std::string
  {
    if (src.empty())
    {
      src = filename + ":" + std::to_string(id);
    }
    else
    {
      src += "/" + filename + ":" + std::to_string(id);
    }
    return src;
  }

  // {Your code here}
  void parse_directory(std::string &src, std::list<DirectoryEntry> &list)
  {
    size_t idx = 0;
    size_t len = src.size();
    std::string name;
    inode_id_t inode_id;

    while (idx < len)
    {
      auto find_res = src.find_first_of(':', idx);
      if (find_res == std::string::npos)
      {
        break;
      }
      name = src.substr(idx, find_res - idx);

      idx = find_res + 1;
      assert(idx < len);

      find_res = src.find_first_of('/', idx);
      inode_id = std::stol(src.substr(idx, find_res - idx));

      list.push_back(DirectoryEntry({name, inode_id}));

      if (find_res == std::string::npos)
      {
        break;
      }
      idx = find_res + 1;
    }
  }

  // {Your code here}
  auto rm_from_directory(std::string src, std::string filename) -> std::string
  {

    auto res = std::string("");
    size_t filename_idx = 0;
    while (true)
    {
      filename_idx = src.find_first_of(filename + ":", filename_idx);
      assert(filename_idx != std::string::npos);
      if (filename_idx == 0 || src[filename_idx - 1] == '/')
      {
        // 找到了文件条目
        break;
      }
    }

    size_t slash_idx = src.find_first_of('/', filename_idx);
    assert(slash_idx != std::string::npos);
    src.erase(filename_idx, slash_idx - filename_idx);
    res = std::move(src);
    return res;
  }

  /**
   * { Your implementation here }
   */
  auto read_directory(FileOperation *fs, inode_id_t id,
                      std::list<DirectoryEntry> &list) -> ChfsNullResult
  {
    auto read_res = fs->read_file(id);
    if (read_res.is_err())
    {
      return ChfsNullResult(read_res.unwrap_error());
    }

    auto buffer = read_res.unwrap();
    auto bufferStr = std::string(buffer.begin(), buffer.end());
    parse_directory(bufferStr, list);

    return KNullOk;
  }

  // {Your code here}
  auto FileOperation::lookup(inode_id_t id, const char *name)
      -> ChfsResult<inode_id_t>
  {
    std::list<DirectoryEntry> list;

    auto read_dir_res = read_directory(this, id, list);
    if (read_dir_res.is_err())
    {
      return ChfsResult<inode_id_t>(read_dir_res.unwrap_error());
    }

    auto it = std::find_if(list.begin(), list.end(), [name](const DirectoryEntry &entry)
                           { return entry.name == name; });

    if (it != list.end())
    {
      return ChfsResult<inode_id_t>((*it).id);
    }

    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }

  // {Your code here}
  auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
      -> ChfsResult<inode_id_t>
  {

    // TODO:
    // 1. Check if `name` already exists in the parent.
    //    If already exist, return ErrorType::AlreadyExist.
    auto list = std::list<DirectoryEntry>();
    auto read_dir_res = read_directory(this, id, list);
    if (read_dir_res.is_err())
    {
      return ChfsResult<inode_id_t>(read_dir_res.unwrap_error());
    }

    auto it = std::find_if(list.begin(), list.end(), [name](const DirectoryEntry &entry)
                           { return entry.name == name; });
    if (it != list.end())
    {
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }

    // 2. Create the new inode.
    auto allocate_block_res = this->block_allocator_->allocate();
    if (allocate_block_res.is_err())
    {
      return ChfsResult<inode_id_t>(allocate_block_res.unwrap_error());
    }
    auto allocate_inode_res = this->inode_manager_->allocate_inode(type, allocate_block_res.unwrap());
    if (allocate_inode_res.is_err())
    {
      return ChfsResult<inode_id_t>(allocate_inode_res.unwrap_error());
    }

    // 3. Append the new entry to the parent directory.
    auto read_dir_file_res = this->read_file(id);
    if (read_dir_file_res.is_err())
    {
      return ChfsResult<inode_id_t>(read_dir_file_res.unwrap_error());
    }
    auto buffer = read_dir_file_res.unwrap();
    auto dir_src = std::string(buffer.begin(), buffer.end());

    dir_src = append_to_directory(dir_src, name, allocate_inode_res.unwrap());
    auto write_dir_file_res = this->write_file(id, std::vector<u8>(dir_src.begin(), dir_src.end()));
    if (write_dir_file_res.is_err())
    {
      return ChfsResult<inode_id_t>(write_dir_file_res.unwrap_error());
    }

    return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
  }

  // {Your code here}
  auto FileOperation::unlink(inode_id_t parent, const char *name)
      -> ChfsNullResult
  {

    // TODO:
    // 1. Remove the file, you can use the function `remove_file`
    auto lookup_res = this->lookup(parent, name);
    if (lookup_res.is_err())
    {
      return ChfsNullResult(lookup_res.unwrap_error());
    }
    this->remove_file(lookup_res.unwrap());

    // 2. Remove the entry from the directory.
    auto read_dir_file_res = this->read_file(parent);
    if (read_dir_file_res.is_err())
    {
      return ChfsNullResult(read_dir_file_res.unwrap_error());
    }
    auto buffer = read_dir_file_res.unwrap();
    auto dir_src = std::string(buffer.begin(), buffer.end());
    dir_src = rm_from_directory(dir_src, name);

    auto write_dir_file_res = this->write_file(parent, std::vector<u8>(dir_src.begin(), dir_src.end()));
    if (write_dir_file_res.is_err())
    {
      return ChfsNullResult(write_dir_file_res.unwrap_error());
    }

    return KNullOk;
  }

} // namespace chfs
