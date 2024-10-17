#include <random>

#include "./common.h"
#include "filesystem/directory_op.h"
#include "gtest/gtest.h"

namespace chfs {

TEST(FileSystemBase, Utilities) {
  std::vector<u8> content;
  std::list<DirectoryEntry> list;

  std::string input(content.begin(), content.end());

  parse_directory(input, list);
  ASSERT_TRUE(list.empty());

  input = append_to_directory(input, "test", 2);
  parse_directory(input, list);

  ASSERT_TRUE(list.size() == 1);

  for (uint i = 0; i < 100; i++) {
    input = append_to_directory(input, "test", i + 2);
  }
  list.clear();
  parse_directory(input, list);
  ASSERT_EQ(list.size(), 1 + 100);
}

TEST(FileSystemBase, TestParseDirectory) {
  std::list<DirectoryEntry> list;

  std::string input =  "file1:1/file2:2/file3:3";
  
  parse_directory(input, list);
  
  ASSERT_TRUE(list.size() == 3);
  auto it = list.begin();
  ASSERT_TRUE((*it).id == 1);
  ASSERT_TRUE((*it).name == "file1");
  ++ it;
  ASSERT_TRUE((*it).id == 2);
  ASSERT_TRUE((*it).name == "file2");

  ++it;
  ASSERT_TRUE((*it).id == 3);
  ASSERT_TRUE((*it).name == "file3");
}

TEST(FileSystemBase, UtilitiesRemove) {
  std::vector<u8> content;
  std::list<DirectoryEntry> list;

  std::string input(content.begin(), content.end());

  for (uint i = 0; i < 100; i++) {
    input = append_to_directory(input, "test" + std::to_string(i), i + 2);
  }

  input = rm_from_directory(input, "test0");
  // std::cout << input << std::endl;

  list.clear();
  parse_directory(input, list);
  ASSERT_EQ(list.size(), 99);

  input = rm_from_directory(input, "test12");
  list.clear();
  parse_directory(input, list);
  ASSERT_EQ(list.size(), 98);
}

TEST(FileSystemBase, TestRemoveFromDir) {
  std::string dirStr = "file1:1/file2:2/file3:3";
  
  auto rm_file1_res = rm_from_directory(dirStr, "file1");
  ASSERT_EQ(rm_file1_res, "file2:2/file3:3");

  auto rm_file2_res = rm_from_directory(dirStr, "file2");
  ASSERT_EQ(rm_file2_res, "file1:1/file3:3");

  auto rm_file3_res = rm_from_directory(dirStr, "file3");
  ASSERT_EQ(rm_file3_res, "file1:1/file2:2");

  auto rm_all_res = rm_from_directory(rm_from_directory(rm_from_directory(dirStr, "file1"), "file2"), "file3");
  ASSERT_EQ(rm_all_res, "");

}

TEST(FileSystemBase, TestAppendToDir) {
  std::string dirStr = "file1:1";

  auto app_file2_res = append_to_directory(dirStr, "file2", 2);
  ASSERT_EQ(app_file2_res, "file1:1/file2:2");
}

TEST(FileSystemTest, DirectOperationAdd) {
  auto bm =
      std::shared_ptr<BlockManager>(new BlockManager(kBlockNum, kBlockSize));
  auto fs = FileOperation(bm, kTestInodeNum);

  auto res = fs.alloc_inode(InodeType::Directory);
  if (res.is_err()) {
    std::cerr << "Cannot allocate inode for root directory. " << std::endl;
    exit(1);
  }
  CHFS_ASSERT(res.unwrap() == 1, "The allocated inode number is incorrect ");

  std::list<DirectoryEntry> list;
  {
    auto res = read_directory(&fs, 1, list);
    ASSERT_TRUE(res.is_ok());
    ASSERT_TRUE(list.empty());
  }
}

TEST(FileSystemTest, TestDirOperation) {
  auto bm = 
    std::shared_ptr<BlockManager>(new BlockManager(kBlockNum, kBlockSize));
  auto fs = FileOperation(bm, kTestInodeNum);

  auto root_inode_res = fs.alloc_inode(InodeType::Directory);
  if (root_inode_res.is_err()) {
    std::cerr << "Cannot allocate inode for root directory. " << std::endl;
    exit(1);
  }

  inode_id_t root_inode_id = root_inode_res.unwrap();
  for(int i = 0;i < 20;++i) {
    auto file_name = "file-" + std::to_string(i);
    auto mk_res = fs.mk_helper(root_inode_id, file_name.c_str(), InodeType::FILE);
    ASSERT_TRUE(mk_res.is_ok());
  }
  
  std::list<DirectoryEntry> list;
  auto read_dir_res = read_directory(&fs, root_inode_id, list);
  ASSERT_TRUE(read_dir_res.is_ok());
  ASSERT_EQ(list.size(), 20);

  auto lookup_file2_res = fs.lookup(root_inode_id, "file-2");
  ASSERT_TRUE(lookup_file2_res.is_ok());

  auto lookup_file33_res = fs.lookup(root_inode_id, "file-33");
  ASSERT_TRUE(lookup_file33_res.is_err());

  for(int i = 18; i >= 0; --i) {
    auto file_name = "file-" + std::to_string(i);
    auto unlink_res = fs.unlink(root_inode_id, file_name.c_str());
    ASSERT_TRUE(unlink_res.is_ok());
  }
  list.clear();
  read_dir_res = read_directory(&fs, root_inode_id, list);
  ASSERT_TRUE(read_dir_res.is_ok());

  auto unlink_res = fs.unlink(root_inode_id, "notexist");
  ASSERT_TRUE(unlink_res.is_err());
}

// TEST(FileSystemTest, TestNestedDir) {
//   auto bm = 
//     std::shared_ptr<BlockManager>(new BlockManager(kBlockNum, kBlockSize));
//   auto fs = FileOperation(bm, kTestInodeNum);

//   auto root_inode_res = fs.alloc_inode(InodeType::Directory);
//   if (root_inode_res.is_err()) {
//     std::cerr << "Cannot allocate inode for root directory. " << std::endl;
//     exit(1);
//   }

//   inode_id_t root_inode_id = root_inode_res.unwrap();

//   fs.mk_helper(root_inode_id, "dir1", InodeType::Directory);


// }

TEST(FileSystemTest, mkdir) {
  auto bm =
      std::shared_ptr<BlockManager>(new BlockManager(kBlockNum, kBlockSize));
  auto fs = FileOperation(bm, kTestInodeNum);

  auto res = fs.alloc_inode(InodeType::Directory);
  if (res.is_err()) {
    std::cerr << "Cannot allocate inode for root directory. " << std::endl;
    exit(1);
  }

  for (uint i = 0; i < 100; i++) {
    auto res = fs.mkdir(1, ("test" + std::to_string(i)).c_str());
    ASSERT_TRUE(res.is_ok());
  }

  std::list<DirectoryEntry> list;
  {
    auto res = read_directory(&fs, 1, list);
    ASSERT_TRUE(res.is_ok());
  }
  ASSERT_EQ(list.size(), 100);
}

} // namespace chfs