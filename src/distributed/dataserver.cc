#include "distributed/dataserver.h"
#include "common/util.h"
#include "common/bitmap.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
    // 保留用于存储version的block字节数
    // 将存储version的block放在末尾
    auto version_block_bytes = bm->total_blocks() * sizeof(version_t);
    auto total_version_blocks = version_block_bytes / bm->block_size();
    if(version_block_bytes % bm->block_size() != 0) {
      total_version_blocks += 1;
    }

    // 初始化version block，版本号全部置为0
    for(block_id_t i = 0; i < total_version_blocks; ++i) {
      auto version_block_id = bm->total_blocks() - total_version_blocks + i;
      bm->zero_block(version_block_id);
    }

    // 在bitmap上标记version block已使用
    if(bm->total_blocks() % (bm->block_size() * KBitsPerByte) == 0) {
      auto total_version_bitmap_blocks = bm->total_blocks() / (bm->block_size() * KBitsPerByte);
      block_id_t first_version_bitmap_block_id = 
        block_allocator_->first_bitmap_block_id() 
        + block_allocator_->total_bitmap_block() 
        - total_version_bitmap_blocks;

      block_id_t last_version_bitmap_block_id = 
      block_allocator_->first_bitmap_block_id() 
      + block_allocator_->total_bitmap_block() 
      - 1;

      // 此情况下对齐，将每个bitmap block的bit全部置为1
      std::vector<u8> buffer(bm->block_size(), 0xff);
      for(block_id_t i = first_version_bitmap_block_id; i <= last_version_bitmap_block_id; ++i) {
        bm->write_block(i, buffer.data());
      }
    } else {
      auto total_version_bitmap_blocks = bm->total_blocks() / (bm->block_size() * KBitsPerByte) + 1;
      block_id_t first_version_bitmap_block_id = 
        block_allocator_->first_bitmap_block_id() 
        + block_allocator_->total_bitmap_block() 
        - total_version_bitmap_blocks;
      block_id_t last_version_bitmap_block_id = 
        block_allocator_->first_bitmap_block_id() 
        + block_allocator_->total_bitmap_block() 
        - 1;
      // 此情况下非对齐，将第一个bitmap block部分bit置为1，其余全部置为1
      // 将后面的bitmap block全部置为1
      std::vector<u8> buffer(bm->block_size(), 0xff);
      for(block_id_t i = first_version_bitmap_block_id + 1; i <= last_version_bitmap_block_id; ++i) {
        bm->write_block(i, buffer.data());
      }
      // 计算偏移量并将第一个bitmap block部分bit置为1
      auto partial_bits = bm->total_blocks() % (bm->block_size() * KBitsPerByte);
      auto bit_offset = bm->block_size() * KBitsPerByte - partial_bits;
      bm->read_block(first_version_bitmap_block_id, buffer.data());
      Bitmap bitmap(buffer.data(), bm->block_size());
      for(usize i = bit_offset; i < bm->block_size() * KBitsPerByte; ++i) {
        bitmap.set(i);
      }
    }
    
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // 检查version是否匹配
  if(read_block_version(block_id) != version) {
    return {};
  }

  auto block_data = std::vector<u8>(block_allocator_->bm->block_size());
  auto read_block_res = block_allocator_->bm->read_block(block_id, block_data.data());
  if(read_block_res.is_err()) {
    return {};
  }

  return std::vector<u8> (block_data.data() + offset, block_data.data() + offset + len);
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  auto write_partial_block_res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
  return write_partial_block_res.is_ok();
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto allocate_res = block_allocator_->allocate();
  if(allocate_res.is_err()) {
    return {};
  }

  version_t version = read_block_version(allocate_res.unwrap()) + 1;
  update_block_version(allocate_res.unwrap(), version);
  return std::make_pair(allocate_res.unwrap(), version);
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  auto free_res = block_allocator_->deallocate(block_id);
  if(free_res.is_err()) {
    return false;
  }

  return update_block_version(block_id, read_block_version(block_id) + 1);
}

auto DataServer::read_block_version(block_id_t block_id) -> version_t {
  auto retrieve_block_version_pos_res = retrieve_block_version_pos(block_id);
  auto version_block_id = retrieve_block_version_pos_res.first;
  auto version_offset = retrieve_block_version_pos_res.second;

  std::vector<u8> version_block_data(block_allocator_->bm->block_size());
  auto read_version_block_res = block_allocator_->bm->read_block(version_block_id, version_block_data.data());
  if(read_version_block_res.is_err()) {
    return {};
  }
  version_t version = *reinterpret_cast<version_t*>(version_block_data.data() + version_offset);
  return version;
}

auto DataServer::update_block_version(block_id_t block_id, version_t version) -> bool {
  auto retrieve_block_version_pos_res = retrieve_block_version_pos(block_id);
  auto version_block_id = retrieve_block_version_pos_res.first;
  auto version_offset = retrieve_block_version_pos_res.second;

  std::vector<u8> version_block_data(block_allocator_->bm->block_size());
  auto read_version_block_res = block_allocator_->bm->read_block(version_block_id, version_block_data.data());
  if(read_version_block_res.is_err()) {
    return false;
  }
  *reinterpret_cast<version_t*>(version_block_data.data() + version_offset) = version;
  auto write_version_block_res = block_allocator_->bm->write_block(version_block_id, version_block_data.data());
  return write_version_block_res.is_ok();
}

auto DataServer::retrieve_block_version_pos(block_id_t block_id) -> std::pair<block_id_t, usize> {
  auto bm = block_allocator_->bm;

  // 计算version block的id
  auto rhs_version_blocks = 
    (bm->total_blocks() - block_id) * sizeof(version_t) / bm->block_size();

  usize version_offset = 0;
  block_id_t version_block_id = bm->block_size() - rhs_version_blocks;
  if((bm->total_blocks() - block_id) * sizeof(version_t) % bm->block_size() != 0) {
    version_block_id -= 1;
    version_offset = 
      bm->block_size() 
      - (bm->total_blocks() - block_id) * sizeof(version_t) % bm->block_size();
  }

  return std::make_pair(version_block_id, version_offset);
}

} // namespace chfs
