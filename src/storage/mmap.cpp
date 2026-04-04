#include "../../include/storage/mmap.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>

bool MmapFile::open(const std::string& filename, bool create) {
  filename_     = filename;
  int open_mode = O_RDWR | (create ? O_CREAT : 0);
  fd_           = ::open(filename.c_str(), open_mode, 0644);
  if (fd_ < 0) {
    perror("open");
    return false;
  }

  struct stat st;
  if (fstat(fd_, &st) < 0) {
    perror("fstat");
    close();
    return false;
  }
  file_size_ = st.st_size;
  if (file_size_ > 0) {
    mapped_data_ = mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (mapped_data_ == MAP_FAILED) {
      perror("mmap");
      close();
      return false;
    }
  } else {
    mapped_data_ = nullptr;
  }
  return true;
}

bool MmapFile::create(const std::string& filename, const std::vector<uint8_t>& buf) {
  if (!create_and_map(filename, buf.size())) {
    std::cerr << "Failed to create or map file: " << filename << std::endl;
    return false;
  }
  std::memcpy(data(), buf.data(), buf.size());
  sync();
  return true;
}

void MmapFile::close() {
  if (mapped_data_ && mapped_data_ != MAP_FAILED) {
    munmap(mapped_data_, file_size_);
    mapped_data_ = nullptr;
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  file_size_ = 0;
}

bool MmapFile::write(size_t offset, const void* data, size_t size) {
  size_t new_size = offset + size;
  if (ftruncate(fd_, new_size) < 0) {
    perror("ftruncate");
    return false;
  }
  if (mapped_data_ && mapped_data_ != MAP_FAILED) {
    munmap(mapped_data_, file_size_);
  }
  file_size_   = new_size;
  mapped_data_ = mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (mapped_data_ == MAP_FAILED) {
    perror("mmap");
    mapped_data_ = nullptr;
    return false;
  }
  std::memcpy(static_cast<uint8_t*>(mapped_data_) + offset, data, size);
  sync();
  return true;
}

std::vector<uint8_t> MmapFile::read(size_t offset, size_t length) {
  // 检查映射内存有效性
  if (mapped_data_ == nullptr || mapped_data_ == MAP_FAILED || offset >= file_size_) {
    return {};  // 返回空vector
  }

  // 修正读取长度，防止越界
  size_t               max_length = std::min(length, file_size_ - offset);
  std::vector<uint8_t> result(max_length);

  // 复制数据
  const uint8_t* data = static_cast<const uint8_t*>(this->data());
  memcpy(result.data(), data + offset, max_length);

  return result;
}

bool MmapFile::sync() {
  // 刷新映射到磁盘
  if (mapped_data_ != nullptr && mapped_data_ != MAP_FAILED) {
    return msync(mapped_data_, file_size_, MS_SYNC) == 0;
  }
  return true;  // 没有映射时，视为同步成功
}
bool MmapFile::create_and_map(const std::string& path, size_t size) {
  // 创建并打开文件（可读写、创建、截断）
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd_ == -1) {
    return false;
  }

  // 设置文件大小
  if (ftruncate(fd_, size) == -1) {
    ::close(fd_);
    fd_ = -1;
    return false;
  }

  // 映射文件到内存
  mapped_data_ = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (mapped_data_ == MAP_FAILED) {
    ::close(fd_);
    fd_          = -1;
    mapped_data_ = nullptr;
    return false;
  }

  file_size_ = size;
  return true;
}