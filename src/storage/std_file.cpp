#include "../../include/storage/file.h"
#include <iostream>

// 打开文件，可选择创建新文件
#include <filesystem>

bool StdFile::open(const std::string& filename, bool create) {
  filename_ = filename;

  if (create) {
    // 创建空文件
    std::ofstream(filename, std::ios::binary | std::ios::trunc).close();
  } else if (!std::filesystem::exists(filename)) {
    std::cerr << "File does not exist: " << filename << std::endl;
    return false;  // 文件不存在，返回错误
  }

  // 尝试以读写模式打开
  file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);

  if (!file_.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    perror("Error");
  }

  return file_.is_open();
}
// 创建文件并写入数据
bool StdFile::create(const std::string& filename, const std::vector<uint8_t>& data) {
  if (!open(filename, true)) {
    return false;
  }
  if (!data.empty()) {
    file_.seekp(0);
    file_.write(reinterpret_cast<const char*>(data.data()), data.size());
    file_.flush();
  }
  return file_.good();
}

// 读取数据
std::vector<uint8_t> StdFile::read(size_t offset = 0, size_t length = SIZE_MAX) {
  if (!file_.is_open())
    throw std::runtime_error("File not open");

  size_t file_size = size();
  if (offset >= file_size)
    throw std::out_of_range("Offset is beyond file size");

  size_t               read_size = std::min(length, file_size - offset);
  std::vector<uint8_t> buffer(read_size);

  file_.seekg(offset);
  file_.read(reinterpret_cast<char*>(buffer.data()), read_size);

  buffer.resize(file_.gcount());  // 调整为实际读取大小
  return buffer;
}
// 关闭文件
void StdFile::close() {
  if (file_.is_open()) {
    sync();
    file_.close();
  }
}

// 获取文件大小
size_t StdFile::size() {
  if (!file_.is_open()) {
    throw std::runtime_error("File not open");
  }
  auto current_pos = file_.tellg();
  file_.seekg(0, std::ios::end);
  size_t file_size = file_.tellg();
  file_.seekg(current_pos, std::ios::beg);  // 恢复原位置
  return file_size;
}

// 从指定偏移写入数据
bool StdFile::write(size_t offset, const void* data, size_t size) {
  if (!file_.is_open()) {
    return false;
  }
  file_.seekp(offset, std::ios::beg);
  file_.write(static_cast<const char*>(data), size);
  return file_.good();
}

// 同步数据到磁盘
bool StdFile::sync() {
  if (!file_.is_open()) {
    return false;
  }
  file_.flush();
  return file_.good();
}

// 删除文件
bool StdFile::remove() {
  close();
  return std::remove(filename_.c_str()) == 0;
}
