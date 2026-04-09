#include <fstream>
#include <iostream>
#include <vector>
#include <cstdint>
#include <cstring>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: dump_blocks file\n";
    return 1;
  }
  std::ifstream f(argv[1], std::ios::binary);
  if (!f) {
    std::cerr << "open fail\n";
    return 2;
  }
  f.seekg(0, std::ios::end);
  size_t filesize = f.tellg();
  std::cout << "filesize=" << filesize << "\n";
  f.seekg(0);
  std::vector<uint8_t> buf(filesize);
  f.read(reinterpret_cast<char*>(buf.data()), buf.size());
  // 简单尝试：从 file start 扫描寻找 uint16_t num_elements near end of blocks
  // 这里按你保存格式假设： [data][offsets(uint16_t)][num(uint16_t)][maybe hash(uint32_t)]
  // 因为实际文件可能是多个 block concatenated，这里只做一次解析示例（从头开始）
  size_t pos = 0;
  for (int b = 0; b < 100 && pos + 4 < buf.size(); ++b) {
    // try to find num at several candidate positions near end of reasonable block
    // brute-force: try num_pos = pos + some min to max range
    size_t found = SIZE_MAX;
    for (size_t num_pos = pos + 1; num_pos + 1 < buf.size() && num_pos < pos + 4096; ++num_pos) {
      uint16_t num;
      std::memcpy(&num, buf.data() + num_pos, sizeof(uint16_t));
      size_t offsets_section_start = (num_pos >= static_cast<size_t>(num) * 2)
                                         ? (num_pos - static_cast<size_t>(num) * 2)
                                         : SIZE_MAX;
      if (offsets_section_start == SIZE_MAX)
        continue;
      // quick sanity: offsets_section_start should be >= pos (data section non-negative)
      if (offsets_section_start >= pos && offsets_section_start + num * 2 == num_pos) {
        // check first offset within data
        if (num > 0) {
          uint16_t first_off;
          std::memcpy(&first_off, buf.data() + offsets_section_start, sizeof(uint16_t));
          if (first_off < (num_pos - pos)) {
            found = num_pos;
            break;
          }
        } else {
          found = num_pos;
          break;
        }
      }
    }
    if (found == SIZE_MAX)
      break;
    uint16_t num;
    std::memcpy(&num, buf.data() + found, sizeof(uint16_t));
    size_t offsets_section_start = found - static_cast<size_t>(num) * 2;
    std::cout << "BLOCK " << b << " pos=" << pos << " data_len=" << (offsets_section_start - pos)
              << " num_offsets=" << num << " offsets_start=" << offsets_section_start << "\n";
    for (size_t i = 0; i < num && i < 10; i++) {
      uint16_t off;
      std::memcpy(&off, buf.data() + offsets_section_start + i * 2, 2);
      std::cout << "  off[" << i << "]=" << off << "\n";
    }
    // advance pos to after this block (assume no hash)
    pos = found + 2;
  }
  return 0;
}