#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

class BlockIterator;
class Block : public std::enable_shared_from_this<Block> {
 public:
  friend class BlockIterator;
  Block();
  explicit Block(std::size_t capacity);
  std::vector<uint8_t> encode(bool with_hash = true);

  static std::shared_ptr<Block> decode(const std::vector<uint8_t>& encoded, bool with_hash = true);
  std::string                   get_first_key();
  std::optional<size_t>         get_idx_binary(const std::string& key, const uint64_t tranc_id = 0);
  std::optional<size_t>         get_prefix_begin_idx_binary(const std::string& key,
                                                            const uint64_t     tranc_id = 0);
  std::optional<size_t>         get_prefix_end_idx_binary(const std::string& key,
                                                          const uint64_t     tranc_id = 0);
  std::optional<std::size_t>    get_offset(const std::size_t index);
  std::size_t                   get_cur_size() const;

  std::optional<uint64_t>             get_tranc_id(const std::size_t offset) const;
  std::optional<std::string>          get_value_binary(const std::string& key);
  bool                                KeyExists(const std::string& key);
  std::pair<std::string, std::string> get_first_and_last_key();
  bool          add_entry(const std::string& key, const std::string& value, const uint64_t tranc_id,
                          bool force_write = false);
  bool          is_empty() const;
  BlockIterator get_iterator(const std::string& key, const uint64_t tranc_id = 0);
  BlockIterator begin();
  BlockIterator end();
  // BlockIterator                       current_iterator();
  std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
  get_prefix_iterator(std::string key, const uint64_t tranc_id);

 private:
  std::vector<uint8_t>  Data_;
  std::vector<uint16_t> Offset_;
  std::size_t           capcity;
  struct Entry {
    std::string    key;
    std::string    value;
    const uint64_t tranc_id;
  };
  std::string                   get_key(const std::size_t offset) const;
  std::string                   get_value(const std::size_t offset) const;
  std::shared_ptr<Block::Entry> get_entry(std::size_t offset);
};