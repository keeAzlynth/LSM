#include "../../include/iterator/BaseIterator.h"

auto operator<=>(const SerachIterator& lhs, const SerachIterator& rhs) -> std::strong_ordering {
  if (lhs.key_ != rhs.key_) {
    return lhs.key_ <=> rhs.key_;
  }
  if (lhs.transaction_id_ != rhs.transaction_id_) {
    return lhs.transaction_id_ <=> rhs.transaction_id_;
  }
  return lhs.level_ <=> rhs.level_;
}
