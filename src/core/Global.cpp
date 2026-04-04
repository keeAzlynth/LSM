#include "../../include/core/Global.h"
int Global_::generateRandom(int begin, int end) {
  static std::mt19937                generator(std::random_device{}());
  std::uniform_int_distribution<int> distribution(begin, end);
  return distribution(generator);
}
