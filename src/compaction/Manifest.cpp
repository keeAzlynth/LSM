#include "../../include/compaction/Manifest.h"
#include "../../include/core/Global.h"
#include <array>
#include <filesystem>
#include <span>


// ─── Constructor ─────────────────────────────────────────────────────────────

Manifest::Manifest(std::string_view dir) {
  namespace fs = std::filesystem;
  if (!fs::exists(dir))
    fs::create_directories(dir);
  path_ = std::string(dir) + "/MANIFEST";

  if (fs::exists(path_)) {
    loaded_     = true;
    auto reader = FileObj::open(path_, /*create=*/false);
    replay(reader);
    reader.close();
  }

  // Open (or create) for appending; positions write cursor at end.
  file_ = FileObj::open(path_, /*create=*/true);
}

// ─── Public API ──────────────────────────────────────────────────────────────

void Manifest::add_sst(const SstMeta& meta) {
  // Encode ADD_SST payload.
  std::vector<uint8_t> payload;
  payload.reserve(40 + meta.first_key.size() + meta.last_key.size());
  Global_::write_le<uint64_t>(payload, static_cast<uint64_t>(meta.sst_id));
  Global_::write_le<uint64_t>(payload, static_cast<uint64_t>(meta.level));
  Global_::write_le<uint64_t>(payload, meta.min_tranc_id);
  Global_::write_le<uint64_t>(payload, meta.max_tranc_id);
  Global_::write_le<uint32_t>(payload, static_cast<uint32_t>(meta.first_key.size()));
  payload.insert(payload.end(), meta.first_key.begin(), meta.first_key.end());
  Global_::write_le<uint32_t>(payload, static_cast<uint32_t>(meta.last_key.size()));
  payload.insert(payload.end(), meta.last_key.begin(), meta.last_key.end());

  std::lock_guard lk(mu_);
  live_[meta.sst_id] = meta;
  write_record(kAddSst, std::move(payload));
}

void Manifest::remove_sst(size_t sst_id) {
  std::vector<uint8_t> payload;
  Global_::write_le<uint64_t>(payload, static_cast<uint64_t>(sst_id));

  std::lock_guard lk(mu_);
  live_.erase(sst_id);
  write_record(kRemoveSst, payload);
}

void Manifest::clear() {
  std::lock_guard lk(mu_);
  live_.clear();
  file_.close();
  // Overwrite with an empty file; subsequent writes start from the beginning.
  file_ = FileObj::create_and_write(path_, {});
}
void Manifest::sync() {
  std::lock_guard lk(mu_);
  file_.sync();
}
std::vector<SstMeta> Manifest::get_live_ssts() const {
  std::lock_guard      lk(mu_);
  std::vector<SstMeta> out;
  out.reserve(live_.size());
  for (const auto& [_, m] : live_) out.push_back(m);
  return out;
}

uint64_t Manifest::checkpoint_tranc_id() const {
  std::lock_guard lk(mu_);
  uint64_t        cp = 0;
  for (const auto& [_, m] : live_) cp = std::max(cp, m.max_tranc_id);
  return cp;
}

// ─── write_record ─────────────────────────────────────────────────────────────
//  Layout: [type(1)] [len(4)] [payload(N)] [Global_::crc32c(4)]
//  CRC covers type ‖ payload so that swapped record-type bugs are caught.

void Manifest::write_record(uint8_t type, const std::vector<uint8_t>& payload) {
  // Build CRC input: type ‖ payload
  std::vector<uint8_t> crc_input;
  crc_input.reserve(1 + payload.size());
  crc_input.push_back(type);
  crc_input.insert(crc_input.end(), payload.begin(), payload.end());
  const uint32_t crc = Global_::crc32c(std::span{crc_input});

  // Assemble complete record
  std::vector<uint8_t> record;
  record.reserve(9 + payload.size());  // 1 + 4 + N + 4
  record.push_back(type);
  Global_::write_le<uint32_t>(record, static_cast<uint32_t>(payload.size()));
  record.insert(record.end(), payload.begin(), payload.end());
  Global_::write_le<uint32_t>(record, crc);

  file_.append(record);
}

// ─── replay ──────────────────────────────────────────────────────────────────
//  Iterates records sequentially.  Stops (not errors) on truncation so that a
//  crash mid-write does not prevent restart.  Stops on CRC mismatch (genuine
//  corruption) to avoid applying partial state.

void Manifest::replay(FileObj& f) {
  const size_t file_size = f.size();
  size_t       offset    = 0;

  // Minimum valid record: type(1) + len(4) + payload(0) + crc(4) = 9 bytes.
  while (offset + 9 <= file_size) {
    const uint8_t  type = f.read_uint8(offset);
    const uint32_t plen = f.read_uint32(offset + 1);

    // Guard against truncation.
    if (offset + 5 + plen + 4 > file_size)
      break;

    auto           payload    = f.read_to_slice(offset + 5, plen);
    const uint32_t stored_crc = f.read_uint32(offset + 5 + plen);

    // Verify CRC; stop on mismatch (do not silently skip corrupt records).
    std::vector<uint8_t> crc_input;
    crc_input.reserve(1 + plen);
    crc_input.push_back(type);
    crc_input.insert(crc_input.end(), payload.begin(), payload.end());
    if (Global_::crc32c(std::span{crc_input}) != stored_crc)
      break;

    offset += 5 + plen + 4;  // advance past this record

    std::span<const uint8_t> sp{payload};

    // ── ADD_SST ──────────────────────────────────────────────────────────
    // Minimum payload: 8+8+8+8+4+4 = 40 bytes (with both keys empty).
    if (type == kAddSst && payload.size() >= 40) {
      SstMeta meta;
      meta.sst_id       = static_cast<size_t>(Global_::read_le<uint64_t>(sp, 0));
      meta.level        = static_cast<size_t>(Global_::read_le<uint64_t>(sp, 8));
      meta.min_tranc_id = Global_::read_le<uint64_t>(sp, 16);
      meta.max_tranc_id = Global_::read_le<uint64_t>(sp, 24);

      const uint32_t fk_len = Global_::read_le<uint32_t>(sp, 32);
      size_t         off2   = 36;
      if (off2 + fk_len > payload.size())
        continue;
      meta.first_key = std::string(reinterpret_cast<const char*>(payload.data() + off2), fk_len);
      off2 += fk_len;

      if (off2 + 4 > payload.size())
        continue;
      const uint32_t lk_len = Global_::read_le<uint32_t>(sp, off2);
      off2 += 4;
      if (off2 + lk_len > payload.size())
        continue;
      meta.last_key = std::string(reinterpret_cast<const char*>(payload.data() + off2), lk_len);

      live_[meta.sst_id] = std::move(meta);

      // ── REMOVE_SST ───────────────────────────────────────────────────────
    } else if (type == kRemoveSst && payload.size() >= 8) {
      const size_t sst_id = static_cast<size_t>(Global_::read_le<uint64_t>(sp, 0));
      live_.erase(sst_id);
    }
    // Unknown types are silently ignored for forward-compatibility.
  }
}