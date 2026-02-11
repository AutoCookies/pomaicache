#pragma once

#include "pomai_cache/types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace pomai_cache {

enum class FsyncMode { Never, EverySec, Always };

struct SsdConfig {
  bool enabled{false};
  std::string dir{"./data"};
  std::size_t value_min_bytes{32 * 1024};
  std::size_t max_bytes{2ULL * 1024 * 1024 * 1024};
  std::size_t max_read_mb_s{256};
  std::size_t max_write_mb_s{256};
  std::size_t compaction_batch{256};
  double gc_fragmentation_threshold{0.25};
  FsyncMode fsync{FsyncMode::EverySec};
};

struct SsdStats {
  std::size_t bytes{0};
  std::uint64_t gets{0};
  std::uint64_t hits{0};
  std::uint64_t misses{0};
  std::uint64_t promotions{0};
  std::uint64_t demotions{0};
  double read_mb{0.0};
  double write_mb{0.0};
  std::uint64_t gc_runs{0};
  std::uint64_t gc_bytes_reclaimed{0};
  std::uint64_t gc_time_ms{0};
  double fragmentation_estimate{0.0};
  std::size_t index_rebuild_ms{0};
};

struct SsdMeta {
  std::uint64_t seq{0};
  std::int64_t ttl_epoch_ms{-1};
  std::size_t len{0};
};

class SsdStore {
public:
  explicit SsdStore(SsdConfig cfg);

  bool init(std::string *err = nullptr);
  bool put(const std::string &key, const std::vector<std::uint8_t> &value,
           std::optional<TimePoint> ttl_deadline, std::uint64_t seq,
           std::string *err = nullptr);
  bool del(const std::string &key, std::uint64_t seq,
           std::string *err = nullptr);

  std::optional<std::vector<std::uint8_t>> get(const std::string &key,
                                               SsdMeta *meta = nullptr);
  bool contains(const std::string &key) const;
  std::size_t erase_expired(std::size_t max_items, TimePoint now);
  void maybe_compact();

  const SsdStats &stats() const { return stats_; }
  std::size_t size() const { return index_.size(); }

private:
  struct IndexEntry {
    std::uint32_t segment_id{0};
    std::uint64_t offset{0};
    std::uint32_t len{0};
    std::uint64_t seq{0};
    std::int64_t ttl_epoch_ms{-1};
    bool tombstone{false};
  };

  struct SegmentMeta {
    std::uint32_t id{0};
    std::size_t bytes{0};
  };

  std::string seg_path(std::uint32_t id) const;
  bool append_record(const std::string &key,
                     const std::vector<std::uint8_t> &value,
                     std::int64_t ttl_epoch_ms, std::uint64_t seq,
                     bool tombstone, IndexEntry *entry, std::string *err);
  bool sync_for_policy();
  bool load_manifest(std::vector<std::uint32_t> *segments,
                     std::uint32_t *active);
  bool write_manifest();
  bool scan_segment(std::uint32_t id, bool repair_tail);
  bool read_entry(const IndexEntry &e, std::vector<std::uint8_t> *value_out);
  bool consume_write_budget(std::size_t bytes);
  bool consume_read_budget(std::size_t bytes);
  void refill_tokens();
  static std::uint64_t fnv1a(const std::string &s);

  SsdConfig cfg_;
  SsdStats stats_;
  std::unordered_map<std::string, IndexEntry> index_;
  std::vector<SegmentMeta> segments_;
  std::uint32_t active_segment_{1};
  int active_fd_{-1};
  std::uint64_t last_fsync_epoch_s_{0};
  std::size_t live_bytes_{0};
  std::size_t total_segment_bytes_{0};

  std::chrono::steady_clock::time_point token_refill_{};
  double read_tokens_{0.0};
  double write_tokens_{0.0};
};

} // namespace pomai_cache
