#include "pomai_cache/ssd_store.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string_view>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pomai_cache {
namespace {
#pragma pack(push, 1)
struct RecordHeader {
  std::uint32_t magic;
  std::uint32_t checksum;
  std::uint64_t key_hash;
  std::uint64_t seq;
  std::uint64_t offset_next;
  std::int64_t ttl_epoch_ms;
  std::uint32_t key_len;
  std::uint32_t value_len;
  std::uint8_t tombstone;
  std::uint8_t reserved[7];
};
#pragma pack(pop)

constexpr std::uint32_t kMagic = 0x504d3443; // PMC4

std::uint32_t checksum32(const std::string &key, const std::vector<std::uint8_t> &value,
                         const RecordHeader &h) {
  std::uint32_t sum = 2166136261u;
  auto mix = [&](std::uint8_t b) {
    sum ^= b;
    sum *= 16777619u;
  };
  auto *p = reinterpret_cast<const std::uint8_t *>(&h);
  for (std::size_t i = 0; i < sizeof(RecordHeader); ++i) {
    if (i >= offsetof(RecordHeader, checksum) &&
        i < offsetof(RecordHeader, checksum) + sizeof(h.checksum))
      continue;
    mix(p[i]);
  }
  for (unsigned char c : key)
    mix(c);
  for (auto b : value)
    mix(b);
  return sum;
}

std::int64_t to_epoch_ms(std::optional<TimePoint> t) {
  if (!t.has_value())
    return -1;
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t->time_since_epoch()).count();
  return static_cast<std::int64_t>(ms);
}

bool fsync_dir(const std::string &dir) {
  int dfd = open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
    return false;
  bool ok = ::fsync(dfd) == 0;
  close(dfd);
  return ok;
}

} // namespace

SsdStore::SsdStore(SsdConfig cfg) : cfg_(std::move(cfg)) {
  token_refill_ = std::chrono::steady_clock::now();
  read_tokens_ = static_cast<double>(cfg_.max_read_mb_s) * 1024.0 * 1024.0;
  write_tokens_ = static_cast<double>(cfg_.max_write_mb_s) * 1024.0 * 1024.0;
}

bool SsdStore::init(std::string *err) {
  if (!cfg_.enabled)
    return true;
  std::filesystem::create_directories(cfg_.dir);
  std::vector<std::uint32_t> segs;
  std::uint32_t active = 1;
  if (!load_manifest(&segs, &active)) {
    segs = {1};
    active = 1;
  }
  segments_.clear();
  total_segment_bytes_ = 0;
  auto start = std::chrono::steady_clock::now();
  for (auto s : segs) {
    if (!scan_segment(s, true)) {
      if (err)
        *err = "segment scan failed";
      return false;
    }
    SegmentMeta sm;
    sm.id = s;
    sm.bytes = std::filesystem::exists(seg_path(s)) ? std::filesystem::file_size(seg_path(s)) : 0;
    segments_.push_back(sm);
    total_segment_bytes_ += sm.bytes;
  }
  if (segments_.empty())
    segments_.push_back({1, 0});
  active_segment_ = active;
  bool found = false;
  for (auto &s : segments_)
    if (s.id == active_segment_)
      found = true;
  if (!found) {
    active_segment_ = segments_.back().id;
  }
  auto p = seg_path(active_segment_);
  active_fd_ = open(p.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
  if (active_fd_ < 0) {
    if (err)
      *err = "failed to open active segment";
    return false;
  }
  stats_.bytes = live_bytes_;
  stats_.fragmentation_estimate = total_segment_bytes_ == 0 ? 0.0 :
      1.0 - static_cast<double>(live_bytes_) / static_cast<double>(total_segment_bytes_);
  stats_.index_rebuild_ms = static_cast<std::size_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
          .count());
  return write_manifest();
}

bool SsdStore::put(const std::string &key, const std::vector<std::uint8_t> &value,
                   std::optional<TimePoint> ttl_deadline, std::uint64_t seq,
                   std::string *err) {
  if (!cfg_.enabled)
    return false;
  IndexEntry ie;
  if (!append_record(key, value, to_epoch_ms(ttl_deadline), seq, false, &ie, err))
    return false;
  auto it = index_.find(key);
  if (it != index_.end() && !it->second.tombstone)
    live_bytes_ -= it->second.len;
  index_[key] = ie;
  live_bytes_ += ie.len;
  stats_.bytes = live_bytes_;
  return true;
}

bool SsdStore::del(const std::string &key, std::uint64_t seq, std::string *err) {
  if (!cfg_.enabled)
    return false;
  IndexEntry ie;
  std::vector<std::uint8_t> empty;
  if (!append_record(key, empty, -1, seq, true, &ie, err))
    return false;
  auto it = index_.find(key);
  if (it != index_.end() && !it->second.tombstone)
    live_bytes_ -= it->second.len;
  ie.tombstone = true;
  ie.len = 0;
  index_[key] = ie;
  stats_.bytes = live_bytes_;
  return true;
}

std::optional<std::vector<std::uint8_t>> SsdStore::get(const std::string &key, SsdMeta *meta) {
  ++stats_.gets;
  auto it = index_.find(key);
  if (it == index_.end() || it->second.tombstone) {
    ++stats_.misses;
    return std::nullopt;
  }
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
  if (it->second.ttl_epoch_ms >= 0 && it->second.ttl_epoch_ms <= now_ms) {
    index_.erase(it);
    ++stats_.misses;
    return std::nullopt;
  }
  std::vector<std::uint8_t> out;
  if (!read_entry(it->second, &out)) {
    ++stats_.misses;
    return std::nullopt;
  }
  if (meta) {
    meta->seq = it->second.seq;
    meta->ttl_epoch_ms = it->second.ttl_epoch_ms;
    meta->len = it->second.len;
  }
  ++stats_.hits;
  return out;
}

bool SsdStore::contains(const std::string &key) const {
  auto it = index_.find(key);
  return it != index_.end() && !it->second.tombstone;
}

std::size_t SsdStore::erase_expired(std::size_t max_items, TimePoint now) {
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  std::size_t removed = 0;
  for (auto it = index_.begin(); it != index_.end() && removed < max_items;) {
    if (!it->second.tombstone && it->second.ttl_epoch_ms >= 0 && it->second.ttl_epoch_ms <= now_ms) {
      live_bytes_ -= it->second.len;
      it = index_.erase(it);
      ++removed;
    } else {
      ++it;
    }
  }
  stats_.bytes = live_bytes_;
  return removed;
}

void SsdStore::maybe_compact() {
  if (!cfg_.enabled)
    return;
  if (segments_.size() < 2)
    return;
  stats_.fragmentation_estimate = total_segment_bytes_ == 0 ? 0.0 :
      1.0 - static_cast<double>(live_bytes_) / static_cast<double>(total_segment_bytes_);
  if (stats_.fragmentation_estimate < cfg_.gc_fragmentation_threshold)
    return;

  auto start = std::chrono::steady_clock::now();
  const std::uint32_t compact_id = segments_.back().id + 1;
  const std::string compact_path = seg_path(compact_id);
  int fd = open(compact_path.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_APPEND, 0644);
  if (fd < 0)
    return;

  std::size_t copied = 0;
  std::size_t reclaimed_before = total_segment_bytes_;
  std::unordered_map<std::string, IndexEntry> new_index;
  for (const auto &[k, e] : index_) {
    if (copied >= cfg_.compaction_batch)
      break;
    if (e.tombstone)
      continue;
    std::vector<std::uint8_t> val;
    if (!read_entry(e, &val))
      continue;
    RecordHeader h{};
    h.magic = kMagic;
    h.key_hash = fnv1a(k);
    h.seq = e.seq;
    h.ttl_epoch_ms = e.ttl_epoch_ms;
    h.key_len = static_cast<std::uint32_t>(k.size());
    h.value_len = static_cast<std::uint32_t>(val.size());
    h.tombstone = 0;
    h.offset_next = 0;
    h.checksum = checksum32(k, val, h);
    off_t off = lseek(fd, 0, SEEK_END);
    if (off < 0)
      continue;
    write(fd, &h, sizeof(h));
    write(fd, k.data(), k.size());
    if (!val.empty())
      write(fd, val.data(), val.size());
    IndexEntry ne;
    ne.segment_id = compact_id;
    ne.offset = static_cast<std::uint64_t>(off);
    ne.len = h.value_len;
    ne.seq = h.seq;
    ne.ttl_epoch_ms = h.ttl_epoch_ms;
    ne.tombstone = false;
    new_index[k] = ne;
    ++copied;
  }
  fsync(fd);
  close(fd);
  if (copied == 0) {
    std::filesystem::remove(compact_path);
    return;
  }

  for (const auto &[k, e] : new_index)
    index_[k] = e;

  std::vector<SegmentMeta> keep;
  for (auto &s : segments_)
    if (s.id == active_segment_)
      keep.push_back(s);
  keep.push_back({compact_id, std::filesystem::file_size(compact_path)});
  segments_ = keep;
  total_segment_bytes_ = 0;
  for (const auto &s : segments_)
    total_segment_bytes_ += s.bytes;
  write_manifest();

  stats_.gc_runs++;
  std::size_t reclaimed_after = total_segment_bytes_;
  if (reclaimed_before > reclaimed_after)
    stats_.gc_bytes_reclaimed += reclaimed_before - reclaimed_after;
  stats_.gc_time_ms += static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
}

std::string SsdStore::seg_path(std::uint32_t id) const {
  return cfg_.dir + "/segment_" + std::to_string(id) + ".log";
}

bool SsdStore::append_record(const std::string &key, const std::vector<std::uint8_t> &value,
                             std::int64_t ttl_epoch_ms, std::uint64_t seq,
                             bool tombstone, IndexEntry *entry, std::string *err) {
  refill_tokens();
  const std::size_t need = sizeof(RecordHeader) + key.size() + value.size();
  if (!consume_write_budget(need)) {
    if (err)
      *err = "ssd write rate limited";
    return false;
  }
  if (stats_.bytes + value.size() > cfg_.max_bytes) {
    if (err)
      *err = "ssd tier full";
    return false;
  }
  RecordHeader h{};
  h.magic = kMagic;
  h.key_hash = fnv1a(key);
  h.seq = seq;
  h.ttl_epoch_ms = ttl_epoch_ms;
  h.key_len = static_cast<std::uint32_t>(key.size());
  h.value_len = static_cast<std::uint32_t>(value.size());
  h.tombstone = tombstone ? 1 : 0;
  h.offset_next = 0;
  h.checksum = checksum32(key, value, h);

  off_t off = lseek(active_fd_, 0, SEEK_END);
  if (off < 0)
    return false;
  if (write(active_fd_, &h, sizeof(h)) != static_cast<ssize_t>(sizeof(h)))
    return false;
  if (write(active_fd_, key.data(), key.size()) != static_cast<ssize_t>(key.size()))
    return false;
  if (!value.empty() && write(active_fd_, value.data(), value.size()) != static_cast<ssize_t>(value.size()))
    return false;
  if (!sync_for_policy())
    return false;

  stats_.write_mb += static_cast<double>(need) / (1024.0 * 1024.0);
  if (entry) {
    entry->segment_id = active_segment_;
    entry->offset = static_cast<std::uint64_t>(off);
    entry->len = static_cast<std::uint32_t>(value.size());
    entry->seq = seq;
    entry->ttl_epoch_ms = ttl_epoch_ms;
    entry->tombstone = tombstone;
  }
  for (auto &s : segments_) {
    if (s.id == active_segment_) {
      s.bytes += need;
      total_segment_bytes_ += need;
      break;
    }
  }
  return true;
}

bool SsdStore::sync_for_policy() {
  if (cfg_.fsync == FsyncMode::Never)
    return true;
  if (cfg_.fsync == FsyncMode::Always)
    return ::fsync(active_fd_) == 0;
  const auto now_s = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  if (now_s != last_fsync_epoch_s_) {
    last_fsync_epoch_s_ = now_s;
    return ::fsync(active_fd_) == 0;
  }
  return true;
}

bool SsdStore::load_manifest(std::vector<std::uint32_t> *segments, std::uint32_t *active) {
  std::ifstream in(cfg_.dir + "/manifest.txt");
  if (!in.is_open())
    return false;
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind("active=", 0) == 0)
      *active = static_cast<std::uint32_t>(std::stoul(line.substr(7)));
    if (line.rfind("segment=", 0) == 0)
      segments->push_back(static_cast<std::uint32_t>(std::stoul(line.substr(8))));
  }
  if (segments->empty())
    segments->push_back(*active);
  return true;
}

bool SsdStore::write_manifest() {
  const std::string tmp = cfg_.dir + "/manifest.tmp";
  const std::string final = cfg_.dir + "/manifest.txt";
  {
    std::ofstream out(tmp, std::ios::trunc);
    if (!out.is_open())
      return false;
    out << "active=" << active_segment_ << "\n";
    for (const auto &s : segments_)
      out << "segment=" << s.id << "\n";
    out.flush();
  }
  int fd = open(tmp.c_str(), O_RDONLY);
  if (fd < 0)
    return false;
  bool ok = fsync(fd) == 0;
  close(fd);
  if (!ok)
    return false;
  if (rename(tmp.c_str(), final.c_str()) != 0)
    return false;
  return fsync_dir(cfg_.dir);
}

bool SsdStore::scan_segment(std::uint32_t id, bool repair_tail) {
  const std::string path = seg_path(id);
  int fd = open(path.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0)
    return false;
  off_t off = 0;
  while (true) {
    RecordHeader h{};
    ssize_t r = pread(fd, &h, sizeof(h), off);
    if (r == 0)
      break;
    if (r != static_cast<ssize_t>(sizeof(h)) || h.magic != kMagic) {
      if (repair_tail)
        ftruncate(fd, off);
      break;
    }
    std::string key(h.key_len, '\0');
    std::vector<std::uint8_t> value(h.value_len);
    if (pread(fd, key.data(), h.key_len, off + static_cast<off_t>(sizeof(h))) !=
        static_cast<ssize_t>(h.key_len)) {
      if (repair_tail)
        ftruncate(fd, off);
      break;
    }
    if (h.value_len > 0 &&
        pread(fd, value.data(), h.value_len,
              off + static_cast<off_t>(sizeof(h) + h.key_len)) !=
            static_cast<ssize_t>(h.value_len)) {
      if (repair_tail)
        ftruncate(fd, off);
      break;
    }
    const auto sum = checksum32(key, value, h);
    if (sum != h.checksum) {
      if (repair_tail)
        ftruncate(fd, off);
      break;
    }
    IndexEntry e;
    e.segment_id = id;
    e.offset = static_cast<std::uint64_t>(off);
    e.len = h.value_len;
    e.seq = h.seq;
    e.ttl_epoch_ms = h.ttl_epoch_ms;
    e.tombstone = h.tombstone != 0;
    auto it = index_.find(key);
    if (it == index_.end() || it->second.seq <= e.seq)
      index_[key] = e;
    off += static_cast<off_t>(sizeof(h) + h.key_len + h.value_len);
  }
  close(fd);

  live_bytes_ = 0;
  for (const auto &[_, e] : index_)
    if (!e.tombstone)
      live_bytes_ += e.len;
  return true;
}

bool SsdStore::read_entry(const IndexEntry &e, std::vector<std::uint8_t> *value_out) {
  refill_tokens();
  if (!consume_read_budget(e.len + sizeof(RecordHeader)))
    return false;
  const std::string path = seg_path(e.segment_id);
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return false;
  RecordHeader h{};
  if (pread(fd, &h, sizeof(h), static_cast<off_t>(e.offset)) !=
      static_cast<ssize_t>(sizeof(h))) {
    close(fd);
    return false;
  }
  std::string key(h.key_len, '\0');
  if (pread(fd, key.data(), h.key_len,
            static_cast<off_t>(e.offset + sizeof(h))) !=
      static_cast<ssize_t>(h.key_len)) {
    close(fd);
    return false;
  }
  value_out->assign(h.value_len, 0);
  if (h.value_len > 0 &&
      pread(fd, value_out->data(), h.value_len,
            static_cast<off_t>(e.offset + sizeof(h) + h.key_len)) !=
          static_cast<ssize_t>(h.value_len)) {
    close(fd);
    return false;
  }
  close(fd);
  stats_.read_mb += static_cast<double>(h.value_len + sizeof(h)) / (1024.0 * 1024.0);
  return true;
}

void SsdStore::refill_tokens() {
  const auto now = std::chrono::steady_clock::now();
  const auto dt = std::chrono::duration<double>(now - token_refill_).count();
  if (dt <= 0)
    return;
  const double rb = static_cast<double>(cfg_.max_read_mb_s) * 1024.0 * 1024.0;
  const double wb = static_cast<double>(cfg_.max_write_mb_s) * 1024.0 * 1024.0;
  read_tokens_ = std::min(rb, read_tokens_ + rb * dt);
  write_tokens_ = std::min(wb, write_tokens_ + wb * dt);
  token_refill_ = now;
}

bool SsdStore::consume_write_budget(std::size_t bytes) {
  if (cfg_.max_write_mb_s == 0)
    return false;
  if (write_tokens_ < static_cast<double>(bytes))
    return false;
  write_tokens_ -= static_cast<double>(bytes);
  return true;
}

bool SsdStore::consume_read_budget(std::size_t bytes) {
  if (cfg_.max_read_mb_s == 0)
    return false;
  if (read_tokens_ < static_cast<double>(bytes))
    return false;
  read_tokens_ -= static_cast<double>(bytes);
  return true;
}

std::uint64_t SsdStore::fnv1a(const std::string &s) {
  std::uint64_t hash = 14695981039346656037ull;
  for (unsigned char c : s) {
    hash ^= c;
    hash *= 1099511628211ull;
  }
  return hash;
}

} // namespace pomai_cache
