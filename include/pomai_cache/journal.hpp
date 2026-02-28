#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <string_view>

/**
 * Inspired by DragonflyDB's `dfly::journal::Journal` (src/server/journal/journal.h).
 * 
 * Performance Analysis:
 * 1. Sequential Write: Journaling uses append-only writes, which are extremely fast
 *    on both SSDs and RAM.
 * 2. Versioning (LSN): Using Log Sequence Numbers (LSN) allows replicas to resume
 *    synchronization from the exact point they disconnected.
 * 3. Binary Format: We use a lightweight binary format for commands to minimize
 *    network and disk I/O.
 */

namespace pomai_cache {

enum class OpCode : uint8_t {
  SELECT = 0,
  SET = 1,
  DEL = 2,
  EXPIRE = 3,
  FLUSH = 4,
  AI_PUT = 10,
  AI_DEL = 11
};

struct JournalEntry {
  uint64_t lsn;
  OpCode opcode;
  std::vector<std::string> args;
};

class Journal {
public:
  Journal() = default;

  // Append a write operation to the journal
  uint64_t record(OpCode op, const std::vector<std::string>& args) {
    std::unique_lock lock(mu_);
    uint64_t cur_lsn = next_lsn_++;
    
    // In a real production system, this would write to a circular buffer or file.
    // For now, we keep a small in-memory backlog for replication sync.
    backlog_.push_back({cur_lsn, op, {args.begin(), args.end()}});
    if (backlog_.size() > max_backlog_) {
      backlog_.erase(backlog_.begin());
    }
    return cur_lsn;
  }

  // Retrieve entries from a specific LSN (for replication)
  std::vector<JournalEntry> get_since(uint64_t lsn) const {
    std::shared_lock lock(mu_);
    std::vector<JournalEntry> result;
    for (const auto& entry : backlog_) {
      if (entry.lsn >= lsn) {
        result.push_back(entry);
      }
    }
    return result;
  }

  uint64_t cur_lsn() const {
    std::shared_lock lock(mu_);
    return next_lsn_ - 1;
  }

private:
  mutable std::shared_mutex mu_;
  uint64_t next_lsn_{1};
  std::vector<JournalEntry> backlog_;
  const size_t max_backlog_{10000};
};

} // namespace pomai_cache
