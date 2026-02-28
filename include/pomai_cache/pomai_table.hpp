#pragma once

#include "pomai_cache/compact_string.hpp"
#include "pomai_cache/types.hpp"
#include <vector>
#include <memory>
#include <optional>

namespace pomai_cache {

/**
 * PomaiTable: A cache-friendly hash table inspired by DashTable.
 * - Uses 64-byte Metadata Buckets (7 slots each).
 * - 1-byte fingerprints for rapid filtering.
 * - Optimized for linear probing or simple chaining.
 */
class PomaiTable {
public:
    struct Slot {
        CompactString key;
        Entry value;
    };

    struct alignas(64) MetaBucket {
        uint8_t fingerprints[7];
        uint8_t count;
        Slot* slots[7];

        MetaBucket() : count(0) {
            std::memset(fingerprints, 0, sizeof(fingerprints));
            std::memset(slots, 0, sizeof(slots));
        }
    };

    static_assert(sizeof(MetaBucket) == 64, "MetaBucket must be exactly 64 bytes (one cache line)");

    PomaiTable(size_t capacity = 1024) : size_(0) {
        num_buckets_ = (capacity + 6) / 7;
        buckets_.resize(num_buckets_);
    }

    ~PomaiTable() {
        for (auto& bucket : buckets_) {
            for (int i = 0; i < bucket.count; ++i) {
                delete bucket.slots[i];
            }
        }
    }

    void set(const std::string& key, const Entry& entry) {
        uint64_t h = std::hash<std::string>{}(key);
        uint8_t fp = static_cast<uint8_t>(h & 0xFF);
        if (fp == 0) fp = 1; // 0 is reserved for empty

        size_t idx = (h / 256) % num_buckets_;

        // Search for existing
        for (size_t i = 0; i < buckets_[idx].count; ++i) {
            if (buckets_[idx].fingerprints[i] == fp && buckets_[idx].slots[i]->key == key) {
                buckets_[idx].slots[i]->value = entry;
                return;
            }
        }

        // Insert new
        if (buckets_[idx].count < 7) {
            buckets_[idx].slots[buckets_[idx].count] = new Slot{CompactString(key), entry};
            buckets_[idx].fingerprints[buckets_[idx].count] = fp;
            buckets_[idx].count++;
            size_++;
        } else {
            // Chaining/Overflow - simple implementation: grow or rehash
            // For now, let's just use a second bucket or rehash if we were more complex.
            // Simplified: always rehash if a bucket is full for this prototype.
            rehash();
            set(key, entry);
        }
    }

    Entry* get(const std::string& key) {
        uint64_t h = std::hash<std::string>{}(key);
        uint8_t fp = static_cast<uint8_t>(h & 0xFF);
        if (fp == 0) fp = 1;

        size_t idx = (h / 256) % num_buckets_;
        auto& bucket = buckets_[idx];

        for (size_t i = 0; i < bucket.count; ++i) {
            if (bucket.fingerprints[i] == fp && bucket.slots[i]->key == key) {
                return &bucket.slots[i]->value;
            }
        }
        return nullptr;
    }

    bool contains(const std::string& key) {
        return get(key) != nullptr;
    }

    bool erase(const std::string& key) {
        uint64_t h = std::hash<std::string>{}(key);
        uint8_t fp = static_cast<uint8_t>(h & 0xFF);
        if (fp == 0) fp = 1;

        size_t idx = (h / 256) % num_buckets_;
        auto& bucket = buckets_[idx];

        for (size_t i = 0; i < bucket.count; ++i) {
            if (bucket.fingerprints[i] == fp && bucket.slots[i]->key == key) {
                delete bucket.slots[i];
                // Swap with last
                bucket.slots[i] = bucket.slots[bucket.count - 1];
                bucket.fingerprints[i] = bucket.fingerprints[bucket.count - 1];
                bucket.count--;
                size_--;
                return true;
            }
        }
        return false;
    }

    size_t size() const { return size_; }

    Slot* getRandomSlot() const {
        if (size_ == 0) return nullptr;
        size_t attempts = 0;
        while (attempts < 100) {
            size_t idx = rand() % num_buckets_;
            if (buckets_[idx].count > 0) {
                return buckets_[idx].slots[rand() % buckets_[idx].count];
            }
            attempts++;
        }
        return nullptr;
    }

    // Iterator-like access for eviction policies
    template<typename F>
    void iterate(F&& f) const {
        for (const auto& bucket : buckets_) {
            for (int i = 0; i < bucket.count; ++i) {
                f(std::string(bucket.slots[i]->key.view()), bucket.slots[i]->value);
            }
        }
    }

private:
    void rehash() {
        auto old_buckets = std::move(buckets_);
        num_buckets_ *= 2;
        buckets_.clear();
        buckets_.resize(num_buckets_);
        size_ = 0;

        for (auto& bucket : old_buckets) {
            for (int i = 0; i < bucket.count; ++i) {
                set(std::string(bucket.slots[i]->key.view()), bucket.slots[i]->value);
                delete bucket.slots[i];
            }
        }
    }

    size_t num_buckets_;
    std::vector<MetaBucket> buckets_;
    size_t size_{0};
};

} // namespace pomai_cache
