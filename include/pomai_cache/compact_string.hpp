#pragma once

#include <string>
#include <string_view>
#include <cstring>
#include <algorithm>

namespace pomai_cache {

/**
 * Inspired by DragonflyDB's `CompactObj` (src/core/compact_object.h).
 * Inlines small strings (up to 23 bytes) directly in the object to avoid heap allocations.
 * Larger strings are stored on the heap.
 */
class CompactString {
public:
    static constexpr size_t kInlineLimit = 23;

    CompactString() : size_(0) {
        data_.inline_data[0] = '\0';
    }

    CompactString(std::string_view sv) {
        size_ = sv.size();
        if (is_inline()) {
            std::memcpy(data_.inline_data, sv.data(), size_);
            data_.inline_data[size_] = '\0';
        } else {
            data_.heap_ptr = new char[size_ + 1];
            std::memcpy(data_.heap_ptr, sv.data(), size_);
            data_.heap_ptr[size_] = '\0';
        }
    }

    ~CompactString() {
        if (!is_inline()) {
            delete[] data_.heap_ptr;
        }
    }

    CompactString(const CompactString& other) : size_(other.size_) {
        if (is_inline()) {
            std::memcpy(data_.inline_data, other.data_.inline_data, size_ + 1);
        } else {
            data_.heap_ptr = new char[size_ + 1];
            std::memcpy(data_.heap_ptr, other.data_.heap_ptr, size_ + 1);
        }
    }

    CompactString(CompactString&& other) noexcept : size_(other.size_) {
        std::memcpy(&data_, &other.data_, sizeof(Data));
        other.size_ = 0;
        other.data_.inline_data[0] = '\0';
    }

    CompactString& operator=(const CompactString& other) {
        if (this == &other) return *this;
        if (!is_inline()) delete[] data_.heap_ptr;
        size_ = other.size_;
        if (is_inline()) {
            std::memcpy(data_.inline_data, other.data_.inline_data, size_ + 1);
        } else {
            data_.heap_ptr = new char[size_ + 1];
            std::memcpy(data_.heap_ptr, other.data_.heap_ptr, size_ + 1);
        }
        return *this;
    }

    CompactString& operator=(CompactString&& other) noexcept {
        if (this == &other) return *this;
        if (!is_inline()) delete[] data_.heap_ptr;
        size_ = other.size_;
        std::memcpy(&data_, &other.data_, sizeof(Data));
        other.size_ = 0;
        other.data_.inline_data[0] = '\0';
        return *this;
    }

    std::string_view view() const {
        return is_inline() ? std::string_view(data_.inline_data, size_) 
                           : std::string_view(data_.heap_ptr, size_);
    }

    bool operator==(const CompactString& other) const {
        return view() == other.view();
    }

    bool operator==(std::string_view sv) const {
        return view() == sv;
    }

    size_t size() const { return size_; }

private:
    bool is_inline() const { return size_ <= kInlineLimit; }

    size_t size_;
    union Data {
        char inline_data[kInlineLimit + 1];
        char* heap_ptr;
    } data_;
};

} // namespace pomai_cache

namespace std {
    template <>
    struct hash<pomai_cache::CompactString> {
        size_t operator()(const pomai_cache::CompactString& s) const {
            return std::hash<std::string_view>{}(s.view());
        }
    };
}
