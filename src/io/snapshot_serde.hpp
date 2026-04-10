/*!
 * Copyright (c) 2026 Microsoft Corporation. All rights reserved.
 * Copyright (c) 2026 The LightGBM developers. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for license information.
 */
#ifndef LIGHTGBM_SRC_IO_SNAPSHOT_SERDE_HPP_
#define LIGHTGBM_SRC_IO_SNAPSHOT_SERDE_HPP_

#include <LightGBM/meta.h>
#include <LightGBM/utils/common.h>
#include <LightGBM/utils/log.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>

namespace LightGBM {

class SnapshotWriter {
 public:
  SnapshotWriter() = default;

  template <typename T>
  void WriteScalar(const T& value) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
    const auto* begin = reinterpret_cast<const char*>(&value);
    buffer_.append(begin, sizeof(T));
  }

  void WriteBool(bool value) {
    WriteScalar<uint8_t>(value ? 1 : 0);
  }

  void WriteString(const std::string& value) {
    if (value.size() > static_cast<size_t>((std::numeric_limits<uint64_t>::max)())) {
      Log::Fatal("Snapshot string is too large to serialize");
    }
    WriteScalar<uint64_t>(static_cast<uint64_t>(value.size()));
    buffer_.append(value);
  }

  template <typename T, typename Alloc>
  void WriteVector(const std::vector<T, Alloc>& values) {
    if (values.size() > static_cast<size_t>((std::numeric_limits<uint64_t>::max)())) {
      Log::Fatal("Snapshot vector is too large to serialize");
    }
    WriteScalar<uint64_t>(static_cast<uint64_t>(values.size()));
    if (!values.empty()) {
      static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
      const auto* begin = reinterpret_cast<const char*>(values.data());
      buffer_.append(begin, sizeof(T) * values.size());
    }
  }

  std::string Take() {
    return std::move(buffer_);
  }

 private:
  std::string buffer_;
};

class SnapshotReader {
 public:
  explicit SnapshotReader(const std::string& buffer)
      : ptr_(buffer.data()), end_(buffer.data() + buffer.size()) {}

  SnapshotReader(const char* data, size_t len)
      : ptr_(data), end_(data + len) {}

  template <typename T>
  T ReadScalar() {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
    EnsureRemaining(sizeof(T));
    T value;
    std::memcpy(&value, ptr_, sizeof(T));
    ptr_ += sizeof(T);
    return value;
  }

  bool ReadBool() {
    return ReadScalar<uint8_t>() != 0;
  }

  std::string ReadString() {
    const auto size = ReadScalar<uint64_t>();
    EnsureRemaining(static_cast<size_t>(size));
    std::string value(ptr_, static_cast<size_t>(size));
    ptr_ += static_cast<size_t>(size);
    return value;
  }

  template <typename T, typename Alloc = std::allocator<T>>
  std::vector<T, Alloc> ReadVector() {
    const auto size = ReadScalar<uint64_t>();
    if (size > static_cast<uint64_t>((std::numeric_limits<size_t>::max)())) {
      Log::Fatal("Snapshot vector is too large to deserialize");
    }
    const size_t size_t_value = static_cast<size_t>(size);
    std::vector<T, Alloc> values(size_t_value);
    if (!values.empty()) {
      static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
      const size_t bytes = sizeof(T) * size_t_value;
      EnsureRemaining(bytes);
      std::memcpy(values.data(), ptr_, bytes);
      ptr_ += bytes;
    }
    return values;
  }

  bool IsConsumed() const {
    return ptr_ == end_;
  }

 private:
  void EnsureRemaining(size_t bytes) const {
    if (end_ - ptr_ < static_cast<ptrdiff_t>(bytes)) {
      Log::Fatal("Snapshot is truncated or corrupted");
    }
  }

  const char* ptr_;
  const char* end_;
};

}  // namespace LightGBM

#endif  // LIGHTGBM_SRC_IO_SNAPSHOT_SERDE_HPP_
