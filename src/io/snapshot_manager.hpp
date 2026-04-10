/*!
 * Copyright (c) 2026 Microsoft Corporation. All rights reserved.
 * Copyright (c) 2026 The LightGBM developers. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for license information.
 */
#ifndef LIGHTGBM_SRC_IO_SNAPSHOT_MANAGER_HPP_
#define LIGHTGBM_SRC_IO_SNAPSHOT_MANAGER_HPP_

#include <LightGBM/utils/file_io.h>
#include <LightGBM/utils/log.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace LightGBM {

class SnapshotManager {
 public:
  static bool Exists(const std::string& filename) {
    return VirtualFileWriter::Exists(filename);
  }

  static std::string Read(const std::string& filename) {
    std::ifstream stream(filename, std::ios::binary);
    if (!stream.is_open()) {
      Log::Fatal("Snapshot file %s is not available for reads", filename.c_str());
    }
    stream.seekg(0, std::ios::end);
    const std::streamoff size = stream.tellg();
    if (size < 0) {
      Log::Fatal("Cannot determine size of snapshot file %s", filename.c_str());
    }
    stream.seekg(0, std::ios::beg);
    std::string buffer(static_cast<size_t>(size), '\0');
    if (!buffer.empty()) {
      stream.read(&buffer[0], size);
      if (stream.fail()) {
        Log::Fatal("Cannot read snapshot file %s", filename.c_str());
      }
    }
    return buffer;
  }

  static bool WriteAtomic(const std::string& filename, const std::string& payload) {
    const std::string temp_filename = filename + ".tmp";
    DurableWrite(temp_filename, payload);
    AtomicReplace(temp_filename, filename);
    FlushDirectory(ParentDirectory(filename));
    return true;
  }

 private:
  static std::string ParentDirectory(const std::string& filename) {
    const auto pos = filename.find_last_of("/\\");
    if (pos == std::string::npos) {
      return ".";
    }
    if (pos == 0) {
      return filename.substr(0, 1);
    }
    return filename.substr(0, pos);
  }

  static void DurableWrite(const std::string& filename, const std::string& payload) {
    std::FILE* file = nullptr;
#ifdef _WIN32
    fopen_s(&file, filename.c_str(), "wb");
#else
    file = std::fopen(filename.c_str(), "wb");
#endif
    if (file == nullptr) {
      Log::Fatal("Snapshot file %s is not available for writes", filename.c_str());
    }
    const size_t written = std::fwrite(payload.data(), 1, payload.size(), file);
    if (written != payload.size()) {
      std::fclose(file);
      std::remove(filename.c_str());
      Log::Fatal("Failed to fully write snapshot file %s", filename.c_str());
    }
    if (std::fflush(file) != 0) {
      std::fclose(file);
      std::remove(filename.c_str());
      Log::Fatal("Failed to flush snapshot file %s", filename.c_str());
    }
#ifdef _WIN32
    if (_commit(_fileno(file)) != 0) {
      std::fclose(file);
      std::remove(filename.c_str());
      Log::Fatal("Failed to sync snapshot file %s", filename.c_str());
    }
#else
    if (fsync(fileno(file)) != 0) {
      std::fclose(file);
      std::remove(filename.c_str());
      Log::Fatal("Failed to sync snapshot file %s", filename.c_str());
    }
#endif
    if (std::fclose(file) != 0) {
      std::remove(filename.c_str());
      Log::Fatal("Failed to close snapshot file %s", filename.c_str());
    }
  }

  static void AtomicReplace(const std::string& source, const std::string& destination) {
#ifdef _WIN32
    if (!MoveFileExA(source.c_str(), destination.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
      std::remove(source.c_str());
      Log::Fatal("Failed to atomically replace snapshot file %s", destination.c_str());
    }
#else
    if (std::rename(source.c_str(), destination.c_str()) != 0) {
      std::remove(source.c_str());
      Log::Fatal("Failed to atomically replace snapshot file %s", destination.c_str());
    }
#endif
  }

  static void FlushDirectory(const std::string& directory) {
#ifdef _WIN32
    (void)directory;
#else
    const int fd = open(directory.c_str(), O_RDONLY);
    if (fd < 0) {
      Log::Fatal("Failed to open snapshot directory %s for syncing", directory.c_str());
    }
    if (fsync(fd) != 0) {
      close(fd);
      Log::Fatal("Failed to sync snapshot directory %s", directory.c_str());
    }
    close(fd);
#endif
  }
};

}  // namespace LightGBM

#endif  // LIGHTGBM_SRC_IO_SNAPSHOT_MANAGER_HPP_
