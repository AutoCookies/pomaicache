#pragma once

#include <string>
#include <thread>
#include <atomic>
#include <vector>
#include <iostream>
#include "pomai_cache/engine_shard.hpp"

/**
 * Inspired by DragonflyDB's `dfly::Replica` (src/server/replica.h).
 * 
 * Performance Analysis:
 * 1. Asynchronous Replication: Commands are streamed from Master to Replica
 *    without blocking the Master's execution path.
 * 2. Shard-Aware Routing: The replica routes incoming commands to the local
 *    thread-owning shards, maintaining the "Share-Nothing" performance.
 * 3. Partial Re-sync: Using the Journal LSNs, the replica can re-sync from
 *    the exact point of failure on a network hiccup.
 */

namespace pomai_cache {

class Replica {
public:
  Replica(std::string master_host, int master_port)
      : host_(std::move(master_host)), port_(master_port), running_(false) {}

  void start() {
    running_ = true;
    thread_ = std::thread([this]() { run_loop(); });
  }

  void stop() {
    running_ = false;
    if (thread_.joinable()) thread_.join();
  }

private:
  void run_loop() {
    // In a production implementation, this would connect to the Master,
    // send PSYNC, and then loop receiving commands.
    // For this prototype, we'll simulate the logic structure.
    
    std::cout << "Replica connecting to master at " << host_ << ":" << port_ << "...\n";
    
    // Simulate receiving commands from Master
    while (running_) {
      // 1. Receive raw RESP from master
      // 2. Parse into command vector
      // 3. Route to target shard
      
      // Pseudo-logic for routing:
      /*
      auto cmd = receive_command();
      if (cmd[0] == "SET") {
        std::string key(cmd[1]);
        auto* shard = ShardSet::instance().get_shard(key);
        if (shard) {
          std::vector<uint8_t> val(cmd[2].begin(), cmd[2].end());
          shard->engine().set(key, val, std::nullopt, "replica");
        }
      }
      */
      
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  std::string host_;
  int port_;
  std::atomic<bool> running_;
  std::thread thread_;
};

} // namespace pomai_cache
