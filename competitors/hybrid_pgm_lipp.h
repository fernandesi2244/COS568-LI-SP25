#pragma once

#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex> // For reader-writer lock
#include <unordered_set>
#include <thread>
#include <future>
#include <condition_variable>
#include <queue>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

// Adaptive flushing strategies
enum FlushingMode {
  FIXED_THRESHOLD = 0,  // Use a constant threshold
  WORKLOAD_ADAPTIVE = 1 // Adapt threshold based on workload pattern
};

template <class KeyType, class SearchClass, size_t pgm_error = 64>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params = std::vector<int>())
      : lipp_(params), pgm_(params), pgm_size_(0), is_flushing_(false)
  {
    // Parse parameters with validation
    flush_threshold_ = (params.size() > 0 && params[0] > 0) ? params[0] : 5;
    batch_size_ = (params.size() > 1 && params[1] > 0) ? params[1] : 1000;
    flushing_mode_ = (params.size() > 2 && params[2] >= 0 && params[2] <= 1) ? 
        static_cast<FlushingMode>(params[2]) : WORKLOAD_ADAPTIVE;
    
    // Stats for diagnostics
    flush_count_ = 0;
    lookups_since_last_flush_ = 0;
    inserts_since_last_flush_ = 0;
    
    // Initialize the flush worker thread
    flush_worker_active_ = true;
    flush_worker_ = std::thread(&HybridPGMLIPP::FlushWorkerThread, this);
  }
  
  ~HybridPGMLIPP() {
    // Signal worker thread to stop and wait for it
    {
      std::lock_guard<std::mutex> lock(flush_mutex_);
      flush_worker_active_ = false;
      flush_cv_.notify_one();
    }
    
    if (flush_worker_.joinable()) {
      flush_worker_.join();
    }
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Save initial data size to calculate threshold
    initial_data_size_ = data.size();
    flush_threshold_count_ = initial_data_size_ * flush_threshold_ / 100;
    
    // Ensure minimum threshold to avoid too frequent flushes
    flush_threshold_count_ = std::max<size_t>(flush_threshold_count_, 100);
    
    // Build LIPP index with initial data - no lock needed during build
    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) {
    // Track lookup operation for adaptive flushing
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      lookups_since_last_flush_++;
    }
    
    // First try to find in PGM (where newer data is)
    size_t pgm_res;
    {
      std::shared_lock<std::shared_mutex> lock(pgm_mutex_);
      pgm_res = pgm_.EqualityLookup(lookup_key, thread_id);
    }
    
    if (pgm_res != util::NOT_FOUND) {
      return pgm_res;
    }

    // If not found in PGM, check LIPP with a shared lock
    std::shared_lock<std::shared_mutex> lipp_lock(lipp_mutex_);
    return lipp_.EqualityLookup(lookup_key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    // Get results from PGM with read lock
    uint64_t pgm_res;
    {
      std::shared_lock<std::shared_mutex> lock(pgm_mutex_);
      pgm_res = pgm_.RangeQuery(lower_key, upper_key, thread_id);
    }
    
    // Get results from LIPP with read lock
    uint64_t lipp_res;
    {
      std::shared_lock<std::shared_mutex> lock(lipp_mutex_);
      lipp_res = lipp_.RangeQuery(lower_key, upper_key, thread_id);
    }

    return pgm_res + lipp_res;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // Track insert operation for adaptive flushing
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      inserts_since_last_flush_++;
    }
    
    // Insert into PGM data structures with exclusive lock
    {
      std::unique_lock<std::shared_mutex> lock(pgm_mutex_);
      pgm_data_.emplace_back(data);
      pgm_.Insert(data, thread_id);
      
      // Update size with atomic to allow safe reading from other threads
      pgm_size_.fetch_add(1, std::memory_order_relaxed);
    }

    // Check if we need to flush from PGM to LIPP
    CheckAndTriggerFlush();
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { 
    size_t pgm_size = 0;
    size_t lipp_size = 0;
    
    {
      std::shared_lock<std::shared_mutex> lock(pgm_mutex_);
      pgm_size = pgm_.size();
    }
    
    {
      std::shared_lock<std::shared_mutex> lock(lipp_mutex_);
      lipp_size = lipp_.size();
    }
    
    return pgm_size + lipp_size;
  }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
    // Both LIPP and PGM have unique key requirements
    std::string name = SearchClass::name();
    return name != "LinearAVX" && unique && !multithread;
  }

  std::vector<std::string> variants() const { 
    std::vector<std::string> vec;
    vec.push_back(SearchClass::name());
    vec.push_back(std::to_string(pgm_error));
    vec.push_back(std::to_string(flush_threshold_));
    vec.push_back(std::to_string(batch_size_));
    vec.push_back(std::to_string(flushing_mode_));
    vec.push_back("flushes:" + std::to_string(flush_count_));
    return vec;
  }

 private:
  Lipp<KeyType> lipp_;
  DynamicPGM<KeyType, SearchClass, pgm_error> pgm_;

  // Thread-safe access to data structures
  std::vector<KeyValue<KeyType>> pgm_data_;
  mutable std::shared_mutex pgm_mutex_; // Shared mutex for PGM
  mutable std::shared_mutex lipp_mutex_; // Shared mutex for LIPP
  
  // Parameters
  size_t initial_data_size_;
  size_t flush_threshold_; // Percentage of initial data size
  size_t flush_threshold_count_; // Absolute count for flushing
  size_t batch_size_; // Number of items to flush in each batch
  FlushingMode flushing_mode_; // Flushing strategy
  
  // Atomic current state
  std::atomic<size_t> pgm_size_; // Current size of PGM data
  std::atomic<bool> is_flushing_; // Flag for tracking flush state
  
  // Statistics with thread-safe access
  mutable size_t lookups_since_last_flush_;
  mutable size_t inserts_since_last_flush_;
  mutable std::mutex stats_mutex_;
  std::atomic<size_t> flush_count_; // Number of flushes performed
  
  // Flush worker thread management
  std::thread flush_worker_;
  std::mutex flush_mutex_;
  std::condition_variable flush_cv_;
  bool flush_worker_active_;
  std::queue<size_t> flush_queue_; // Queue for flush tasks (thread_id)

  // Trigger flush operation if needed
  void CheckAndTriggerFlush() {
    // Skip if already flushing
    if (is_flushing_.load(std::memory_order_relaxed)) {
      return;
    }
    
    // Check if we need to flush based on current state
    bool should_flush = false;
    
    if (flushing_mode_ == FIXED_THRESHOLD) {
      // Fixed threshold mode
      should_flush = (pgm_size_.load(std::memory_order_relaxed) >= flush_threshold_count_);
    } else {
      // Adaptive mode based on workload pattern
      size_t lookups = 0;
      size_t inserts = 0;
      
      {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        lookups = lookups_since_last_flush_;
        inserts = inserts_since_last_flush_;
      }
      
      size_t total_ops = lookups + inserts;
      if (total_ops > 1000) { // Only adapt after sufficient operations
        double lookup_ratio = static_cast<double>(lookups) / total_ops;
        
        // For lookup-heavy workloads, flush more frequently (lower threshold)
        // For insert-heavy workloads, flush less frequently (higher threshold)
        size_t adaptive_threshold = flush_threshold_count_;
        if (lookup_ratio > 0.8) {
          // Lookup-heavy: flush earlier to improve lookup performance
          adaptive_threshold = flush_threshold_count_ / 2;
        } else if (lookup_ratio < 0.2) {
          // Insert-heavy: delay flushing to batch more insertions
          adaptive_threshold = flush_threshold_count_ * 2;
        }
        
        should_flush = (pgm_size_.load(std::memory_order_relaxed) >= adaptive_threshold);
      } else {
        // Not enough operations to adapt yet, use default threshold
        should_flush = (pgm_size_.load(std::memory_order_relaxed) >= flush_threshold_count_);
      }
    }
    
    // If we should flush and no flush is currently in progress
    if (should_flush && !is_flushing_.exchange(true, std::memory_order_acquire)) {
      // Signal the flush worker thread to start flushing
      uint32_t thread_id = 0; // Default thread ID
      
      {
        std::lock_guard<std::mutex> lock(flush_mutex_);
        flush_queue_.push(thread_id);
        flush_cv_.notify_one();
      }
    }
  }
  
  // Worker thread for asynchronous flushing
  void FlushWorkerThread() {
    while (true) {
      uint32_t thread_id = 0;
      bool has_task = false;
      
      // Wait for a task or shutdown signal
      {
        std::unique_lock<std::mutex> lock(flush_mutex_);
        flush_cv_.wait(lock, [this] { 
          return !flush_queue_.empty() || !flush_worker_active_; 
        });
        
        // Check if we should exit
        if (!flush_worker_active_ && flush_queue_.empty()) {
          break;
        }
        
        // Get task from queue
        if (!flush_queue_.empty()) {
          thread_id = flush_queue_.front();
          flush_queue_.pop();
          has_task = true;
        }
      }
      
      // Process flush task if we have one
      if (has_task) {
        PerformFlush(thread_id);
      }
    }
  }

  // Perform an incremental flush from PGM to LIPP
  void PerformFlush(uint32_t thread_id) {
    // Reset operation counters
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      lookups_since_last_flush_ = 0;
      inserts_since_last_flush_ = 0;
    }
    
    // Create a copy of the current PGM data (snapshot)
    std::vector<KeyValue<KeyType>> data_to_flush;
    {
      std::shared_lock<std::shared_mutex> lock(pgm_mutex_);
      data_to_flush = pgm_data_;
    }
    
    // Track flushed keys
    std::unordered_set<KeyType> flushed_keys;
    
    // Process in batches
    const size_t total_items = data_to_flush.size();
    for (size_t start = 0; start < total_items; start += batch_size_) {
      // Calculate current batch size
      size_t end = std::min(start + batch_size_, total_items);
      
      // Process this batch with exclusive lock on LIPP
      {
        std::unique_lock<std::shared_mutex> lipp_lock(lipp_mutex_);
        for (size_t i = start; i < end; i++) {
          const auto& item = data_to_flush[i];
          lipp_.Insert(item, thread_id);
          flushed_keys.insert(item.key);
        }
      }
      
      // Periodically yield to allow other operations
      if (start % (batch_size_ * 5) == 0 && start > 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    }
    
    // Remove flushed items from PGM data structures
    {
      std::unique_lock<std::shared_mutex> lock(pgm_mutex_);
      
      // Only remove keys that were successfully flushed
      auto new_end = std::remove_if(pgm_data_.begin(), pgm_data_.end(),
                                  [&flushed_keys](const KeyValue<KeyType>& item) {
                                    return flushed_keys.find(item.key) != flushed_keys.end();
                                  });
      
      size_t removed_count = std::distance(new_end, pgm_data_.end());
      pgm_data_.erase(new_end, pgm_data_.end());
      
      // Update size
      pgm_size_.store(pgm_data_.size(), std::memory_order_relaxed);
      
      // Rebuild PGM index efficiently
      pgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>());
      
      // Use bulk insert if possible, otherwise individual inserts
      if (!pgm_data_.empty()) {
        for (const auto& item : pgm_data_) {
          pgm_.Insert(item, thread_id);
        }
      }
    }
    
    // Increment flush count
    flush_count_.fetch_add(1, std::memory_order_relaxed);
    
    // Mark flushing as complete
    is_flushing_.store(false, std::memory_order_release);
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H