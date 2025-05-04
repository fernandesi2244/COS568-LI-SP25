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
#include <thread>
#include <queue>
#include <chrono>
#include <unordered_set>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error = 64>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params = std::vector<int>())
      : lipp_(params), pgm_(params), pgm_size_(0), is_flushing_(false)
  {
    // Parse parameters
    if (params.size() > 0) {
      flush_threshold_ = params[0]; // Percentage of initial data size
    } else {
      flush_threshold_ = 5; // Default 5%
    }
    
    if (params.size() > 1) {
      batch_size_ = params[1]; // Batch size for flushing
    } else {
      batch_size_ = 1000; // Default batch size
    }
    
    if (params.size() > 2) {
      adaptive_mode_ = params[2]; // 0=fixed, 1=workload-adaptive
    } else {
      adaptive_mode_ = 1; // Default adaptive mode
    }
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Save initial data size to calculate threshold
    initial_data_size_ = data.size();
    flush_threshold_count_ = initial_data_size_ * flush_threshold_ / 100;
    
    // Track operation stats for adaptive flushing
    lookups_since_last_flush_ = 0;
    inserts_since_last_flush_ = 0;
    
    // Build LIPP index with initial data
    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) {
    // Track lookup operation for adaptive flushing
    lookups_since_last_flush_++;
    
    // First try to find in PGM (where newer data is)
    size_t pgm_res = pgm_.EqualityLookup(lookup_key, thread_id);
    if (pgm_res != util::NOT_FOUND) {
      return pgm_res;
    }

    // If not found in PGM, check LIPP
    return lipp_.EqualityLookup(lookup_key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    // Aggregate across both PGM and LIPP
    uint64_t pgm_res = pgm_.RangeQuery(lower_key, upper_key, thread_id);
    uint64_t lipp_res = lipp_.RangeQuery(lower_key, upper_key, thread_id);

    return pgm_res + lipp_res;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // Track insert operation for adaptive flushing
    inserts_since_last_flush_++;
    
    // Use mutex to protect pgm_data_ during insertion
    {
      std::lock_guard<std::mutex> lock(pgm_mutex_);
      pgm_data_.emplace_back(data);
      pgm_.Insert(data, thread_id);
      pgm_size_++;
    }

    // Check if we need to flush from PGM to LIPP
    // But don't block this insertion operation
    CheckAndTriggerFlush(thread_id);
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { 
    return pgm_.size() + lipp_.size(); 
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
    vec.push_back(std::to_string(adaptive_mode_));
    return vec;
  }

 private:
  Lipp<KeyType> lipp_;
  DynamicPGM<KeyType, SearchClass, pgm_error> pgm_;

  std::vector<KeyValue<KeyType>> pgm_data_;
  std::mutex pgm_mutex_;  // Protect pgm_data_ during concurrent operations
  
  // Atomic flag to indicate if flushing is in progress
  mutable std::atomic<bool> is_flushing_;
  
  // Parameters
  size_t initial_data_size_;
  size_t flush_threshold_; // Percentage of initial data size
  size_t flush_threshold_count_; // Absolute count for flushing
  size_t batch_size_; // Number of items to flush in each batch
  size_t adaptive_mode_; // 0=fixed threshold, 1=workload-adaptive
  
  // Current state
  size_t pgm_size_ = 0; // Current size of PGM data
  
  // Workload tracking for adaptive flushing
  mutable std::atomic<size_t> lookups_since_last_flush_;
  mutable std::atomic<size_t> inserts_since_last_flush_;

  // Trigger flush operation if needed
  void CheckAndTriggerFlush(uint32_t thread_id) {
    // Skip if already flushing
    if (is_flushing_.load(std::memory_order_relaxed)) {
      return;
    }

    bool should_flush = false;

    if (adaptive_mode_ == 0) {
      should_flush = (pgm_size_ >= flush_threshold_count_);
    } else {
      size_t total_ops = lookups_since_last_flush_ + inserts_since_last_flush_;
      if (total_ops > 1000) {
        double lookup_ratio = static_cast<double>(lookups_since_last_flush_) / total_ops;
        size_t adaptive_threshold = flush_threshold_count_;
        if (lookup_ratio > 0.8) {
          adaptive_threshold = flush_threshold_count_ / 2;
        } else if (lookup_ratio < 0.2) {
          adaptive_threshold = flush_threshold_count_ * 2;
        }
        should_flush = (pgm_size_ >= adaptive_threshold);
      } else {
        should_flush = (pgm_size_ >= flush_threshold_count_);
      }
    }

    if (should_flush && !is_flushing_.exchange(true, std::memory_order_acquire)) {
        // Launch a background flush thread
        std::thread(&HybridPGMLIPP::FlushWorker, this, thread_id).detach();
    }
  }

  // Flush data from PGM to LIPP incrementally
  void IncrementalFlush(uint32_t thread_id) {
    // Reset operation counters
    lookups_since_last_flush_ = 0;
    inserts_since_last_flush_ = 0;
    
    // Create a copy of the current PGM data to flush
    std::vector<KeyValue<KeyType>> data_to_flush;
    {
      std::lock_guard<std::mutex> lock(pgm_mutex_);
      // Create a copy of current data, but DON'T clear pgm_data_ yet
      data_to_flush = pgm_data_;
      
      // We're taking a snapshot, but keeping original data intact
      // We will only remove these specific items later
    }
    
    // Sort the data for batch insertion
    std::sort(data_to_flush.begin(), data_to_flush.end(), 
              [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                return a.key < b.key;
              });
    
    // Process in batches to avoid long blocking operations
    const size_t total_items = data_to_flush.size();
    size_t items_processed = 0;
    
    // Keep track of successfully flushed keys
    std::vector<KeyType> successfully_flushed_keys;
    successfully_flushed_keys.reserve(total_items);
    
    while (items_processed < total_items) {
      // Determine the size of the current batch
      size_t batch_end = std::min(items_processed + batch_size_, total_items);
      size_t current_batch_size = batch_end - items_processed;
      
      // Insert this batch into LIPP
      for (size_t i = 0; i < current_batch_size; i++) {
        const auto& item = data_to_flush[items_processed + i];
        lipp_.Insert(item, thread_id);
        
        // Keep track of successfully flushed keys
        successfully_flushed_keys.push_back(item.key);
      }
      
      items_processed += current_batch_size;
      
      // Periodically check if we should pause flushing
      if (items_processed % (batch_size_ * 5) == 0) {
        // Short sleep to yield CPU
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    }
    
    // Only remove the items we've successfully flushed
    {
      std::lock_guard<std::mutex> lock(pgm_mutex_);
      
      // Remove only the flushed items from pgm_data_
      // This preserves any new items added during the flush
      if (!successfully_flushed_keys.empty()) {
        // Create a set for efficient lookup
        std::unordered_set<KeyType> flushed_keys_set(
            successfully_flushed_keys.begin(), successfully_flushed_keys.end());
        
        // Remove flushed items from pgm_data_
        auto new_end = std::remove_if(pgm_data_.begin(), pgm_data_.end(),
            [&flushed_keys_set](const KeyValue<KeyType>& item) {
                return flushed_keys_set.count(item.key) > 0;
            });
        pgm_data_.erase(new_end, pgm_data_.end());
        
        // Update size
        pgm_size_ = pgm_data_.size();
        
        // Rebuild PGM with remaining items
        pgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>());
        for (const auto& item : pgm_data_) {
          pgm_.Insert(item, thread_id);
        }
      }
    }
    
    // Mark flushing as complete
    is_flushing_.store(false, std::memory_order_release);
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H