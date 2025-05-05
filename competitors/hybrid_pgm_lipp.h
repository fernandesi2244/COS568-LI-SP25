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
#include <shared_mutex>
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
      : lipp_(params), 
        pgm_front_(new DynamicPGM<KeyType, SearchClass, pgm_error>(params)),
        pgm_back_(new DynamicPGM<KeyType, SearchClass, pgm_error>(params)),
        pgm_front_size_(0), 
        pgm_back_size_(0),
        is_flushing_(false),
        lookups_since_last_flush_(0), 
        inserts_since_last_flush_(0), 
        flush_count_(0)
  {
    // Parse parameters with validation
    flush_threshold_ = (params.size() > 0 && params[0] > 0) ? params[0] : 5;
    std::cout << "Flush threshold set to: " << flush_threshold_ << "%" << std::endl;
    flushing_mode_ = (params.size() > 1 && params[1] >= 0 && params[1] <= 1) ? 
        static_cast<FlushingMode>(params[1]) : WORKLOAD_ADAPTIVE;
    
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
    
    // Clean up the double buffer
    delete pgm_front_;
    delete pgm_back_;
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Save initial data size
    initial_data_size_ = data.size();
    
    // Interpret threshold parameter as absolute count in thousands
    // Instead of a percentage of initial data size
    flush_threshold_count_ = flush_threshold_ * 1000;
    
    std::cout << "Initial data size: " << initial_data_size_ << std::endl;
    std::cout << "Flush threshold count: " << flush_threshold_count_ << " keys" << std::endl;
    
    // Build LIPP index with initial data - no lock needed during build
    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) {
    // Track lookup operation for adaptive flushing using atomic increment
    lookups_since_last_flush_.fetch_add(1, std::memory_order_relaxed);
    
    // First try to find in front PGM (where newest data is)
    size_t pgm_front_res = util::NOT_FOUND;
    {
      std::shared_lock<std::shared_mutex> lock(pgm_front_mutex_);
      pgm_front_res = pgm_front_->EqualityLookup(lookup_key, thread_id);
    }
    
    if (pgm_front_res != util::NOT_FOUND) {
      return pgm_front_res;
    }
    
    // Then try the back PGM if it's not empty
    size_t pgm_back_res = util::NOT_FOUND;
    if (pgm_back_size_.load(std::memory_order_relaxed) > 0) {
      std::shared_lock<std::shared_mutex> lock(pgm_back_mutex_);
      pgm_back_res = pgm_back_->EqualityLookup(lookup_key, thread_id);
    }
    
    if (pgm_back_res != util::NOT_FOUND) {
      return pgm_back_res;
    }

    // If not found in either PGM, check LIPP
    std::shared_lock<std::shared_mutex> lipp_lock(lipp_mutex_);
    return lipp_.EqualityLookup(lookup_key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) {
    // Get results from front PGM with read lock
    uint64_t pgm_front_res = 0;
    {
      std::shared_lock<std::shared_mutex> lock(pgm_front_mutex_);
      pgm_front_res = pgm_front_->RangeQuery(lower_key, upper_key, thread_id);
    }
    
    // Get results from back PGM with read lock if it's not empty
    uint64_t pgm_back_res = 0;
    if (pgm_back_size_.load(std::memory_order_relaxed) > 0) {
      std::shared_lock<std::shared_mutex> lock(pgm_back_mutex_);
      pgm_back_res = pgm_back_->RangeQuery(lower_key, upper_key, thread_id);
    }
    
    // Get results from LIPP with read lock
    uint64_t lipp_res = 0;
    {
      std::shared_lock<std::shared_mutex> lock(lipp_mutex_);
      lipp_res = lipp_.RangeQuery(lower_key, upper_key, thread_id);
    }

    return pgm_front_res + pgm_back_res + lipp_res;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // Track insert operation for adaptive flushing using atomic increment
    inserts_since_last_flush_.fetch_add(1, std::memory_order_relaxed);
    
    // Insert into front PGM data structures with exclusive lock
    {
      std::unique_lock<std::shared_mutex> lock(pgm_front_mutex_);
      pgm_front_data_.push_back(data);
      pgm_front_->Insert(data, thread_id);
      
      // Update size with atomic to allow safe reading from other threads
      pgm_front_size_.fetch_add(1, std::memory_order_relaxed);
    }

    // Check if we need to swap buffers and trigger flush
    CheckAndTriggerFlush(thread_id);
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { 
    size_t pgm_front_size = 0;
    size_t pgm_back_size = 0;
    size_t lipp_size = 0;
    
    {
      std::shared_lock<std::shared_mutex> lock(pgm_front_mutex_);
      pgm_front_size = pgm_front_->size();
    }
    
    {
      std::shared_lock<std::shared_mutex> lock(pgm_back_mutex_);
      pgm_back_size = pgm_back_->size();
    }
    
    {
      std::shared_lock<std::shared_mutex> lock(lipp_mutex_);
      lipp_size = lipp_.size();
    }
    
    return pgm_front_size + pgm_back_size + lipp_size;
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
    vec.push_back(std::to_string(flushing_mode_));
    vec.push_back("flushes:" + std::to_string(flush_count_.load(std::memory_order_relaxed)));
    return vec;
  }

 private:
  Lipp<KeyType> lipp_;
  
  // Double-buffer for PGM indexes
  DynamicPGM<KeyType, SearchClass, pgm_error>* pgm_front_;
  DynamicPGM<KeyType, SearchClass, pgm_error>* pgm_back_;
  
  // Double-buffer data vectors
  std::vector<KeyValue<KeyType>> pgm_front_data_;
  std::vector<KeyValue<KeyType>> pgm_back_data_;
  
  // Mutexes for each buffer to allow concurrent operations
  mutable std::shared_mutex pgm_front_mutex_;
  mutable std::shared_mutex pgm_back_mutex_;
  mutable std::shared_mutex lipp_mutex_;
  
  // Parameters
  size_t initial_data_size_;
  size_t flush_threshold_; // Percentage of initial data size
  size_t flush_threshold_count_; // Absolute count for flushing
  FlushingMode flushing_mode_; // Flushing strategy
  
  // Atomic current state
  std::atomic<size_t> pgm_front_size_; // Current size of front PGM data
  std::atomic<size_t> pgm_back_size_;  // Current size of back PGM data
  std::atomic<bool> is_flushing_; // Flag for tracking flush state
  
  // Statistics with atomic counters (more efficient than mutex)
  mutable std::atomic<size_t> lookups_since_last_flush_;
  mutable std::atomic<size_t> inserts_since_last_flush_;
  std::atomic<size_t> flush_count_; // Number of flushes performed
  
  // Flush worker thread management
  std::thread flush_worker_;
  std::mutex flush_mutex_;
  std::condition_variable flush_cv_;
  bool flush_worker_active_;
  std::queue<size_t> flush_queue_; // Queue for flush tasks (thread_id)

  // Trigger flush operation if needed by swapping buffers
  void CheckAndTriggerFlush(uint32_t thread_id) {
    // Skip if already flushing
    if (is_flushing_.load(std::memory_order_relaxed)) {
      return;
    }
    
    // Check if we need to flush based on current state
    bool should_flush = false;
    
    if (flushing_mode_ == FIXED_THRESHOLD) {
      // Fixed threshold mode
      std::cout << "PGM front size: " << pgm_front_size_.load(std::memory_order_relaxed) << std::endl;
      std::cout << "Flush threshold count: " << flush_threshold_count_ << std::endl;
      should_flush = (pgm_front_size_.load(std::memory_order_relaxed) >= flush_threshold_count_);
    } else {
      // Adaptive mode based on workload pattern
      size_t lookups = lookups_since_last_flush_.load(std::memory_order_relaxed);
      size_t inserts = inserts_since_last_flush_.load(std::memory_order_relaxed);
      
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
        
        should_flush = (pgm_front_size_.load(std::memory_order_relaxed) >= adaptive_threshold);

        std::cout << "Adaptive flush threshold: " << adaptive_threshold << std::endl;
        std::cout << "Current flush size: " << pgm_front_size_.load(std::memory_order_relaxed) << std::endl;
        std::cout << "Lookups since last flush: " << lookups_since_last_flush_.load(std::memory_order_relaxed) << std::endl;
        std::cout << "Inserts since last flush: " << inserts_since_last_flush_.load(std::memory_order_relaxed) << std::endl;
        std::cout << "Total operations since last flush: " << total_ops << std::endl;
      } else {
        // Not enough operations to adapt yet, use default threshold
        should_flush = (pgm_front_size_.load(std::memory_order_relaxed) >= flush_threshold_count_);
      }
    }
    
    // If we should flush and no flush is currently in progress
    if (should_flush && !is_flushing_.exchange(true, std::memory_order_acquire)) {
      // Make sure back buffer is empty
      if (pgm_back_size_.load(std::memory_order_relaxed) == 0) {
        // Swap front and back buffers
        SwapBuffers();
        
        // Signal the flush worker thread to start flushing
        {
          std::lock_guard<std::mutex> lock(flush_mutex_);
          flush_queue_.push(thread_id);
          flush_cv_.notify_one();
        }
      } else {
        // Back buffer is not empty, so wait
        is_flushing_.store(false, std::memory_order_release);
      }
    }
  }
  
  // Swap front and back buffers
  void SwapBuffers() {
    // Take exclusive locks on both buffers
    std::unique_lock<std::shared_mutex> front_lock(pgm_front_mutex_);
    std::unique_lock<std::shared_mutex> back_lock(pgm_back_mutex_);
    
    // Swap buffer pointers
    std::swap(pgm_front_, pgm_back_);
    
    // Swap data vectors
    pgm_back_data_.swap(pgm_front_data_);
    
    // Update sizes
    pgm_back_size_.store(pgm_front_size_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    pgm_front_size_.store(0, std::memory_order_relaxed);
    
    // Clear front data (now the empty buffer)
    pgm_front_data_.clear();
    
    // Reset counters
    lookups_since_last_flush_.store(0, std::memory_order_relaxed);
    inserts_since_last_flush_.store(0, std::memory_order_relaxed);
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

  // Perform an asynchronous flush from back PGM to LIPP
  void PerformFlush(uint32_t thread_id) {
    std::vector<KeyValue<KeyType>> data_to_flush;
    
    // Get a copy of the back buffer data with read lock
    {
      std::shared_lock<std::shared_mutex> lock(pgm_back_mutex_);
      if (pgm_back_data_.empty()) {
        is_flushing_.store(false, std::memory_order_release);
        return;
      }
      data_to_flush = pgm_back_data_;
    }
    
    // Sort by key for better insertion performance
    std::sort(data_to_flush.begin(), data_to_flush.end(),
              [](const KeyValue<KeyType>& a, const KeyValue<KeyType>& b) {
                  return a.key < b.key;
              });
    
    // Use bulk insertion with exclusive lock on LIPP
    {
        std::unique_lock<std::shared_mutex> lipp_lock(lipp_mutex_);
        lipp_.BulkInsert(data_to_flush, thread_id);
    }
    
    // Clear the back buffer with exclusive lock
    {
      std::unique_lock<std::shared_mutex> lock(pgm_back_mutex_);
      pgm_back_data_.clear();
      
      // Reset the back PGM index
      delete pgm_back_;
      pgm_back_ = new DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>());
      
      // Reset size
      pgm_back_size_.store(0, std::memory_order_relaxed);
    }
    
    // Increment flush count
    flush_count_.fetch_add(1, std::memory_order_relaxed);
    
    // Mark flushing as complete
    is_flushing_.store(false, std::memory_order_release);
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H