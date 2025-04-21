#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <mutex>
#include <atomic>
#include <memory>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error = 64> //, size_t flush_threshold_percent = 5>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params = std::vector<int>()) {
    // Use the template parameter as default, but allow override from params
    flush_threshold_ = params.size() > 0 ? params[0] : 5; // 5 is default flush threshold percentage
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Save initial data size to calculate threshold
    initial_data_size_ = data.size();
    flush_threshold_count_ = initial_data_size_ * flush_threshold_ / 100;
    
    // Start with empty PGM (will be used for new insertions)
    std::vector<std::pair<KeyType, uint64_t>> empty_data;
    
    // Prepare data for LIPP bulk load (initial data goes to LIPP)
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }

    uint64_t build_time = util::timing([&] {
      // Initialize empty PGM
      pgm_ = decltype(pgm_)(empty_data.begin(), empty_data.end());
      
      // Bulk load LIPP with initial data
      lipp_.bulk_load(loading_data.data(), loading_data.size());
    });

    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // First try to find in PGM (where newer data is)
    auto it = pgm_.find(lookup_key);
    if (it != pgm_.end()) {
      return it->value();
    }
    
    // If not found in PGM, look in LIPP
    uint64_t value;
    if (lipp_.find(lookup_key, value)) {
      return value;
    }
    
    return util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    // Get results from PGM
    auto pgm_it = pgm_.lower_bound(lower_key);
    uint64_t result = 0;
    while(pgm_it != pgm_.end() && pgm_it->key() <= upper_key) {
      result += pgm_it->value();
      ++pgm_it;
    }
    
    // Get results from LIPP
    auto lipp_it = lipp_.lower_bound(lower_key);
    while(lipp_it != lipp_.end() && lipp_it->comp.data.key <= upper_key) {
      // Avoid double counting - ensure the key is not in PGM
      if (pgm_.find(lipp_it->comp.data.key) == pgm_.end()) {
        result += lipp_it->comp.data.value;
      }
      ++lipp_it;
    }
    
    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // Insert into PGM
    pgm_.insert(data.key, data.value);
    
    // Check if we need to flush from PGM to LIPP
    pgm_size_ = FlushIfNeeded(thread_id);
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { return pgm_.size_in_bytes() + lipp_.index_size(); }

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
    return vec;
  }

 private:
  mutable LIPP<KeyType, uint64_t> lipp_;
  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_;
  
  // Threshold parameters
  size_t initial_data_size_;
  size_t flush_threshold_; // Percentage of initial data size
  size_t flush_threshold_count_; // Absolute count for flushing
  std::atomic<size_t> pgm_size_{0};
  
  // Mutex for flushing
  mutable std::mutex flush_mutex_;

  // Flush data from PGM to LIPP if needed
  size_t FlushIfNeeded(uint32_t thread_id) {
    // Use atomic to avoid locking when not needed
    size_t current_size = pgm_size_.fetch_add(1) + 1;
    
    // Check if we need to flush
    if (current_size >= flush_threshold_count_) {
      // Try to get lock - only one thread should do the flush
      if (flush_mutex_.try_lock()) {
        try {
          // Recheck in case another thread did the flush
          if (pgm_size_.load() >= flush_threshold_count_) {
            // Extract all data from PGM
            std::vector<std::pair<KeyType, uint64_t>> data_to_flush;
            for (auto it = pgm_.begin(); it != pgm_.end(); ++it) {
              data_to_flush.emplace_back(it->key(), it->value());
            }
            
            // Insert data from PGM into LIPP one by one
            for (const auto& item : data_to_flush) {
              lipp_.insert(item.first, item.second);
            }
            
            // Clear PGM
            std::vector<std::pair<KeyType, uint64_t>> empty_data;
            pgm_ = decltype(pgm_)(empty_data.begin(), empty_data.end());
            
            // Reset counter
            pgm_size_.store(0);
          }
        } catch (...) {
          // Ensure mutex is released even if exception occurs
          flush_mutex_.unlock();
          throw;
        }
        flush_mutex_.unlock();
      }
    }
    
    return current_size;
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H