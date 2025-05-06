#pragma once

#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <memory>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error = 64> //, size_t flush_threshold_percent = 5>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params = std::vector<int>())
      : lipp_(params), pgm_(params), pgm_size_(0), flush_count_(0)
  {
    // Use the template parameter as default, but allow override from params
    flush_threshold_ = params.size() > 0 ? params[0] : 5; // 5 is default flush threshold percentage
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    // Save initial data size to calculate threshold
    initial_data_size_ = data.size();
    flush_threshold_count_ = initial_data_size_ * flush_threshold_ / 100 / 10;

    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
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
    // Insert into PGM
    pgm_data_.emplace_back(data);
    pgm_.Insert(data, thread_id);

    pgm_size_++;

    // Check if we need to flush from PGM to LIPP
    FlushIfNeeded(thread_id);
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const { return pgm_.size() + lipp_.size(); }

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
    vec.push_back(std::to_string(flush_count_)); // Added flush count to variants
    return vec;
  }

 private:
  Lipp<KeyType> lipp_;
  DynamicPGM<KeyType, SearchClass, pgm_error> pgm_;

  std::vector<KeyValue<KeyType>> pgm_data_;
  
  // Threshold parameters
  size_t initial_data_size_;
  size_t flush_threshold_; // Percentage of initial data size
  size_t flush_threshold_count_; // Absolute count for flushing
  size_t pgm_size_ = 0; // Current size of PGM data
  
  // Flush counter
  size_t flush_count_; // Added counter to track number of flushes

  // Flush data from PGM to LIPP if needed
  void FlushIfNeeded(uint32_t thread_id) {
    if (pgm_size_ < flush_threshold_count_) {
      return; // No need to flush yet
    }

    // Increment flush counter
    flush_count_++;

    // Move data from PGM to LIPP
    for (const auto& data : pgm_data_) {
      lipp_.Insert(data, thread_id);
    }
    pgm_data_.clear(); // Clear PGM data after moving to LIPP
    pgm_size_ = 0; // Reset PGM size
    pgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>()); // Reset PGM
    std::cout << "Flushed " << flush_threshold_count_ << " items from PGM to LIPP. Flush count: " << flush_count_ << std::endl;
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H