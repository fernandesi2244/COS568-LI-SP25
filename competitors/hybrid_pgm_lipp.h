// hybrid_pgm_lipp.h
#pragma once

#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error = 64>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params = std::vector<int>())
      : lipp_(params),
        pgm_active_(params),
        pgm_flushing_(params),        // ← explicitly construct the “flushing” PGM
        active_data_(),               // ← default‐construct the vectors
        flushing_data_(),
        initial_data_size_(0),
        flush_threshold_(params.size() > 0 ? params[0] : 5),
        flush_threshold_count_(0),
        pgm_size_(0),
        flush_count_(0),
        flush_in_progress_(false),
        stop_(false)
  {
    flush_threshold_ = params.size() > 0 ? params[0] : 5;  // percent
    // start background flushing thread
    flush_thread_ = std::thread(&HybridPGMLIPP::FlushThread, this);
  }

  ~HybridPGMLIPP() {
    {
      std::lock_guard<std::mutex> lg(flush_mutex_);
      stop_ = true;
      flush_cv_.notify_all();
    }
    if (flush_thread_.joinable()) {
      flush_thread_.join();
    }
  }

  // Build only LIPP; PGM buffers start empty.
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    initial_data_size_ = data.size();
    flush_threshold_count_ = initial_data_size_ * flush_threshold_ / 100 / 10;

    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    // 1) New data in active PGM
    auto res = pgm_active_.EqualityLookup(lookup_key, thread_id);
    if (res != util::NOT_FOUND) return res;

    // 2) If a flush is in progress, also check the flushing buffer
    if (flush_in_progress_) {
      res = pgm_flushing_.EqualityLookup(lookup_key, thread_id);
      if (res != util::NOT_FOUND) return res;
    }

    // 3) Finally in LIPP
    return lipp_.EqualityLookup(lookup_key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi, uint32_t thread_id) const {
    uint64_t sum = 0;
    sum += pgm_active_.RangeQuery(lo, hi, thread_id);
    if (flush_in_progress_) {
      sum += pgm_flushing_.RangeQuery(lo, hi, thread_id);
    }
    sum += lipp_.RangeQuery(lo, hi, thread_id);
    return sum;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    // 1) push into active buffer
    active_data_.emplace_back(data);
    pgm_active_.Insert(data, thread_id);
    ++pgm_size_;

    // 2) if over threshold and no flush underway, trigger
    FlushIfNeeded();
  }

  std::string name() const { return "HybridPGMLIPP"; }

  std::size_t size() const {
    return pgm_active_.size() + pgm_flushing_.size() + lipp_.size();
  }

  bool applicable(bool unique, bool range_query, bool insert,
                  bool multithread, const std::string& ops_filename) const {
    auto nm = SearchClass::name();
    return nm != "LinearAVX" && unique && !multithread;
  }

  std::vector<std::string> variants() const {
    return {
      SearchClass::name(),
      std::to_string(pgm_error),
      std::to_string(flush_threshold_),
      std::to_string(flush_count_.load())
    };
  }

 private:
  void FlushIfNeeded() {
    if (pgm_size_ < flush_threshold_count_) return;
    std::unique_lock<std::mutex> lock(flush_mutex_);
    if (flush_in_progress_) return;

    // swap active → flushing
    flushing_data_.swap(active_data_);
    pgm_flushing_ = std::move(pgm_active_);
    pgm_active_ = DynamicPGM<KeyType, SearchClass, pgm_error>({});
    pgm_size_ = 0;

    flush_in_progress_ = true;
    flush_count_.fetch_add(1);
    lock.unlock();
    flush_cv_.notify_one();
  }

  void FlushThread() {
    std::unique_lock<std::mutex> lock(flush_mutex_);
    while (true) {
      flush_cv_.wait(lock, [&] { return flush_in_progress_ || stop_; });
      if (stop_) break;

      // take data out locally
      auto batch = std::move(flushing_data_);
      lock.unlock();

      // bulk-insert into LIPP (thread_id = 0)
      lipp_.BulkInsert(batch, 0);
      
      // // Instead of bulk inserting, we can just insert one by one.
      // for (const auto& kv : batch) {
      //   lipp_.Insert(kv, 0);
      // }

      lock.lock();
      pgm_flushing_ = DynamicPGM<KeyType, SearchClass, pgm_error>({});
      flush_in_progress_ = false;
    }
  }

  // Primary indices/buffers
  Lipp<KeyType>                                    lipp_;
  DynamicPGM<KeyType, SearchClass, pgm_error>      pgm_active_;
  DynamicPGM<KeyType, SearchClass, pgm_error>      pgm_flushing_;

  // Data storage
  std::vector<KeyValue<KeyType>>                   active_data_;
  std::vector<KeyValue<KeyType>>                   flushing_data_;

  // Flush control
  size_t                                           initial_data_size_;
  size_t                                           flush_threshold_;       // percent
  size_t                                           flush_threshold_count_; // absolute
  size_t                                           pgm_size_;             // active buffer size
  std::atomic<size_t>                              flush_count_;          // total flushes
  std::atomic<bool>                                flush_in_progress_;
  std::mutex                                       flush_mutex_;
  std::condition_variable                          flush_cv_;
  std::thread                                      flush_thread_;
  bool                                             stop_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
