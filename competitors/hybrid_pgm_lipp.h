#pragma once

#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

template <class KeyType, class SearchClass, size_t pgm_error = 64>
class HybridPGMLIPP : public Competitor<KeyType, SearchClass> {
 public:
  HybridPGMLIPP(const std::vector<int>& params = {})
      : lipp_(params),
        pgm_(params),
        pgm_size_(0),
        flush_threshold_percent_(params.empty() ? 5 : params[0]),
        stop_flush_(false),
        flush_in_progress_(false),
        flush_count_(0) {
    flush_thread_ = std::thread(&HybridPGMLIPP::AsyncFlushWorker, this);
  }

  ~HybridPGMLIPP() {
    stop_flush_ = true;
    cv_flush_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) override {
    initial_data_size_ = data.size();
    flush_threshold_count_ = initial_data_size_ * flush_threshold_percent_ / 100 / 10;
    return lipp_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& key, uint32_t thread_id) const override {
    size_t res = pgm_.EqualityLookup(key, thread_id);
    return (res != util::NOT_FOUND) ? res : lipp_.EqualityLookup(key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lower, const KeyType& upper, uint32_t thread_id) const override {
    return pgm_.RangeQuery(lower, upper, thread_id) + lipp_.RangeQuery(lower, upper, thread_id);
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) override {
    {
      std::lock_guard<std::mutex> lock(pgm_mutex_);
      pgm_data_.emplace_back(data);
      pgm_.Insert(data, thread_id);
      pgm_size_++;
    }

    if (pgm_size_ >= flush_threshold_count_ && !flush_in_progress_) {
      flush_in_progress_ = true;
      cv_flush_.notify_one();
    }
  }

  std::string name() const override { return "HybridPGMLIPP"; }

  std::size_t size() const override { return pgm_.size() + lipp_.size(); }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string&) const override {
    return unique && !multithread && SearchClass::name() != "LinearAVX";
  }

  std::vector<std::string> variants() const override {
    return {SearchClass::name(), std::to_string(pgm_error), std::to_string(flush_threshold_percent_), std::to_string(flush_count_.load())};
  }

 private:
  Lipp<KeyType> lipp_;
  DynamicPGM<KeyType, SearchClass, pgm_error> pgm_;

  std::vector<KeyValue<KeyType>> pgm_data_;
  mutable std::mutex pgm_mutex_;

  size_t initial_data_size_;
  size_t flush_threshold_percent_;
  size_t flush_threshold_count_;
  std::atomic<size_t> pgm_size_;

  std::thread flush_thread_;
  mutable std::mutex flush_mutex_;
  std::condition_variable cv_flush_;
  std::atomic<bool> stop_flush_;
  std::atomic<bool> flush_in_progress_;

  std::atomic<size_t> flush_count_;

  void AsyncFlushWorker() {
    while (!stop_flush_) {
      std::unique_lock<std::mutex> lock(flush_mutex_);
      cv_flush_.wait(lock, [this]() { return flush_in_progress_ || stop_flush_; });

      if (stop_flush_) break;

      std::vector<KeyValue<KeyType>> local_data;
      {
        std::lock_guard<std::mutex> pgm_lock(pgm_mutex_);
        local_data.swap(pgm_data_);
        pgm_size_ = 0;
        pgm_ = DynamicPGM<KeyType, SearchClass, pgm_error>();
      }

      if (!local_data.empty()) {
        lipp_.BulkInsert(local_data, 0);
        flush_count_++;
      }

      flush_in_progress_ = false;
    }
  }
};

#endif
