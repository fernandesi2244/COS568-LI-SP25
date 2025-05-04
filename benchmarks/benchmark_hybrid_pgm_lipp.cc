#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                 bool pareto, const std::vector<int>& params) {
  if (!pareto){
    util::fail("Hybrid PGM-LIPP's hyperparameter cannot be set");
  }
  else{
    // Test different pgm_error values with default params
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 16>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 32>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 64>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 128>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 256>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 512>>();
    benchmark.template Run<HybridPGMLIPP<uint64_t, Searcher, 1024>>();
  }
}

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, const std::string& filename) {
  // Optimized parameters for each dataset and workload
  if (filename.find("mix") != std::string::npos) {
    bool is_insertion_heavy = filename.find("0.900000i") != std::string::npos;
    
    // Get dataset name
    std::string dataset_name;
    if (filename.find("fb_100M") != std::string::npos) {
      dataset_name = "fb_100M";
    } else if (filename.find("books_100M") != std::string::npos) {
      dataset_name = "books_100M";
    } else if (filename.find("osmc_100M") != std::string::npos) {
      dataset_name = "osmc_100M";
    }
    
    // Facebook dataset
    if (dataset_name.find("fb_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for Facebook dataset
        std::vector<int> params_5_adaptive = {5, 1}; // 5% threshold, adaptive mode
        std::vector<int> params_10_adaptive = {10, 1}; // 10% threshold, adaptive mode
        std::vector<int> params_15_adaptive = {15, 1}; // 15% threshold, adaptive mode
        std::vector<int> params_10_fixed = {10, 0}; // 10% threshold, fixed mode
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_15_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_fixed);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(params_10_adaptive);
      } else {
        // Lookup-heavy (10% inserts) for Facebook dataset
        std::vector<int> params_1_adaptive = {1, 1}; // 1% threshold, adaptive mode
        std::vector<int> params_3_adaptive = {3, 1}; // 3% threshold, adaptive mode
        std::vector<int> params_5_adaptive = {5, 1}; // 5% threshold, adaptive mode
        std::vector<int> params_3_fixed = {3, 0}; // 3% threshold, fixed mode
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_1_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_5_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_3_fixed);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 32>>(params_3_adaptive);
      }
    }
    // Books dataset
    else if (dataset_name.find("books_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for Books dataset
        std::vector<int> params_5_adaptive = {5, 1}; // 5% threshold, adaptive mode
        std::vector<int> params_10_adaptive = {10, 1}; // 10% threshold, adaptive mode
        std::vector<int> params_15_adaptive = {15, 1}; // 15% threshold, adaptive mode
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_5_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_15_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 128>>(params_10_adaptive);
      } else {
        // Lookup-heavy (10% inserts) for Books dataset
        std::vector<int> params_1_adaptive = {1, 1}; // 1% threshold, adaptive mode
        std::vector<int> params_3_adaptive = {3, 1}; // 3% threshold, adaptive mode
        std::vector<int> params_5_adaptive = {5, 1}; // 5% threshold, adaptive mode
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_1_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(params_3_adaptive);
      }
    }
    // OSMC dataset
    else if (dataset_name.find("osmc_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for OSMC dataset
        std::vector<int> params_5_adaptive = {5, 1}; // 5% threshold, adaptive mode
        std::vector<int> params_10_adaptive = {10, 1}; // 10% threshold, adaptive mode
        std::vector<int> params_20_adaptive = {20, 1}; // 20% threshold, adaptive mode
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_20_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64>>(params_10_adaptive);
      } else {
        // Lookup-heavy (10% inserts) for OSMC dataset
        std::vector<int> params_1_adaptive = {1, 1}; // 1% threshold, adaptive mode
        std::vector<int> params_3_adaptive = {3, 1}; // 3% threshold, adaptive mode
        std::vector<int> params_5_adaptive = {5, 1}; // 5% threshold, adaptive mode
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_1_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_5_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_3_adaptive);
      }
    }
    // Default for any other dataset
    else {
      if (is_insertion_heavy) {
        // Insertion-heavy default configuration
        std::vector<int> params_10_adaptive = {10, 1}; // 10% threshold, adaptive mode
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_10_adaptive);
      } else {
        // Lookup-heavy default configuration
        std::vector<int> params_3_adaptive = {3, 1}; // 3% threshold, adaptive mode
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_3_adaptive);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_3_adaptive);
      }
    }
  } else {
    // For non-mixed workloads, use default configurations
    if (filename.find("0.000000i") != std::string::npos) {
      // Lookup-only workload
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>();
    } else if (filename.find("0m") != std::string::npos) {
      // Non-mixed workload
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>();
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);