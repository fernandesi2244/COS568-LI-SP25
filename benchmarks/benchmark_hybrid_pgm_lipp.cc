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
        // Insertion-heavy (90% inserts) for Facebook dataset - fixed thresholds only
        // Use absolute counts rather than percentages: 50k, 100k, 200k, 400k keys
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        std::vector<int> params_200k = {200, 0}; // 200k threshold
        std::vector<int> params_400k = {400, 0}; // 400k threshold
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_200k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_400k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(params_100k);
      } else {
        // Lookup-heavy (10% inserts) for Facebook dataset - fixed thresholds only
        // Lower thresholds since there are fewer inserts
        std::vector<int> params_10k = {10, 0};   // 10k threshold
        std::vector<int> params_25k = {25, 0};   // 25k threshold
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_10k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 32>>(params_25k);
      }
    }
    // Books dataset
    else if (dataset_name.find("books_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for Books dataset - fixed thresholds only
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        std::vector<int> params_200k = {200, 0}; // 200k threshold
        std::vector<int> params_400k = {400, 0}; // 400k threshold
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_200k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_400k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 128>>(params_100k);
      } else {
        // Lookup-heavy (10% inserts) for Books dataset - fixed thresholds only
        std::vector<int> params_10k = {10, 0};   // 10k threshold
        std::vector<int> params_25k = {25, 0};   // 25k threshold
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(params_25k);
      }
    }
    // OSMC dataset
    else if (dataset_name.find("osmc_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for OSMC dataset - fixed thresholds only
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        std::vector<int> params_200k = {200, 0}; // 200k threshold
        std::vector<int> params_400k = {400, 0}; // 400k threshold
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_200k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_400k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64>>(params_100k);
      } else {
        // Lookup-heavy (10% inserts) for OSMC dataset - fixed thresholds only
        std::vector<int> params_10k = {10, 0};   // 10k threshold
        std::vector<int> params_25k = {25, 0};   // 25k threshold
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_10k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_25k);
      }
    }
    // Default for any other dataset
    else {
      std::cout << "Unknown dataset: " << dataset_name << std::endl;
      if (is_insertion_heavy) {
        // Insertion-heavy default configuration - fixed thresholds only
        std::vector<int> params_100k = {100, 0}; // 100k threshold
        std::vector<int> params_200k = {200, 0}; // 200k threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_100k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_200k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_100k);
      } else {
        // Lookup-heavy default configuration - fixed thresholds only
        std::vector<int> params_25k = {25, 0};   // 25k threshold
        std::vector<int> params_50k = {50, 0};   // 50k threshold
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_25k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_50k);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_25k);
      }
    }
  } else {
    // For non-mixed workloads, use default configurations with fixed thresholds
    if (filename.find("0.000000i") != std::string::npos) {
      // Lookup-only workload
      std::vector<int> params_50k = {50, 0};  // 50k threshold
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_50k);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_50k);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_50k);
    } else if (filename.find("0m") != std::string::npos) {
      // Non-mixed workload
      std::vector<int> params_50k = {50, 0};  // 50k threshold
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_50k);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_50k);
      benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(params_50k);
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);