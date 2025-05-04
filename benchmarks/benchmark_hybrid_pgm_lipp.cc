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
    // Test different pgm_error values
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
  if (filename.find("fb_100M") != std::string::npos) {
    if (filename.find("0.000000i") != std::string::npos) {
      // Lookup-only workload
      benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 16>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>();
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>();
    } else if (filename.find("mix") == std::string::npos) {
      // Non-mixed workload
      if (filename.find("0m") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>();
      }
    } else {
      // Mixed workload
      if (filename.find("0.900000i") != std::string::npos) {
        // Insertion-heavy mixed workload (90% inserts, 10% lookups)
        // Test different flush thresholds
        std::vector<int> params_5_1000_1 = {5, 1000, 1}; // 5% threshold, 1000 batch size, adaptive
        std::vector<int> params_10_1000_1 = {10, 1000, 1}; // 10% threshold, 1000 batch size, adaptive
        std::vector<int> params_15_2000_1 = {15, 2000, 1}; // 15% threshold, 2000 batch size, adaptive
        std::vector<int> params_10_500_0 = {10, 500, 0}; // 10% threshold, 500 batch size, fixed
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_15_2000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_500_0);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_10_1000_1);
      } else if (filename.find("0.100000i") != std::string::npos) {
        // Lookup-heavy mixed workload (10% inserts, 90% lookups)
        std::vector<int> params_1_500_1 = {1, 500, 1}; // 1% threshold, 500 batch size, adaptive
        std::vector<int> params_3_1000_1 = {3, 1000, 1}; // 3% threshold, 1000 batch size, adaptive
        std::vector<int> params_5_2000_1 = {5, 2000, 1}; // 5% threshold, 2000 batch size, adaptive
        std::vector<int> params_1_500_0 = {1, 500, 0}; // 1% threshold, 500 batch size, fixed
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_1_500_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_3_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_2000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_1_500_0);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_1_500_1);
      }
    }
  }
  if (filename.find("books_100M") != std::string::npos) {
    // Similar patterns for books dataset
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.900000i") != std::string::npos) {
        // Insertion-heavy
        std::vector<int> params_5_1000_1 = {5, 1000, 1};
        std::vector<int> params_10_2000_1 = {10, 2000, 1};
        std::vector<int> params_15_3000_1 = {15, 3000, 1};
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>(params_5_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>(params_10_2000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 128>>(params_5_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64>>(params_15_3000_1);
      } else if (filename.find("0.100000i") != std::string::npos) {
        // Lookup-heavy
        std::vector<int> params_1_500_1 = {1, 500, 1};
        std::vector<int> params_3_1000_1 = {3, 1000, 1};
        std::vector<int> params_5_1500_1 = {5, 1500, 1};
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 32>>(params_1_500_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>(params_3_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_5_1500_1);
      }
    }
  }
  if (filename.find("osmc_100M") != std::string::npos) {
    // Similar patterns for osmc dataset
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.900000i") != std::string::npos) {
        // Insertion-heavy
        std::vector<int> params_5_1000_1 = {5, 1000, 1};
        std::vector<int> params_10_2000_1 = {10, 2000, 1};
        std::vector<int> params_20_4000_1 = {20, 4000, 1};
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_10_2000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(params_5_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_20_4000_1);
      } else if (filename.find("0.100000i") != std::string::npos) {
        // Lookup-heavy
        std::vector<int> params_1_500_1 = {1, 500, 1};
        std::vector<int> params_3_1000_1 = {3, 1000, 1};
        std::vector<int> params_5_1500_1 = {5, 1500, 1};
        
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(params_1_500_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(params_3_1000_1);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(params_5_1500_1);
      }
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);