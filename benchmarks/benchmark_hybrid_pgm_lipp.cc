#include "benchmarks/benchmark_hybrid_pgm_lipp.h"

#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Helper function to determine optimal parameters for each dataset
template <typename Searcher, int record>
std::vector<int> get_dataset_params(const std::string& dataset_name, bool is_insertion_heavy) {
  // Default parameters: {flush_threshold_pct, batch_size, flushing_mode}
  std::vector<int> params;
  
  if (dataset_name.find("fb_100M") != std::string::npos) {
    // Facebook dataset parameters
    if (is_insertion_heavy) {
      // 90% inserts, 10% lookups
      params = {15, 2000, 1}; // 15% threshold, larger batches, adaptive mode
    } else {
      // 10% inserts, 90% lookups
      params = {3, 500, 1};  // 3% threshold, smaller batches, adaptive mode
    }
  } else if (dataset_name.find("books_100M") != std::string::npos) {
    // Books dataset parameters
    if (is_insertion_heavy) {
      params = {10, 2000, 1};
    } else {
      params = {2, 800, 1};
    }
  } else if (dataset_name.find("osmc_100M") != std::string::npos) {
    // OSMC dataset parameters
    if (is_insertion_heavy) {
      params = {12, 3000, 1};
    } else {
      params = {5, 1000, 1};
    }
  } else {
    // Default parameters for unknown datasets
    if (is_insertion_heavy) {
      params = {10, 2000, 1};
    } else {
      params = {3, 1000, 1};
    }
  }
  
  return params;
}

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
    
    // Get optimal parameters
    std::vector<int> optimal_params = get_dataset_params<BranchingBinarySearch<record>, record>(
        dataset_name, is_insertion_heavy);
    
    // Create parameter variations
    std::vector<int> low_threshold_params = optimal_params;
    low_threshold_params[0] = std::max(1, optimal_params[0] / 2);
    
    std::vector<int> high_threshold_params = optimal_params;
    high_threshold_params[0] = optimal_params[0] * 2;
    
    std::vector<int> small_batch_params = optimal_params;
    small_batch_params[1] = std::max(100, optimal_params[1] / 2);
    
    std::vector<int> large_batch_params = optimal_params;
    large_batch_params[1] = optimal_params[1] * 2;
    
    std::vector<int> fixed_mode_params = optimal_params;
    fixed_mode_params[2] = 0;
    
    // For Facebook dataset
    if (dataset_name.find("fb_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for Facebook dataset
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(low_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(high_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(small_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(large_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(fixed_mode_params);
        
        // Try alternative parameters
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(optimal_params);
      } else {
        // Lookup-heavy (10% inserts) for Facebook dataset
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(low_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(high_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(small_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(fixed_mode_params);
        
        // Try alternative parameters
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 32>>(optimal_params);
      }
    }
    // For Books dataset
    else if (dataset_name.find("books_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for Books dataset
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(low_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(high_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(small_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(fixed_mode_params);
        
        // Try alternative parameters
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 128>>(optimal_params);
      } else {
        // Lookup-heavy (10% inserts) for Books dataset
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(low_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(high_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(small_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(fixed_mode_params);
        
        // Try alternative parameters
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, 64>>(optimal_params);
      }
    }
    // For OSMC dataset
    else if (dataset_name.find("osmc_100M") != std::string::npos) {
      if (is_insertion_heavy) {
        // Insertion-heavy (90% inserts) for OSMC dataset
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(low_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(high_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(small_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(fixed_mode_params);
        
        // Try alternative parameters
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64>>(optimal_params);
      } else {
        // Lookup-heavy (10% inserts) for OSMC dataset
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(low_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(high_threshold_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(small_batch_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(fixed_mode_params);
        
        // Try alternative parameters
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
      }
    }
    // For any other dataset
    else {
      if (is_insertion_heavy) {
        // Insertion-heavy default configuration
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(optimal_params);
      } else {
        // Lookup-heavy default configuration
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(optimal_params);
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
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