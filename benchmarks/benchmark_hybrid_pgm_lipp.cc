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

// Helper to get the best error bound for PGM based on dataset
template <typename Searcher, int record>
size_t get_best_pgm_error(const std::string& dataset_name, bool is_insertion_heavy) {
  if (dataset_name.find("fb_100M") != std::string::npos) {
    return is_insertion_heavy ? 64 : 32;
  } else if (dataset_name.find("books_100M") != std::string::npos) {
    return is_insertion_heavy ? 128 : 64;
  } else {
    return is_insertion_heavy ? 64 : 32;
  }
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
    
    // Get optimal PGM error bound
    size_t best_error = get_best_pgm_error<BranchingBinarySearch<record>, record>(
        dataset_name, is_insertion_heavy);
    
    // Run with optimal parameters
    if (best_error == 16) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(optimal_params);
    } else if (best_error == 32) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(optimal_params);
    } else if (best_error == 64) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(optimal_params);
    } else if (best_error == 128) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(optimal_params);
    }
    
    // Run with variations to find optimal configuration
    
    // Half the threshold
    std::vector<int> low_threshold_params = optimal_params;
    low_threshold_params[0] = std::max(1, optimal_params[0] / 2);
    
    // Double the threshold
    std::vector<int> high_threshold_params = optimal_params;
    high_threshold_params[0] = optimal_params[0] * 2;
    
    // Half the batch size
    std::vector<int> small_batch_params = optimal_params;
    small_batch_params[1] = std::max(100, optimal_params[1] / 2);
    
    // Double the batch size
    std::vector<int> large_batch_params = optimal_params;
    large_batch_params[1] = optimal_params[1] * 2;
    
    // Fixed (non-adaptive) mode
    std::vector<int> fixed_mode_params = optimal_params;
    fixed_mode_params[2] = 0;
    
    // Run benchmarks with different parameters using the optimal error bound
    if (best_error == 16) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(low_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(high_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(small_batch_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16>>(fixed_mode_params);
    } else if (best_error == 32) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(low_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(high_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(small_batch_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32>>(fixed_mode_params);
    } else if (best_error == 64) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(low_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(high_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(small_batch_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(large_batch_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64>>(fixed_mode_params);
    } else if (best_error == 128) {
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(low_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(high_threshold_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(small_batch_params);
      benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128>>(fixed_mode_params);
    }
    
    // Try alternative search methods based on the dataset
    if (dataset_name == "books_100M") {
      // Books dataset might benefit from interpolation search
      benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, best_error>>(optimal_params);
      
      // For lookup-heavy workloads, also try exponential search
      if (!is_insertion_heavy) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, best_error>>(optimal_params);
      }
    } else if (dataset_name == "fb_100M") {
      // Facebook dataset might benefit from exponential search for lookup-heavy workloads
      if (!is_insertion_heavy) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, ExponentialSearch<record>, best_error>>(optimal_params);
      }
    } else if (dataset_name == "osmc_100M") {
      // OSMC dataset might benefit from linear search for small batches
      if (is_insertion_heavy) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, best_error>>(optimal_params);
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