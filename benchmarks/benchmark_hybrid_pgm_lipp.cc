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
    // Different PGM error bounds with default flush threshold (5%)
    // TODO: Determine if we should go back to OG bounds.
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
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
      } else if (filename.find("0.100000i") != std::string::npos) {
        // Lookup-heavy mixed workload (10% inserts, 90% lookups)
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 1>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16, 1>>();
      }
    }
  }
  if (filename.find("books_100M") != std::string::npos) {
    // Similar patterns for books dataset
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.900000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 128, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, LinearSearch<record>, 64, 10>>();
      } else if (filename.find("0.100000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 32, 1>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, InterpolationSearch<record>, 64, 1>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16, 1>>();
      }
    }
  }
  if (filename.find("osmc_100M") != std::string::npos) {
    // Similar patterns for osmc dataset
    if (filename.find("mix") != std::string::npos) {
      if (filename.find("0.900000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 128, 5>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 10>>();
      } else if (filename.find("0.100000i") != std::string::npos) {
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 16, 1>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 32, 1>>();
        benchmark.template Run<HybridPGMLIPP<uint64_t, BranchingBinarySearch<record>, 64, 1>>();
      }
    }
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);