#include <iostream>
#include <vector>
#include <unistd.h>
#include <random>
#include <optional>
#include <fstream>
#include <atomic>
#include "common.hpp"

#include "pmdk/pipe.hpp"
#include "pmdk/queue.hpp"

using namespace std;

enum TestTarget
{
  PMDK_Queue_Pair,
  PMDK_Queue_Prob,
  PMDK_Pipe
};

TestTarget
parse_target(string target, string kind)
{
  if (target == "pmdk_pipe" && kind == "pipe")
  {
    return TestTarget::PMDK_Pipe;
  }
  else if (target == "pmdk_queue" && kind == "pair")
  {
    return TestTarget::PMDK_Queue_Pair;
  }
  else if (target == "pmdk_queue" && kind.substr(0, 4) == "prob")
  {
    return TestTarget::PMDK_Queue_Prob;
  }
  std::cerr << "Invalid target or bench kind: (target: " << target << ", kind: " << kind << ")" << std::endl;
  exit(0);
}

struct Config
{
  string filepath;
  string target;
  string kind; // bench kind
  int threads;
  double duration;
  int init; // initial number of nodes
  ofstream *output;

  Config(string filepath, string target, string kind, int threads, double duration, int init, ofstream *output) : filepath{filepath}, target{target}, kind{kind}, threads{threads}, duration{duration}, init{init}, output{output} {}
};

Config setup(int argc, char *argv[])
{
  if (argc < 8)
  {
    std::cerr << "no sufficient arguments. plz see usage on readme" << std::endl;
    exit(0);
  }

  srand(time(NULL));
  ifstream f(argv[7]);
  static ofstream of(argv[7], fstream::out | fstream::app);
  if (f.fail())
  {
    of << "target,"
       << "bench kind,"
       << "threads,"
       << "duration,"
       << "relaxed,"
       << "init nodes,"
       << "throughput" << endl;
  }

  //                 filepath, target,  kind,   threads,        duration,      init,          output
  Config cfg = Config(argv[1], argv[2], argv[3], atoi(argv[4]), atof(argv[5]), atoi(argv[6]), &of);
  return cfg;
}

// Calculate throughput when using `nr_thread` threads
double bench(Config cfg)
{
  cout << "bench " << cfg.target + ":" + cfg.kind << " using " << cfg.threads << " threads" << endl;

  TestTarget target = parse_target(cfg.target, cfg.kind);
  int nops = 0;
  switch (target)
  {
  case PMDK_Pipe:
    nops = get_pipe_nops(cfg.filepath, cfg.threads, cfg.duration, cfg.init);
    break;
  case PMDK_Queue_Pair:
    nops = get_queue_nops(cfg.filepath, cfg.threads, cfg.duration, cfg.init, std::nullopt);
    break;
  case PMDK_Queue_Prob:
    int prob = stoi(cfg.kind.substr(4, 3));
    nops = get_queue_nops(cfg.filepath, cfg.threads, cfg.duration, cfg.init, prob);
    break;
    // TODO: other c++ implementations?
  }

  float avg_ops = nops / cfg.duration;
  cout << "avg ops: " << avg_ops << endl;
  return avg_ops;
}

int main(int argc, char *argv[])
{
  Config cfg = setup(argc, argv);
  float avg_ops = bench(cfg);

  // Write result
  *cfg.output
      << cfg.target << ","
      << cfg.kind << ","
      << cfg.threads << ","
      << cfg.duration << ","
      << "none"
      << "," // for relaxed column
      << cfg.init << ","
      << avg_ops << endl;
}
