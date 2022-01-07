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
  PMDK_Queue,
  PMDK_Pipe
};

TestTarget parse_target(string target, string kind)
{
  if (target == "pmdk_pipe" && kind == "pipe")
  {
    return TestTarget::PMDK_Pipe;
  }
  else if (target == "pmdk_queue" && kind == "pair")
  {
    return TestTarget::PMDK_Queue;
  }
  std::cerr << "Invalid target or bench kind: (target: " << target << ", kind: " << kind << ")" << std::endl;
  exit(0);
}

struct Config
{
  string filepath;
  string target;
  string kind;
  int threads;
  double duration;
  ofstream *output;

  Config(string filepath, string target, string kind, int threads, double duration, ofstream *output) : filepath{filepath}, target{target}, kind{kind}, threads{threads}, duration{duration}, output{output} {}
};

Config setup(int argc, char *argv[])
{
  if (argc < 7)
  {
    std::cerr << "Argument 부족. plz see usage on readme" << std::endl;
    exit(0);
  }

  ifstream f(argv[6]);
  static ofstream of(argv[6], fstream::out | fstream::app);
  if (f.fail())
  {
    of << "target,"
       << "bench kind,"
       << "threads,"
       << "duration,"
       << "relaxed,"
       << "throughput" << endl;
  }

  // example: ./bench ./pmem/ pmdk_pipe pipe 16 5
  // TODO: Rust처럼 arg 받게 하기? ./main -f ./pmem/ -a pmdk_pipe -k pipe -t 16 -d 5
  Config cfg = Config(argv[1], argv[2], argv[3], atoi(argv[4]), atof(argv[5]), &of);
  return cfg;
}

// 스레드 `nr_thread`개를 사용할 때의 처리율 계산
double bench(Config cfg)
{
  cout << "bench " << cfg.target + ":" + cfg.kind << " using " << cfg.threads << " threads" << endl;

  TestTarget target = parse_target(cfg.target, cfg.kind);
  int nops = 0;
  switch (target)
  {
  case PMDK_Pipe:
    nops = get_pipe_nops(cfg.filepath, cfg.threads, cfg.duration);
    break;
  case PMDK_Queue:
    nops = get_queue_pair_nops(cfg.filepath, cfg.threads, cfg.duration);
    // TODO: prbo50 test?
    break;

    // TODO: other c++ implementations..
  }

  // 처리율 (op/s) 계산하여 반환
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
      << "none," // relaxed (TODO: relaxed는 그냥 csv에 안찍는게 좋겠다)
      << avg_ops << endl;
}
