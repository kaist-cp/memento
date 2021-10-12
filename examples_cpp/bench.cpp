#include <iostream>
#include <vector>
#include <unistd.h>
#include <random>
#include <optional>
#include <fstream>
#include <atomic>
#include "bench.hpp"

#include "pmdk/pipe.hpp"

using namespace std;

enum TestTarget {
  PMDK_Pipe
};

TestTarget parse_target(string target, string kind) {
  if (target=="pmdk_pipe" && kind=="pipe") {
    return TestTarget::PMDK_Pipe;
  }
  std::cerr << "Invalid target or bench kind: (target: " << target << ", kind: " << kind <<")" << std::endl;
  exit(0);
}

struct Config {
  string filepath;
  string target;
  string kind;
  int threads;
  double duration;
  ofstream *output;

  Config(string filepath, string target, string kind, int threads, double duration, ofstream* output) :
    filepath{filepath}, target{target}, kind{kind}, threads{threads}, duration{duration}, output{output}{}
};

Config setup(int argc, char* argv[]) {
  if (argc < 7) {
    std::cerr << "Argument 부족. plz see usage on readme" << std::endl;
    exit(0);
  }

  ifstream f(argv[6]);
  static ofstream of(argv[6], fstream::out | fstream::app);
  if (f.fail()) {
    of << "target,"
      << "bench kind,"
      << "threads,"
      << "duration,"
      << "throughput" << endl;
  }

  // example: ./bench ./pmem/ pmdk_pipe pipe 16 5
  // TODO: Rust처럼 arg 받게 하기? ./main -f ./pmem/ -a pmdk_pipe -k pipe -t 16 -d 5
  Config cfg = Config(argv[1], argv[2], argv[3], atoi(argv[4]), atof(argv[5]), &of);
  return cfg;
}

// 스레드 `nr_thread`개를 사용할 때의 처리율 계산
double bench(Config cfg) {
  cout << "bench " << cfg.target+":"+cfg.kind << " using " << cfg.threads << " threads" << endl;

  TestTarget target = parse_target(cfg.target, cfg.kind);
  int nops = 0;
  switch (target) {
    case PMDK_Pipe:
      nops = get_pipe_nops(cfg.filepath, cfg.threads, cfg.duration);

    // TODO: other c++ implementations..
  }

  // 처리율 (op/s) 계산하여 반환
  cout << nops << " operations were executed." << endl;
  return nops / cfg.duration;
}

int main(int argc, char* argv[])
{
  Config cfg = setup(argc, argv);
  float avg_mops = bench(cfg);

  // Write result
  *cfg.output
    << cfg.target << ","
    << cfg.kind << ","
    << cfg.threads << ","
    << cfg.duration << ","
    << avg_mops << endl;
}

