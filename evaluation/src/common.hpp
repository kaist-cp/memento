#include <thread>
#include <unistd.h>
#include <vector>
#include <atomic>

#include <iostream>

#define ONE_MILLION 1 * 1000 * 1000              // util
#define SEC_2_MICRO_SEC(sec) ((sec)*ONE_MILLION) // util

#define POOL_SIZE 80 * 1024 * 1024 * 1024 // 80 GB
