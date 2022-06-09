#include "txutil.h"

int SPIN_INIT = 16;
int SPIN_CELL = 1024;
float SPIN_FACTOR = 2;
tm_stats_t *tm_stats_head = 0; // stats for master thread and head of list
__thread tm_stats_t* my_tm_stats = 0; // thread-local stats
tm_stats_t tm_stats = {0};             // global stats, updated only when a thread exits

// state for HTM speculation
__thread void * volatile __attribute__ ((aligned(128))) spec_entry = 0;

// constants controlling HTM speculation
uint32_t TK_MIN_DISTANCE = 0;
uint32_t TK_MAX_DISTANCE = 2;
uint32_t TK_NUM_TRIES    = 2;
bool TM_COND_VARS = true;

// now a #define
// USE_PTHREAD_COND_VARS = true;
