#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <jemalloc/jemalloc.h>

#define MAX_KEY_SIZE    64

// make the comparison with PMDK a fair one
void *oid_to_pointer(uintptr_t base, uint64_t offset) {
    return (void*)(base + offset);
}

struct hashmap_root;
int hashmap_create(struct hashmap_root **);
int hashmap_recover(struct hashmap_root **);
void hashmap_close(struct hashmap_root *);
size_t hashmap_size(struct hashmap_root *);
void doInsert(struct hashmap_root *, char *, size_t, char *, size_t);
void doUpdate(struct hashmap_root *, char *, size_t, char *, size_t);
void doRead(struct hashmap_root *, char *, size_t, char *, size_t);


typedef struct TraceOp {
    enum { Insert, Update, Read } opCode;
    char key[MAX_KEY_SIZE];
    struct TraceOp *next;
} TraceOp;

typedef struct {
    struct hashmap_root *rootPtr;
    char phase;
    char *tracePath;
    size_t valueSize;
    pthread_barrier_t *barrier;
    size_t totalOps;
} WorkerContext;

void randomContent(char *buffer, size_t sz) {
    buffer[sz] = 0;
    for (size_t i = 0; i < sz; i++) {
        buffer[i] = 'A' + rand() % 26;
        if (rand() % 2 == 0) buffer[i] += 'a' - 'A';
    }
}

uint64_t hash(unsigned char *str) {
    uint64_t hash = 5381;
    int c;
    while (*str != '\0') {
        c = *str;
        hash = ((hash << 5) + hash) + c;
        str++;
    }
    return hash;
}


void *worker(void *arg) {
    WorkerContext *ctx = (WorkerContext *)arg;
    TraceOp *ops = NULL;

    // Load the trace
    ctx->totalOps = 0;
    FILE* trace = fopen(ctx->tracePath, "r");
    assert(trace != NULL);
    char line[255];
    while (fgets(line, sizeof(line), trace)) {
        // remove trailing new-line characters
        if (line[strlen(line) - 1] == '\n') line[strlen(line) - 1] = '\0';
        if (line[strlen(line) - 1] == '\r') line[strlen(line) - 1] = '\0';

        char *delim = strchr(line, ' ');
        if (delim == NULL) continue;
        delim[0] = '\0';

        TraceOp *t = (TraceOp *)malloc(sizeof(TraceOp));
        strcpy(t->key, delim + 1);
        t->next = ops;
        if (strcmp(line, "Read") == 0) t->opCode = Read;
        else if (strcmp(line, "Add") == 0) t->opCode = Insert;
        else if (strcmp(line, "Update") == 0) t->opCode = Update;
        else {
            fprintf(stderr, "unknown operation: %s\n", line);
            free(t);
            continue;
        }
        ops = t;
        ctx->totalOps++;
    }
    fclose(trace);

    // Prepare buffers
    char *value = NULL;
    assert(posix_memalign((void**)&value, 64, ctx->valueSize) == 0);
    randomContent(value, ctx->valueSize - 1);
    char *buffer = NULL;
    assert(posix_memalign((void**)&buffer, 64, ctx->valueSize) == 0);

    // Load half the data before measuring the load latency
    if (ctx->phase == 'L') {
        size_t halfOps = ctx->totalOps / 2;
        while (halfOps-- > 0) {
            assert(ops->opCode == Insert);
            doInsert(ctx->rootPtr, ops->key, strlen(ops->key), value, strlen(value));
            TraceOp *t = ops;
            ops = ops->next;
            free(t);
            ctx->totalOps--; // exclude from throughput measurements
        }
    }

    // Sync with other workers
    pthread_barrier_wait(ctx->barrier);

    // Run the benchmark
    while (ops != NULL) {
        switch(ops->opCode) {
            case Insert:
                doInsert(ctx->rootPtr, ops->key, strlen(ops->key),value, strlen(value));
                break;
            case Update:
                //doUpdate(ctx->rootPtr, ops->key, strlen(ops->key), value, strlen(value));
                break;
            case Read:
                //doRead(ctx->rootPtr, ops->key, strlen(ops->key), buffer, strlen(buffer));
                break;
        }
        TraceOp *t = ops;
        ops = ops->next;
        free(t);
    }

    // Clean-up
    free(value);
    free(buffer);

    return NULL;
}

#ifdef CUSTOM_PTHREAD_CREATE
int custom_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
        void *(*start_routine) (void *), void *arg);
#endif

uint64_t run(char workload, char phase, struct hashmap_root *rootPtr,
        char *tracePath, int threadCount, size_t valueSize, size_t *totalOps) {
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, threadCount + 1);

    pthread_t *threads = (pthread_t *)calloc(threadCount, sizeof(pthread_t));
    WorkerContext *contexts = (WorkerContext *)calloc(threadCount,
            sizeof(WorkerContext));
    assert(contexts != NULL);
    for (int i = 0; i < threadCount; i++) {
        // tracePath += workload-phase-threadCount.i
        char pathPostfix[16], buffer[16];
        pathPostfix[0] = workload;
        pathPostfix[1] = '\0';
        strcat(pathPostfix, phase == 'L' ? "-load-" : "-run-");
        sprintf(buffer, "%d", threadCount);
        strcat(pathPostfix, buffer);
        strcat(pathPostfix, ".");
        sprintf(buffer, "%d", i);
        strcat(pathPostfix, buffer);

        contexts[i].phase = phase;
        contexts[i].tracePath =
            (char *)malloc(strlen(tracePath) + strlen(pathPostfix) + 1);
        strcpy(contexts[i].tracePath, tracePath);
        strcat(contexts[i].tracePath, pathPostfix);

        contexts[i].rootPtr = rootPtr;
        contexts[i].barrier = &barrier;
        contexts[i].valueSize = valueSize;
#ifdef CUSTOM_PTHREAD_CREATE
        custom_pthread_create(&threads[i], NULL, worker, &contexts[i]);
#else
        pthread_create(&threads[i], NULL, worker, &contexts[i]);
#endif
    }

    struct timespec t1, t2;
    pthread_barrier_wait(&barrier);
    clock_gettime(CLOCK_REALTIME, &t1);

    if (totalOps != NULL) *totalOps = 0;
    for (int i = 0; i < threadCount; i++) {
        pthread_join(threads[i], NULL);
        if (totalOps != NULL) *totalOps = *totalOps + contexts[i].totalOps;
        free(contexts[i].tracePath);
    }
    clock_gettime(CLOCK_REALTIME, &t2);

    free(threads);
    free(contexts);
    pthread_barrier_destroy(&barrier);

    int64_t execTime = (t2.tv_sec - t1.tv_sec) * 1E9;
    execTime += t2.tv_nsec - t1.tv_nsec;
    return (uint64_t)execTime;
}

int main(int argc, char **argv) {
    char *tracePath = NULL;
    int threadCount = 1;
    size_t valueSize = 64;
    char workload = 'a';
    bool showMops = true;

    int opt;
    while ((opt = getopt(argc, argv, ":f:t:d:w:rh")) != -1) {
        switch(opt) {
            case 'w':
                if (optarg[0] >= 'a' || optarg[0] <= 'f') {
                    workload = optarg[0];
                }
                else if (optarg[0] >= 'A' || optarg[0] <= 'F') {
                    workload = (optarg[0] - 'A') + 'a';
                }
                break;
            case 'f':
                {
                assert(tracePath == NULL);
                size_t pathLen = strlen(optarg);
                tracePath = (char *)malloc(pathLen + 2);
                assert(tracePath != NULL);
                strcpy(tracePath, optarg);
                tracePath[pathLen] = '\0';
                tracePath[pathLen + 1] = '\0';
                if (tracePath[pathLen - 1] != '/') tracePath[pathLen] = '/';
                }
                break;
            case 't':
                threadCount = (int)strtol(optarg, NULL, 10);
                break;
            case 'd':
                valueSize = (size_t)strtol(optarg, NULL, 10);
                break;
            case 'r':
                showMops = false;
                break;
            case 'h':
            default:
                fprintf(stdout, "Benchmark tool for the HashMap data structure.\n");
                fprintf(stdout, "-f  Trace path for running YCSB load and run phases\n");
                fprintf(stdout, "-w  YCSB workload (A, B, C, D, E or F)\n");
                fprintf(stdout, "-t  Number of worker threads\n");
                fprintf(stdout, "-d  Data size (bytes) -- must be a multiple of 64\n");
                fprintf(stdout, "-r  Show throughput in operations per second\n");
                fprintf(stdout, "-h  Prints this information and returns\n");
                if (tracePath != NULL) free(tracePath);
                return 0;
                break;
        }
    }

    assert(tracePath != NULL);
    assert(threadCount > 0 && threadCount < 64);
    assert(valueSize > 0 && valueSize % 64 == 0);

    // Create (or recover) the hash-tables
    struct hashmap_root *rootPtr = NULL;
    uint64_t exTmL = 0;
    if (hashmap_recover(&rootPtr) != 0) {
        int t = hashmap_create(&rootPtr);
        assert(t == 0);
        exTmL = run(workload, 'L', rootPtr, tracePath, threadCount, valueSize, NULL);
    }

#ifdef KAMINO
    while (!tardis_log_is_empty()) {}
#endif // KAMINO

    size_t totalOps;
    uint64_t exTmR = run(workload, 'R', rootPtr, tracePath, threadCount,
            valueSize, &totalOps);

    size_t hashSize = hashmap_size(rootPtr);
    fprintf(stdout, "Thread count:    %d\n", threadCount);
    fprintf(stdout, "Value size:      %zu\n", valueSize);
    fprintf(stdout, "HashMap size:    %zu\n", hashSize);
    fprintf(stdout, "Load time:       %zu (%.2f ms)\n", exTmL, exTmL / 1E6);
//    fprintf(stdout, "Run time:        %zu (%.2f ms)\n", exTmR, exTmR / 1E6);
    if (showMops) {
        fprintf(stdout, "Throughput:      ");
        fprintf(stdout, "%.2f Mops/sec\n", totalOps / (exTmL / 1E3));
    }
    else {
		totalOps = 1000000;
        fprintf(stdout, "Load throughput: ");
        fprintf(stdout, "%d Ops/sec\n", (unsigned int)(totalOps / (exTmL / 1E9)));
//        fprintf(stdout, "Run throughput:  ");
//        fprintf(stdout, "%d Ops/sec\n", (unsigned int)(totalOps / (exTmR / 1E9)));
    }

    hashmap_close(rootPtr);
    free(tracePath);
    return 0;
}
