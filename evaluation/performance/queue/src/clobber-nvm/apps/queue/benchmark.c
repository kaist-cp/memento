#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include "benchmark.h"
#include <jemalloc/jemalloc.h>

bool pick(int prob)
{
    return (rand() % 100) < prob;
}

typedef struct TraceOp
{
    enum
    {
        Enqueue,
        Dequeue,
    } opCode;
    char key[MAX_KEY_LENGTH];
    struct TraceOp *next;
} TraceOp;

typedef struct
{
    QueuePtrType queue;
    int tid;
    int duration;
    int prob; // workload: if n==-1 { enq; deq; } else {enq n% or deq 100-n%}
    char phase;
    char *tracePath;
    size_t valueSize;
    int init_nodes;
    pthread_barrier_t *barrier;
    size_t totalOps;
} WorkerContext;

void randomContent(char *buffer, size_t sz)
{
    buffer[sz] = 0;
    for (size_t i = 0; i < sz; i++)
    {
        char c = 'A' + rand() % 26;
        if (rand() % 2 == 0)
            c += 'a' - 'A';
        buffer[i] = c;
    }
}

void *worker_init_queue(void *arg)
{
    WorkerContext *ctx = (WorkerContext *)arg;

    int init_nodes = ctx->init_nodes;
    char *value = (char *)malloc(ctx->valueSize);
    *value = ctx->tid;

    printf("t%d start enq %d nodes\n", ctx->tid, init_nodes);
    for (int i = 0; i < init_nodes; i++)
    {
        doEnqueue(ctx->queue, value, strlen(value));
    }
    printf("t%d finish init\n", ctx->tid);

    return NULL;
}

void *worker(void *arg)
{
    WorkerContext *ctx = (WorkerContext *)arg;
    ctx->totalOps = 0;
    int local_ops = 0;
    int prob = ctx->prob;
    char *value = (char *)malloc(ctx->valueSize);
    *value = ctx->tid;

    // Sync with other workers
    pthread_barrier_wait(ctx->barrier);

    // Run the benchmark
    struct timespec begin, end;
    clock_gettime(CLOCK_REALTIME, &begin);
    if (prob == -1)
    {
        while (true)
        {
            clock_gettime(CLOCK_REALTIME, &end);

            int64_t elapsed = (end.tv_sec - begin.tv_sec) * 1E9 + (end.tv_nsec - begin.tv_nsec);
            if ((ctx->duration * 1E9) < elapsed)
            {
                break;
            }

            doEnqueue(ctx->queue, value, strlen(value));
            doDequeue(ctx->queue);
            local_ops += 1;
        }
    }
    else
    {
        while (true)
        {
            clock_gettime(CLOCK_REALTIME, &end);

            int64_t elapsed = (end.tv_sec - begin.tv_sec) * 1E9 + (end.tv_nsec - begin.tv_nsec);
            if ((ctx->duration * 1E9) < elapsed)
            {
                break;
            }

            if (pick(prob))
            {
                doEnqueue(ctx->queue, value, strlen(value));
            }
            else
            {
                doDequeue(ctx->queue);
            }
            local_ops += 1;
        }
    }
    // Count the number of times the op is executed in `duration` seconds
    ctx->totalOps = local_ops;

    // Clean up
    free(value);

    clock_gettime(CLOCK_REALTIME, &end);
    int64_t elapsed = (end.tv_sec - begin.tv_sec) * 1E9 + (end.tv_nsec - begin.tv_nsec);

    return NULL;
}

#ifdef CUSTOM_PTHREAD_CREATE
int custom_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*start_routine)(void *), void *arg);
#endif

uint64_t run(char workload, char phase, QueuePtrType q, int prob, int threadCount, int duration, size_t valueSize, int init_nodes, size_t *totalOps)
{
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, threadCount + 1);

    pthread_t *threads = (pthread_t *)calloc(threadCount, sizeof(pthread_t));
    WorkerContext *contexts = (WorkerContext *)calloc(threadCount,
                                                      sizeof(WorkerContext));
    assert(contexts != NULL);

    // Initialze queue
    char *value = (char *)malloc(valueSize);
    *value = 0;
    printf("start init %d nodes\n", init_nodes);
    for (int i = 0; i < init_nodes; i++)
    {
        doEnqueue(q, value, strlen(value));
    }
    free(value);
    printf("finish init %d nodes\n", init_nodes);

    // Run
    printf("t0~t%d start run\n", threadCount - 1);
    for (int i = 0; i < threadCount; i++)
    {
        WorkerContext *context = &contexts[i];
        QueuePtrAssign(context->queue, q);
        context->tid = i;
        context->phase = phase;
        context->prob = prob;
        context->valueSize = valueSize;
        context->barrier = &barrier;
        context->duration = duration;
        context->init_nodes = init_nodes;
        pthread_create(&threads[i], NULL, worker, &contexts[i]);
    }
    struct timespec t1, t2;
    pthread_barrier_wait(&barrier);
    clock_gettime(CLOCK_REALTIME, &t1); // start time

    if (totalOps != NULL)
        *totalOps = 0;
    assert(totalOps != NULL);
    for (int i = 0; i < threadCount; i++)
    {
        pthread_join(threads[i], NULL);
        *totalOps = *totalOps + contexts[i].totalOps;

        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        int64_t execTime = (t.tv_sec - t1.tv_sec) * 1E9 + (t.tv_nsec - t1.tv_nsec);
    }
    printf("t0~t%d finish run\n", threadCount - 1);
    clock_gettime(CLOCK_REALTIME, &t2);

    free(value);
    free(threads);
    free(contexts);
    pthread_barrier_destroy(&barrier);

    int64_t execTime = (t2.tv_sec - t1.tv_sec) * 1E9;
    execTime += t2.tv_nsec - t1.tv_nsec;
    return (uint64_t)execTime;
}

int main(int argc, char **argv)
{
    QueuePtrType q;
    int prob = -1;
    int threadCount = 1;
    int duration = 0;
    size_t valueSize = 64;
    int init_nodes = 0;
    char workload = 'a';
    char *workload_q;
    FILE *out;

    int opt;
    while ((opt = getopt(argc, argv, ":k:t:d:i:s:o:h")) != -1)
    {
        switch (opt)
        {
        case 'k':
            if (strcmp(optarg, "pair") == 0)
                prob = -1;
            else if (strcmp(optarg, "prob20") == 0)
                prob = 20;
            else if (strcmp(optarg, "prob50") == 0)
                prob = 50;
            else if (strcmp(optarg, "prob80") == 0)
                prob = 80;
            else
                prob = -1;

            size_t wllen = strlen(optarg);
            workload_q = (char *)malloc(wllen + 1);
            assert(workload_q != NULL);
            strcpy(workload_q, optarg);
            workload_q[wllen] = '\0';
            break;
        case 't':
            threadCount = (int)strtol(optarg, NULL, 10);
            break;
        case 's':
            duration = (int)strtol(optarg, NULL, 10);
            break;
        case 'd':
            valueSize = (size_t)strtol(optarg, NULL, 10);
            break;
        case 'i':
            init_nodes = (int)strtol(optarg, NULL, 10);
            break;
        case 'o':
            if (access(optarg, F_OK) != 0)
            {
                // file doesn't exist
                out = fopen(optarg, "a");
                fprintf(out, "target,");
                fprintf(out, "bench kind,");
                fprintf(out, "threads,");
                fprintf(out, "duration,");
                fprintf(out, "relaxed,");
                fprintf(out, "init nodes,");
                fprintf(out, "throughput\n");
            }
            else
            {
                // file exists
                out = fopen(optarg, "a");
            }
            break;
        case 'h':
        default:
            fprintf(stdout, "Benchmark tool for the Queue data structure.\n");
            fprintf(stdout, "-k  kind of workload: {pair, prob20, prob50, prob80}\n");
            fprintf(stdout, "-t  Number of worker threads\n");
            fprintf(stdout, "-s  Test duration (seconds)\n");
            fprintf(stdout, "-d  Data size (bytes) -- must by a multiple of 64\n");
            fprintf(stdout, "-i  Number of initial nodes\n");
            fprintf(stdout, "-r  Show throughput in operations per second\n");
            fprintf(stdout, "-h  Prints this information and returns\n");
            return 0;
            break;
        }
    }
    fprintf(stdout, "Thread count:    %d\n", threadCount);
    fprintf(stdout, "Value size:      %zu\n", valueSize);
    fprintf(stdout, "Prob:      %d\n", prob);
    if (prob == -1)
    {
        printf("Workload: pair\n");
    }
    else
    {
        printf("Workload: prob%d\n", prob);
    }
    printf("Initial nodes: %d\n", init_nodes);

    QueueCreate(q);

    // uint64_t exTmL = run(workload, 'L', q, tracePath, threadCount, duration, valueSize, NULL);

    size_t totalOps;
    uint64_t exTmR = run(workload_q, 'R', q, prob, threadCount, duration, valueSize, init_nodes, &totalOps);

    fprintf(stdout, "Run time:        %zu (%.2f ms)\n", exTmR, exTmR / 1E6);

    unsigned int avg_ops = totalOps / duration;

    printf("Total Ops = %zu\n", totalOps);
    fprintf(stdout, "Throughput: %d Ops/sec\n", avg_ops);

    // Wrtie result
    fprintf(out, "clobber_queue,");   // target
    fprintf(out, "%s,", workload_q);  // kind
    fprintf(out, "%d,", threadCount); // threads
    fprintf(out, "%d,", duration);    // duration
    fprintf(out, "none,");            // relaxed
    fprintf(out, "%d,", init_nodes);  // init nodes
    fprintf(out, "%d\n", avg_ops);    // throughput

    QueueDestroy(q);
    fclose(out);
    return 0;
}
