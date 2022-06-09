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

void *worker(void *arg)
{
    WorkerContext *ctx = (WorkerContext *)arg;
    // TraceOp *ops = NULL;
    // Load the trace
    ctx->totalOps = 0;
    // FILE *trace = fopen(ctx->tracePath, "r");
    // assert(trace != NULL);
    // char line[255];
    // while (fgets(line, sizeof(line), trace))
    // {
    //     // remove trailing new-line characters
    //     if (line[strlen(line) - 1] == '\n')
    //         line[strlen(line) - 1] = '\0';
    //     if (line[strlen(line) - 1] == '\r')
    //         line[strlen(line) - 1] = '\0';

    //     char *delim = strchr(line, ' ');
    //     if (delim == NULL)
    //         continue;
    //     delim[0] = '\0';

    //     TraceOp *t = (TraceOp *)malloc(sizeof(TraceOp));
    //     strcpy(t->key, delim + 1);
    //     t->next = ops;
    //     if (strcmp(line, "Enqueue") == 0)
    //         t->opCode = Enqueue;
    //     else if (strcmp(line, "Dequeue") == 0)
    //         t->opCode = Dequeue;
    //     else
    //     {
    //         fprintf(stderr, "unknown operation: %s\n", line);
    //         free(t);
    //         continue;
    //     }
    //     ops = t;
    //     ctx->totalOps++;
    // }
    // fclose(trace);

    // // Prepare buffers
    // char *value = (char *)malloc(ctx->valueSize);
    // randomContent(value, ctx->valueSize - 1);
    // char *buffer = (char *)malloc(ctx->valueSize);

    // //printf("list addr = %p \n", ctx->list);
    // // Load half the data before measuring the load latency
    // if (ctx->phase == 'L')
    // {
    //     size_t halfOps = ctx->totalOps / 2;
    //     while (halfOps-- > 0)
    //     {
    //         assert(ops->opCode == Enqueue);
    //         //printf("key = %s, value = %s \n", ops->key, value);

    //         doEnqueue(ctx->queue, value, strlen(value));
    //         // doInsert(ctx->queue, ops->key, strlen(ops->key), value, strlen(value));
    //         TraceOp *t = ops;
    //         ops = ops->next;
    //         free(t);
    //         ctx->totalOps--; // exclude from throughput measurements
    //     }
    // }

    // // Sync with other workers
    // pthread_barrier_wait(ctx->barrier);
    // // Run the benchmark

    // while (ops != NULL)
    // {
    //     switch (ops->opCode)
    //     {
    //     case Enqueue:
    //         // doInsert(ctx->queue, ops->key, strlen(ops->key), value, strlen(value));
    //         doEnqueue(ctx->queue, value, strlen(value));
    //         break;
    //     case Dequeue:
    //         doDequeue(ctx->queue);
    //         //doUpdate(ctx->list, ops->key, strlen(ops->key), value, strlen(value));
    //         break;
    //     }
    //     TraceOp *t = ops;
    //     ops = ops->next;
    //     free(t);
    // }

    // // Clean-up
    // free(value);
    // free(buffer);

    // Count the number of times the op is executed in `duration` seconds

    // Sync with other workers
    pthread_barrier_wait(ctx->barrier);

    char *value = (char *)malloc(ctx->valueSize);
    *value = ctx->tid;

    int local_ops = 0;
    int prob = ctx->prob;
    struct timespec begin, end;
    clock_gettime(CLOCK_REALTIME, &begin);
    // printf("[start] %d\n", ctx->tid);

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
    ctx->totalOps = local_ops;
    // printf("[end] %d\n", ctx->tid);
    // free(value);

    clock_gettime(CLOCK_REALTIME, &end);
    int64_t elapsed = (end.tv_sec - begin.tv_sec) * 1E9 + (end.tv_nsec - begin.tv_nsec);
    // long elapsed = end.tv_sec - begin.tv_sec;
    // int64_t execTime = elapsed * 1E9;
    // execTime += end.tv_nsec - begin.tv_nsec;
    // fprintf(stdout, "t%d time:       %zu (%.2f ms)\n", i, execTime, execTime / 1E6);

    return NULL;
}

#ifdef CUSTOM_PTHREAD_CREATE
int custom_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*start_routine)(void *), void *arg);
#endif

uint64_t run(char workload, char phase, QueuePtrType q, int prob, int threadCount, int duration, size_t valueSize, size_t *totalOps)
{
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, threadCount + 1);

    pthread_t *threads = (pthread_t *)calloc(threadCount, sizeof(pthread_t));
    WorkerContext *contexts = (WorkerContext *)calloc(threadCount,
                                                      sizeof(WorkerContext));
    assert(contexts != NULL);
    for (int i = 0; i < threadCount; i++)
    {
        // printf("[run] start worker %d\n", i);
        WorkerContext *context = &contexts[i];
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

        QueuePtrAssign(context->queue, q);
        // char *traceBuffer = (char *)malloc(strlen(tracePath) + strlen(pathPostfix) + 1);
        // strcpy(traceBuffer, tracePath);
        // strcat(traceBuffer, pathPostfix);
        context->tid = i;
        context->phase = phase;
        // context->tracePath = traceBuffer;
        context->prob = prob;
        context->valueSize = valueSize;
        context->barrier = &barrier;
        context->duration = duration;
#ifdef CUSTOM_PTHREAD_CREATE
        custom_pthread_create(&threads[i], NULL, worker, &contexts[i]);
#else
        pthread_create(&threads[i], NULL, worker, &contexts[i]);
#endif
    }

    struct timespec t1, t2;
    pthread_barrier_wait(&barrier);
    clock_gettime(CLOCK_REALTIME, &t1);

    if (totalOps != NULL)
        *totalOps = 0;
    for (int i = 0; i < threadCount; i++)
    {
        pthread_join(threads[i], NULL);
        if (totalOps != NULL)
            *totalOps = *totalOps + contexts[i].totalOps; // TODO don't log or log once outside the loop

        // free(contexts[i].tracePath);

        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        int64_t execTime = (t.tv_sec - t1.tv_sec) * 1E9 + (t.tv_nsec - t1.tv_nsec);
    }
    clock_gettime(CLOCK_REALTIME, &t2);

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
    char workload = 'a';
    bool showMops = true;

    int opt;
    while ((opt = getopt(argc, argv, ":p:t:d:s:rh")) != -1)
    {
        switch (opt)
        {
        case 'p':
            prob = (int)strtol(optarg, NULL, 10);
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
        case 'r':
            showMops = false;
            break;
        case 'h':
        default:
            fprintf(stdout, "Benchmark tool for the Queue data structure.\n");
            fprintf(stdout, "-p if -1 (pair) else (prob{n})\n");
            fprintf(stdout, "-t  Number of worker threads\n");
            fprintf(stdout, "-s  Test duration (seconds)\n");
            fprintf(stdout, "-d  Data size (bytes) -- must by a multiple of 64\n");
            fprintf(stdout, "-r  Show throughput in operations per second\n");
            fprintf(stdout, "-h  Prints this information and returns\n");
            return 0;
            break;
        }
    }

    // assert(tracePath != NULL);
    // assert(threadCount > 0 && threadCount < 64);
    // assert(valueSize > 0 && valueSize % 64 == 0);

    QueueCreate(q);

    // uint64_t exTmL = run(workload, 'L', q, tracePath, threadCount, duration, valueSize, NULL);

    size_t totalOps;
    uint64_t exTmR = run(workload, 'R', q, prob, threadCount, duration, valueSize, &totalOps);

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

    // fprintf(stdout, "Load time:       %zu (%.2f ms)\n", exTmL, exTmL / 1E6);
    fprintf(stdout, "Run time:        %zu (%.2f ms)\n", exTmR, exTmR / 1E6);

    if (showMops)
    {
        fprintf(stdout, "Throughput:      ");
        // fprintf(stdout, "%.2f Mops/sec\n", totalOps / (exTmL / 1E3));
    }
    else
    {
        // totalOps = 1000000;
        printf("[main] Total Ops = %zu\n", totalOps);
        // fprintf(stdout, "Load throughput: ");
        // fprintf(stdout, "%d Ops/sec\n", (unsigned int)(totalOps / (exTmL / 1E9)));
        fprintf(stdout, "Run throughput:  ");
        fprintf(stdout, "%d Ops/sec\n", (unsigned int)(totalOps / (exTmR / 1E9)));
    }

    // printf("[main] destroy q\n");
    QueueDestroy(q);
    // printf("[main] free tracePath\n");
    return 0;
}
