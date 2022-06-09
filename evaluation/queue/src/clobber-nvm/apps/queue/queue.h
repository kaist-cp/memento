#ifndef _QUEUE_H
#define _QUEUE_H

#include <stdint.h>
#include <pthread.h>

typedef struct queue
{
        pthread_rwlock_t lock;
        struct queuenode *head;
        struct queuenode *tail;
} queue;

struct queuenode
{
        uint64_t value;
        // char *value;
        struct queuenode *next;
};

// void queue_init(struct sk_link *link);
// void queue_add(struct sk_link *link, struct sk_link *prev);
// void queue_del(struct sk_link *link);
// int queue_empty(struct sk_link *link);

struct queue *queue_new(void);
void queue_delete(struct queue *q);

void queue_enqueue(struct queue *q, uint64_t value);
uint64_t queue_dequeue(struct queue *q);

struct queuenode *queuenode_new(uint64_t value);
void queuenode_delete(struct queuenode *node);

void doEnqueue(struct queue *q, uint64_t value, size_t vsize);
uint64_t doDequeue(struct queue *q);

void queueCreate(struct queue **q);
void queueDestroy(struct queue **q);

#endif /* _QUEUE_H */
