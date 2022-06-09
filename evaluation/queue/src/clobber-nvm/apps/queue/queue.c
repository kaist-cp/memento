#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include "queue.h"
#include <string.h>

void queueCreate(struct queue **q)
{
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lock);

    *q = queue_new();
    pthread_mutex_unlock(&lock);
}
void queueDestroy(struct queue **q)
{
    queue_delete(*q);
}
void doEnqueue(struct queue *q, uint64_t value, size_t vsize)
{
    pthread_mutex_lock(&q->lock);

    // printf("lock\n");
    queue_enqueue(q, value);
    // printf("unlock\n");

    pthread_mutex_unlock(&q->lock);
}
uint64_t doDequeue(struct queue *q)
{
    pthread_mutex_lock(&q->lock);

    // printf("lock\n");
    uint64_t v = queue_dequeue(q);
    // printf("unlock\n");

    pthread_mutex_unlock(&q->lock);
    return v;
}

void queue_enqueue(struct queue *q, uint64_t value)
{
    struct queuenode *node = queuenode_new(value);
    q->tail->next = node;
    q->tail = node;

    // // Same logic as PMDK
    // struct queuenode *n = queuenode_new(value);
    // if (q->head == NULL && q->tail == NULL)
    // {
    //     q->head = q->tail = n;
    // }
    // else
    // {
    //     q->tail->next = n;
    //     q->tail = n;
    // }
}
uint64_t queue_dequeue(struct queue *q)
{
    struct queuenode *head = q->head;
    if (head->next == NULL)
    {
        // empty
        return NULL;
    }

    struct queuenode *next = head->next;
    q->head = next;
    queuenode_delete(head);
    return q->head->value;

    // Same logic as PMDK

    // char *value = NULL;
    // if (q->head == NULL)
    // {
    //     return value; // EMPTY
    // }
    // value = q->head->value;
    // struct queuenode *next = q->head->next;
    // queuenode_delete(q->head);
    // q->head = next;
    // if (q->head == NULL)
    // {
    //     q->tail = NULL;
    // }
    // return value;
}
struct queuenode *queuenode_new(uint64_t value)
{
    struct queuenode *node;
    node = malloc(sizeof(*node));
    if (node != NULL)
    {
        node->value = value;
        node->next = NULL;
        // memcpy(node->value, value, strlen(value)); // TODO: 필요?
    }
    return node;
}
void queuenode_delete(struct queuenode *node)
{
    // free(node->value);
    free(node);
}

struct queue *queue_new(void)
{
    struct queue *q = malloc(sizeof(*q));
    if (q != NULL)
    {
        // sentinel node
        struct queuenode *node = queuenode_new(0);
        q->head = q->tail = node;
        // q->head = q->tail = NULL;
    }
    pthread_rwlock_init(&(q->lock), NULL);
    return q;
}
void queue_delete(struct queue *q)
{
    // while (queue_dequeue(q) != NULL)
    //     ;
    // // queuenode_delete(q->head);
    // free(q);
}
