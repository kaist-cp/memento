#include <stdio.h>
#include <string.h>

extern "C"
{
    void test_simple(const char*);
    void test_checkpoint(const char*);
    void test_cas(const char*);
    void test_queue_O0(const char*);
    void test_queue_O1(const char*);
    void test_queue_O2(const char*);
    void test_queue_comb(const char*);
    void test_treiber_stack(const char*);
    void test_list(const char*);
    void test_clevel(const char*);
}

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        printf("Usage: %s <target> <pool_postfix>\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "simple") == 0)
    {
        test_simple(argv[2]);
    }
    else if (strcmp(argv[1], "checkpoint") == 0)
    {
        test_checkpoint(argv[2]);
    }
    else if (strcmp(argv[1], "detectable_cas") == 0)
    {
        test_cas(argv[2]);
    }
    else if (strcmp(argv[1], "queue_O0") == 0)
    {
        test_queue_O0(argv[2]);
    }
    else if (strcmp(argv[1], "queue_O1") == 0)
    {
        test_queue_O1(argv[2]);
    }
    else if (strcmp(argv[1], "queue_O2") == 0)
    {
        test_queue_O2(argv[2]);
    }
    else if (strcmp(argv[1], "queue_comb") == 0)
    {
        test_queue_comb(argv[2]);
    }
    else if (strcmp(argv[1], "treiber_stack") == 0)
    {
        test_treiber_stack(argv[2]);
    }
    else if (strcmp(argv[1], "list") == 0)
    {
        test_list(argv[2]);
    }
    else if (strcmp(argv[1], "clevel") == 0)
    {
        test_clevel(argv[2]);
    }
    else
    {
        printf("Invalid argument.\n");
        return 1;
    }
    // TODO: Other data structures

    return 0;
}
