#include <stdio.h>
#include <string.h>

extern "C"
{
    void test_simple();
    void test_checkpoint();
    void test_cas();
    void test_queue_O0();
    void test_queue_O1();
    void test_queue_O2();
    void test_queue_comb();
    void test_treiber_stack();
    void test_elim_stack();
    void test_list();
    void test_clevel();
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        printf("Usage: %s <target>\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "simple") == 0)
    {
        test_simple();
    }
    else if (strcmp(argv[1], "checkpoint") == 0)
    {
        test_checkpoint();
    }
    else if (strcmp(argv[1], "detectable_cas") == 0)
    {
        test_cas();
    }
    else if (strcmp(argv[1], "queue_O0") == 0)
    {
        test_queue_O0();
    }
    else if (strcmp(argv[1], "queue_O1") == 0)
    {
        test_queue_O1();
    }
    else if (strcmp(argv[1], "queue_O2") == 0)
    {
        test_queue_O2();
    }
    else if (strcmp(argv[1], "queue_comb") == 0)
    {
        test_queue_comb();
    }
    else if (strcmp(argv[1], "treiber_stack") == 0)
    {
        test_treiber_stack();
    }
    else if (strcmp(argv[1], "elim_stack") == 0)
    {
        test_elim_stack();
    }
    else if (strcmp(argv[1], "list") == 0)
    {
        test_list();
    }
    else if (strcmp(argv[1], "clevel") == 0)
    {
        test_clevel();
    }
    else
    {
        printf("Invalid argument.\n");
        return 1;
    }
    // TODO: Other data structures

    return 0;
}
