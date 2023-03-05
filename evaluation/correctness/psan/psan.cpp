#include <stdio.h>
#include <string.h>

extern "C"
{
    void test_simple();
    void test_checkpoint();
    void test_cas();
    void test_queue_O0();
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
    else
    {
        printf("Invalid argument.\n");
        return 1;
    }
    // TODO: Other data structures

    return 0;
}