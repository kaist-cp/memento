#define _GNU_SOURCE
#include <unistd.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/signal.h>

int main(int argc, char *argv[])
{
    int sig = SIGKILL;
    int tgid = -1;
    int tid;

    if (argc == 1)
    {
        printf("Usage: %s [-SIGNUM] [<tgid>] <tid>\n", argv[0]);
        return 1;
    }
    else if (argc == 2)
    {
        tid = atoi(argv[1]);
    }
    else if (argc == 3)
    {
        tgid = atoi(argv[1]);
        tid = atoi(argv[2]);
    }
    else if (argc == 4)
    {
        sig = -atoi(argv[1]);
        tgid = atoi(argv[2]);
        tid = atoi(argv[3]);
    }

#if defined(DEBUG)
    printf("DEBUG: Killing thread %d of thread group %d with signal %d\n", tid, tgid, sig);
    printf("tgkill(tgid: %d, tid: %d, sig: %d)\n", tgid, tid, sig);
#endif

    return syscall(SYS_tgkill, tgid, tid, sig);
}