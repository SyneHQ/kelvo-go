#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ptrace.h>
#include <unistd.h>

_Noreturn static void hold(void) {
    for (;;) pause();
}

static int exit_child(void *unused) {
    (void)unused;
    _exit(23);
}

static int ready(void) {
    return write(3, "R", 1) == 1 ? 0 : 2;
}

int main(int argc, char **argv) {
    if (argc != 2 || getuid() != 65532 || geteuid() != 65532 || getpid() == 1) return 2;
    if (!strcmp(argv[1], "exit")) return 17;
    if (!strcmp(argv[1], "hold")) {
        if (ready()) return 3;
        hold();
    }
    if (!strcmp(argv[1], "trace-stop")) {
        if (ptrace(PTRACE_TRACEME, 0, NULL, NULL) < 0) return 12;
        if (ready()) return 3;
        if (raise(SIGSTOP)) return 13;
        hold();
    }
    if (!strcmp(argv[1], "block-output")) {
        char buffer[65536] = {0};
        int flags = fcntl(1, F_GETFL);
        if (flags < 0 || (flags & O_NONBLOCK)) return 11;
        if (ready()) return 3;
        for (int i = 0; i < 128; i++) {
            size_t sent = 0;
            while (sent < sizeof(buffer)) {
                ssize_t n = write(1, buffer + sent, sizeof(buffer) - sent);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return 4;
                sent += (size_t)n;
            }
        }
        return 0;
    }
    if (!strcmp(argv[1], "clone-parent-zero-signal")) {
        const size_t size = 1 << 20;
        char *stack = malloc(size);
        if (!stack || clone(exit_child, stack + size, CLONE_PARENT, NULL) < 0) return 5;
        return ready();
    }
    if (!strcmp(argv[1], "double-fork")) {
        pid_t first = fork();
        if (first < 0) return 6;
        if (first > 0) return 0;
        if (setsid() < 0) _exit(7);
        pid_t second = fork();
        if (second < 0) _exit(8);
        if (second > 0) _exit(0);
        if (ready()) _exit(9);
        hold();
    }
    return 10;
}
