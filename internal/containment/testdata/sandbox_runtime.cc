// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <thread>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
int main(int argc, char**) {
 if (argc != 2) return 2;
 // Null arguments cannot create a task. Verify the defensive filter result.
 errno = 0;
 if (syscall(SYS_clone3, nullptr, 0) != -1 || errno != ENOSYS) return 3;
 std::atomic<int> finished{0};
 std::thread threads[4];
 for (auto& thread : threads) thread = std::thread([&] { finished.fetch_add(1); });
 for (auto& thread : threads) thread.join();
 if (finished.load() != 4) return 4;
 pid_t pid = fork();
 if (pid < 0) return 5;
 if (pid == 0) _exit(0);
 int status = 0;
 if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status)) return 6;
 std::puts("clone3_denied native_threads_ok ordinary_fork_ok");
 return 0;
}
