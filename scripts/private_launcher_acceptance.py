#!/usr/bin/env python3
"""Compile and test private-worker containment on a disposable Linux host.

Requires a C compiler and Landlock ABI 3+. Uses synthetic socket pairs only.
The test opens no listener and reads no application credentials.
"""
import argparse
import array
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import subprocess
import tempfile
import time

HELPER = r'''
#define _GNU_SOURCE
#include <errno.h>
#include <linux/io_uring.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>
int main(int argc, char **argv) {
    if (argc != 2 || strcmp(argv[1], "worker")) return 10;
    if (socket(AF_INET, SOCK_STREAM, 0) != -1 || errno != EPERM) return 11;
    if (socket(AF_INET6, SOCK_STREAM, 0) != -1 || errno != EPERM) return 12;
    if (socket(AF_UNIX, SOCK_STREAM, 0) != -1 || errno != EPERM) return 13;
    int pair[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != -1 || errno != EPERM) return 14;
    struct sockaddr target = {.sa_family = AF_UNIX};
    if (connect(7, &target, sizeof(target)) != -1 || errno != EPERM) return 15;
#ifdef SYS_io_uring_setup
    struct io_uring_params params = {0};
    if (syscall(SYS_io_uring_setup, 1, &params) != -1 || errno != EPERM) return 16;
#endif
    char data[2], control[CMSG_SPACE(sizeof(int))];
    struct iovec iov = {.iov_base = data, .iov_len = sizeof(data)};
    struct msghdr message = {.msg_iov = &iov, .msg_iovlen = 1,
        .msg_control = control, .msg_controllen = sizeof(control)};
    if (recvmsg(7, &message, MSG_CMSG_CLOEXEC) != 2) return 17;
    struct cmsghdr *header = CMSG_FIRSTHDR(&message);
    if (!header || header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS) return 18;
    int stream; memcpy(&stream, CMSG_DATA(header), sizeof(stream));
    char bytes[5];
    if (read(stream, bytes, sizeof(bytes)) != 5 || memcmp(bytes, "bound", 5)) return 19;
    if (write(stream, "proof", 5) != 5) return 20;
    close(stream); close(7);
    return 0;
}
'''


def run(source):
    compiler = shutil.which("cc")
    if compiler is None:
        raise RuntimeError("A C compiler is required")
    with tempfile.TemporaryDirectory(prefix="kelvo-private-launcher-") as temp:
        root = Path(temp)
        launcher, helper = root / "launcher", root / "worker"
        helper_source = root / "worker.c"
        helper_source.write_text(HELPER)
        for input_path, output_path in ((source, launcher), (helper_source, helper)):
            subprocess.run([compiler, "-Wall", "-Wextra", "-Werror", "-O2", str(input_path), "-o", str(output_path)], check=True, timeout=30)
        results = []
        cases = ("private", "stream", "file", "disconnected", "missing", "unexpected", "snapshot", "jdbc", "unpinned")
        for case in cases:
            parents = []
            control, inherited = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET if case != "stream" else socket.SOCK_STREAM)
            parents += [control, inherited]
            remote, stream = socket.socketpair()
            parents += [remote, stream]
            remote.settimeout(2)
            if case == "private":
                control.sendmsg([b"\x01\x00"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [stream.fileno()]))])
                remote.sendall(b"bound")
            binary = os.open(helper, os.O_RDONLY)
            extra = os.open(helper_source, os.O_RDONLY)
            log = os.open(root / (case + ".log"), os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600)
            selected = inherited.fileno()
            unconnected = None
            if case == "file":
                selected = extra
            elif case == "disconnected":
                unconnected = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                parents.append(unconnected)
                selected = unconnected.fileno()
            # Duplicate inputs above all reserved target descriptors before fork.
            import fcntl
            copies = [fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 32) for fd in (binary, selected, extra, log)]
            args = [str(launcher)]
            if case != "unexpected":
                args += ["--operation-private"]
            if case == "jdbc":
                args += ["--operation-jdbc", "1"]
            args += ["--write", str(root), "--", str(helper) if case == "unpinned" else "/proc/self/fd/5", "worker"]
            pid = os.fork()
            if pid == 0:
                try:
                    os.dup2(copies[3], 1); os.dup2(copies[3], 2)
                    os.dup2(copies[0], 5)
                    if case != "missing": os.dup2(copies[1], 7)
                    else:
                        try: os.close(7)
                        except OSError: pass
                    if case == "snapshot": os.dup2(copies[2], 6)
                    else:
                        try: os.close(6)
                        except OSError: pass
                    os.closerange(3, 5)
                    os.closerange(8, min(resource.getrlimit(resource.RLIMIT_NOFILE)[0], 1048576))
                    os.execv(launcher, args)
                finally:
                    os._exit(125)
            for fd in copies + [binary, extra, log]: os.close(fd)
            deadline = time.monotonic() + 5
            while True:
                done, status = os.waitpid(pid, os.WNOHANG)
                if done: break
                if time.monotonic() > deadline:
                    os.kill(pid, signal.SIGKILL); os.waitpid(pid, 0)
                    raise RuntimeError("Launcher test exceeded its deadline: " + case)
                time.sleep(0.01)
            code = os.waitstatus_to_exitcode(status)
            if case == "private":
                if code != 0:
                    raise RuntimeError("Private launcher failed: " + str(code) + " " + (root / (case + ".log")).read_text())
                if remote.recv(5) != b"proof": raise RuntimeError("Inherited stream did not preserve bytes")
            elif code == 0:
                raise RuntimeError("Launcher accepted invalid channel: " + case)
            results.append({"case": case, "exit": code})
            for item in parents: item.close()
        return {"passed": True, "cases": results, "scope": "Native launcher, Landlock, inherited SCM_RIGHTS, direct socket and io_uring denial"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1] / "sandbox/launcher.c")
    args = parser.parse_args()
    print(json.dumps(run(args.source), indent=2))
