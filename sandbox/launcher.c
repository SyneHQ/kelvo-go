// SPDX-License-Identifier: Apache-2.0
// kelvo-landlock is a single-threaded launcher for a Kelvo query worker.
//
// Landlock ABI 3 confines the calling thread and its descendants, not sibling
// threads. This program must therefore apply the policy before execing the Go
// worker, whose runtime may create threads immediately.
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <linux/sched.h>
#include <limits.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#if !defined(SYS_landlock_create_ruleset)
# if defined(__x86_64__) || defined(__aarch64__)
#  define SYS_landlock_create_ruleset 444
#  define SYS_landlock_add_rule 445
#  define SYS_landlock_restrict_self 446
# else
#  error "Landlock syscall numbers are only defined here for x86_64 and aarch64"
# endif
#endif

#ifndef LANDLOCK_CREATE_RULESET_VERSION
#define LANDLOCK_CREATE_RULESET_VERSION 1U
#endif
#ifndef LANDLOCK_RULE_PATH_BENEATH
#define LANDLOCK_RULE_PATH_BENEATH 1U
#endif

struct kelvo_landlock_ruleset_attr {
	uint64_t handled_access_fs;
};
struct kelvo_landlock_path_beneath_attr {
	uint64_t allowed_access;
	int32_t parent_fd;
	uint32_t reserved;
};

enum {
	LL_EXECUTE = 1ULL << 0,
	LL_WRITE_FILE = 1ULL << 1,
	LL_READ_FILE = 1ULL << 2,
	LL_READ_DIR = 1ULL << 3,
	LL_REMOVE_DIR = 1ULL << 4,
	LL_REMOVE_FILE = 1ULL << 5,
	LL_MAKE_CHAR = 1ULL << 6,
	LL_MAKE_DIR = 1ULL << 7,
	LL_MAKE_REG = 1ULL << 8,
	LL_MAKE_SOCK = 1ULL << 9,
	LL_MAKE_FIFO = 1ULL << 10,
	LL_MAKE_BLOCK = 1ULL << 11,
	LL_MAKE_SYM = 1ULL << 12,
	LL_REFER = 1ULL << 13,
	LL_TRUNCATE = 1ULL << 14,
};

static const uint64_t fs_all =
	LL_EXECUTE | LL_WRITE_FILE | LL_READ_FILE | LL_READ_DIR |
	LL_REMOVE_DIR | LL_REMOVE_FILE | LL_MAKE_CHAR | LL_MAKE_DIR |
	LL_MAKE_REG | LL_MAKE_SOCK | LL_MAKE_FIFO | LL_MAKE_BLOCK |
	LL_MAKE_SYM | LL_REFER | LL_TRUNCATE;
static const uint64_t fs_read = LL_READ_FILE | LL_READ_DIR;
static const uint64_t fs_runtime = LL_READ_FILE | LL_READ_DIR | LL_EXECUTE;
static const uint64_t fs_write = fs_all & ~LL_EXECUTE;

static void die(const char *message) {
	fprintf(stderr, "kelvo-landlock: %s\n", message);
	exit(125);
}

static void die_errno(const char *message) {
	fprintf(stderr, "kelvo-landlock: %s: %s\n", message, strerror(errno));
	exit(125);
}

static int create_ruleset(void) {
	long abi = syscall(SYS_landlock_create_ruleset, NULL, 0,
			   LANDLOCK_CREATE_RULESET_VERSION);
	if (abi < 0)
		die_errno("Landlock ABI query failed");
	if (abi < 3)
		die("Landlock ABI 3 or newer is required");
	struct kelvo_landlock_ruleset_attr attr = { .handled_access_fs = fs_all };
	long fd = syscall(SYS_landlock_create_ruleset, &attr, sizeof(attr), 0);
	if (fd < 0)
		die_errno("Landlock ruleset creation failed");
	return (int)fd;
}

static void add_path_rule(int ruleset, const char *path, uint64_t allowed,
			  int require_regular, int require_directory) {
	int fd = open(path, O_PATH | O_CLOEXEC);
	if (fd < 0)
		die_errno("cannot open policy path");
	struct stat st;
	if (fstat(fd, &st) != 0)
		die_errno("cannot stat policy path");
	if ((require_regular && !S_ISREG(st.st_mode)) ||
	    (require_directory && !S_ISDIR(st.st_mode))) {
		close(fd);
		die("policy path has the wrong file type");
	}
	// Landlock rejects directory-only rights (READ_DIR, MAKE_*, REMOVE_DIR,
	// REFER) when attached to a regular file. Keep only rights meaningful for
	// a file, including TRUNCATE for writable temporary files.
	if (!S_ISDIR(st.st_mode))
		allowed &= LL_EXECUTE | LL_WRITE_FILE | LL_READ_FILE | LL_TRUNCATE;
	struct kelvo_landlock_path_beneath_attr attr = {
		.allowed_access = allowed,
		.parent_fd = fd,
	};
	if (syscall(SYS_landlock_add_rule, ruleset, LANDLOCK_RULE_PATH_BENEATH,
		    &attr, 0) != 0)
		die_errno("Landlock rule creation failed");
	close(fd);
}

static void add_optional_runtime_rule(int ruleset, const char *path,
				      uint64_t allowed) {
	struct stat st;
	if (stat(path, &st) != 0) {
		if (errno == ENOENT)
			return;
		die_errno("cannot stat runtime path");
	}
	add_path_rule(ruleset, path, allowed, 0, 0);
}

static void add_cgroup_limit(int ruleset, const char *controller,
			     const char *group, const char *name) {
	char path[PATH_MAX];
	int length = snprintf(path, sizeof(path), "/sys/fs/cgroup%s%s/%s",
			      controller, group, name);
	if (length < 0 || (size_t)length >= sizeof(path))
		die("cgroup limit path is too long");
	add_optional_runtime_rule(ruleset, path, LL_READ_FILE);
}

static int has_controller(const char *list, const char *name) {
	size_t length = strlen(name);
	for (const char *p = list; *p != '\0';) {
		const char *end = strchr(p, ',');
		if (end == NULL)
			end = p + strlen(p);
		if ((size_t)(end - p) == length && memcmp(p, name, length) == 0)
			return 1;
		p = *end == ',' ? end + 1 : end;
	}
	return 0;
}

static void add_cgroup_paths(int ruleset) {
	// DuckDB 1.5.6 discovers memory/CPU budgets during database construction.
	// Denying an existing limit file causes its constructor to abort. Permit
	// only our own membership and numeric limit files, never a proc/cgroup
	// directory: environment, maps, other processes and cgroup controls stay
	// inaccessible. The launcher and execed worker keep the same process ID
	// and cgroup membership; moving cgroups must happen before launching us.
	FILE *membership = fopen("/proc/self/cgroup", "re");
	if (membership == NULL)
		die_errno("cannot read cgroup membership");
	add_path_rule(ruleset, "/proc/self/cgroup", LL_READ_FILE, 1, 0);
	char line[PATH_MAX + 256];
	while (fgets(line, sizeof(line), membership) != NULL) {
		if (strchr(line, '\n') == NULL && !feof(membership))
			die("cgroup membership line is too long");
		line[strcspn(line, "\n")] = '\0';
		char *first = strchr(line, ':');
		char *second = first == NULL ? NULL : strchr(first + 1, ':');
		if (first == NULL || second == NULL || second[1] != '/')
			die("invalid cgroup membership");
		*first = *second = '\0';
		const char *controllers = first + 1;
		const char *group = second + 1;
		// Refuse namespace-relative paths reaching outside the visible tree.
		// Current cgroup namespaces normally report "/" for containers.
		if (strstr(group, "/../") != NULL ||
		    (strlen(group) >= 3 && strcmp(group + strlen(group) - 3, "/..") == 0))
			die("cgroup membership is outside the visible namespace");
		if (strcmp(group, "/") == 0)
			group = "";
		if (strcmp(line, "0") == 0 && controllers[0] == '\0') {
			add_cgroup_limit(ruleset, "", group, "memory.max");
			add_cgroup_limit(ruleset, "", group, "cpu.max");
			// DuckDB also checks the namespace root as a fallback.
			add_cgroup_limit(ruleset, "", "", "memory.max");
			add_cgroup_limit(ruleset, "", "", "cpu.max");
		}
		if (has_controller(controllers, "memory")) {
			add_cgroup_limit(ruleset, "/memory", group, "memory.limit_in_bytes");
			add_cgroup_limit(ruleset, "/memory", "", "memory.limit_in_bytes");
		}
		if (has_controller(controllers, "cpu")) {
			add_cgroup_limit(ruleset, "/cpu", group, "cpu.cfs_quota_us");
			add_cgroup_limit(ruleset, "/cpu", group, "cpu.cfs_period_us");
			add_cgroup_limit(ruleset, "/cpu", "", "cpu.cfs_quota_us");
			add_cgroup_limit(ruleset, "/cpu", "", "cpu.cfs_period_us");
		}
	}
	if (ferror(membership))
		die_errno("cannot read cgroup membership");
	fclose(membership);
}

static void add_runtime_paths(int ruleset) {
	// Dynamic loader, C/C++ dependencies, CA roots, DNS configuration and Go's
	// zoneinfo database. These are public runtime inputs, not tenant paths.
	static const char *const executable_trees[] = {
		"/lib", "/lib64", "/usr/lib", "/usr/lib64", "/usr/share/zoneinfo", NULL,
	};
	static const char *const certificate_trees[] = {
		"/etc/ssl/certs", "/etc/pki/tls/certs", "/etc/ca-certificates/extracted",
		"/usr/share/ca-certificates", NULL,
	};
	static const char *const config_files[] = {
		"/etc/hosts", "/etc/resolv.conf", "/etc/nsswitch.conf", "/etc/ld.so.cache", NULL,
	};
	for (size_t i = 0; executable_trees[i] != NULL; ++i)
		add_optional_runtime_rule(ruleset, executable_trees[i], fs_runtime);
	for (size_t i = 0; certificate_trees[i] != NULL; ++i)
		add_optional_runtime_rule(ruleset, certificate_trees[i], fs_read);
	for (size_t i = 0; config_files[i] != NULL; ++i)
		add_optional_runtime_rule(ruleset, config_files[i], fs_read);
}

static void install_dangerous_syscall_filter(void) {
#if defined(__x86_64__)
	const uint32_t audit_arch = AUDIT_ARCH_X86_64;
#elif defined(__aarch64__)
	const uint32_t audit_arch = AUDIT_ARCH_AARCH64;
#else
# error "seccomp architecture is only supported here for x86_64 and aarch64"
#endif
	// Parent cgroup placement happens before this filter. Inside the worker,
	// deny clone3 completely with ENOSYS so libc can fall back to clone for
	// threads; pointed-to clone3 arguments cannot be inspected by classic BPF.
	// Legacy clone keeps ordinary threads/fork but cannot create namespaces.
#define DENY(nr) BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, (nr), 0, 1), \
	BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (EPERM & SECCOMP_RET_DATA))
	struct sock_filter filter[] = {
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)),
		BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, audit_arch, 1, 0),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
#if defined(__x86_64__)
		// x32 syscalls share the x86_64 audit arch but set bit 30. Without
		// this explicit rejection they bypass a number-only denylist.
		BPF_JUMP(BPF_JMP | BPF_JSET | BPF_K, 0x40000000U, 0, 1),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (EPERM & SECCOMP_RET_DATA)),
#endif
#ifdef SYS_clone3
		BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone3, 0, 1),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (ENOSYS & SECCOMP_RET_DATA)),
#endif
#ifdef SYS_clone
		BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone, 0, 3),
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, args[0])),
		BPF_JUMP(BPF_JMP | BPF_JSET | BPF_K,
			CLONE_NEWCGROUP | CLONE_NEWIPC | CLONE_NEWNET | CLONE_NEWNS |
			CLONE_NEWPID | CLONE_NEWUSER | CLONE_NEWUTS, 0, 1),
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (EPERM & SECCOMP_RET_DATA)),
		BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
#endif
#ifdef SYS_setsid
		DENY(SYS_setsid),
#endif
#ifdef SYS_setpgid
		DENY(SYS_setpgid),
#endif
#ifdef SYS_unshare
		DENY(SYS_unshare),
#endif
#ifdef SYS_setns
		DENY(SYS_setns),
#endif
		// Landlock ABI 3 restricts truncation by path. DuckDB needs to resize
		// spill files in the permitted job directory; source paths remain read-only.
#ifdef SYS_openat2
		DENY(SYS_openat2),
#endif
#ifdef SYS_ptrace
		DENY(SYS_ptrace),
#endif
#ifdef SYS_process_vm_readv
		DENY(SYS_process_vm_readv),
#endif
#ifdef SYS_process_vm_writev
		DENY(SYS_process_vm_writev),
#endif
#ifdef SYS_bpf
		DENY(SYS_bpf),
#endif
#ifdef SYS_mount
		DENY(SYS_mount),
#endif
#ifdef SYS_umount2
		DENY(SYS_umount2),
#endif
#ifdef SYS_pivot_root
		DENY(SYS_pivot_root),
#endif
#ifdef SYS_keyctl
		DENY(SYS_keyctl),
#endif
#ifdef SYS_add_key
		DENY(SYS_add_key),
#endif
#ifdef SYS_request_key
		DENY(SYS_request_key),
#endif
#ifdef SYS_init_module
		DENY(SYS_init_module),
#endif
#ifdef SYS_finit_module
		DENY(SYS_finit_module),
#endif
#ifdef SYS_delete_module
		DENY(SYS_delete_module),
#endif
		BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
	};
#undef DENY
	struct sock_fprog program = {
		.len = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
		.filter = filter,
	};
	if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &program) != 0)
		die_errno("seccomp filter installation failed");
}

static char *canonical_path(const char *path) {
	char *resolved = realpath(path, NULL);
	if (resolved == NULL)
		die_errno("cannot resolve policy path");
	if (resolved[0] != '/')
		die("policy path must be absolute");
	return resolved;
}

int main(int argc, char **argv) {
	const char *reads[1024];
	size_t read_count = 0;
	const char *read_execs[64];
	size_t read_exec_count = 0;
	const char *write = NULL;
	int i = 1;
	for (; i < argc && strcmp(argv[i], "--") != 0; ++i) {
		if (strcmp(argv[i], "--read") == 0) {
			if (++i == argc || read_count == sizeof(reads) / sizeof(reads[0]))
				die("invalid --read argument");
			reads[read_count++] = argv[i];
			continue;
		}
		if (strcmp(argv[i], "--read-exec") == 0) {
			if (++i == argc || read_exec_count == sizeof(read_execs) / sizeof(read_execs[0]))
				die("invalid --read-exec argument");
			read_execs[read_exec_count++] = argv[i];
			continue;
		}
		if (strcmp(argv[i], "--write") == 0) {
			if (++i == argc || write != NULL)
				die("invalid --write argument");
			write = argv[i];
			continue;
		}
		die("expected --read, --read-exec, --write, or --");
	}
	if (i == argc || write == NULL || i + 2 >= argc)
		die("usage: --read PATH... --write JOBDIR -- /absolute/kelvo worker");
	if (argv[i + 1][0] != '/')
		die("worker executable must be absolute");
	if (strcmp(argv[i + 2], "worker") != 0 || i + 3 != argc)
		die("launcher may only exec the kelvo worker entrypoint");

	int ruleset = create_ruleset();
	add_runtime_paths(ruleset);
	add_cgroup_paths(ruleset);
	for (size_t n = 0; n < read_count; ++n) {
		char *path = canonical_path(reads[n]);
		add_path_rule(ruleset, path, fs_read, 0, 0);
		free(path);
	}
	for (size_t n = 0; n < read_exec_count; ++n) {
		char *path = canonical_path(read_execs[n]);
		add_path_rule(ruleset, path, fs_runtime, 0, 1);
		free(path);
	}
	char *jobdir = canonical_path(write);
	add_path_rule(ruleset, jobdir, fs_write, 0, 1);
	free(jobdir);
	char *worker = canonical_path(argv[i + 1]);
	add_path_rule(ruleset, worker, fs_read | LL_EXECUTE, 1, 0);

	if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0)
		die_errno("cannot set no_new_privs");
	if (syscall(SYS_landlock_restrict_self, ruleset, 0) != 0)
		die_errno("Landlock enforcement failed");
	close(ruleset);
	install_dangerous_syscall_filter();

	argv[i + 1] = worker;
	execv(worker, &argv[i + 1]);
	die_errno("worker exec failed");
}
