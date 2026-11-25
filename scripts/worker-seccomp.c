#include <seccomp.h>
#include <stdio.h>
#include <sys/prctl.h>
#include <unistd.h>

static int add_rule(scmp_filter_ctx filter, const char *name, unsigned int action,
                    unsigned int count, const struct scmp_arg_cmp *args) {
  int number = seccomp_syscall_resolve_name(name);
  // An unavailable allowlisted syscall stays denied by the default action.
  if (number == __NR_SCMP_ERROR) return action == SCMP_ACT_ALLOW ? 0 : -1;
  return seccomp_rule_add_array(filter, action, number, count, args);
}

#include "worker-seccomp-rules.h"

int main(int argc, char **argv) {
  if (argc < 2 || prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
    fputs("browser_worker_seccomp_setup_failed\n", stderr);
    return 1;
  }
  scmp_filter_ctx filter = seccomp_init(DEFAULT_ACTION);
  if (!filter || configure_filter(filter) < 0 || seccomp_load(filter) < 0) {
    fputs("browser_worker_seccomp_setup_failed\n", stderr);
    return 1;
  }
  seccomp_release(filter);
  execvp(argv[1], argv + 1);
  fputs("browser_worker_seccomp_exec_failed\n", stderr);
  return 1;
}
