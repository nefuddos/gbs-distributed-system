#ifndef DAEMON_HEADS
#define DAEMON_HEADS
#include "heads.h"
#endif // DAEMON_HEADS
struct NativeEnvironment {
    string name; // the hash
    map<string, time_t> extrafilestimes;
    // Timestamps for compiler binaries, if they have changed since the time
    // the native env was built, it needs to be rebuilt.
    time_t gcc_bin_timestamp;
    time_t gpp_bin_timestamp;
    time_t clang_bin_timestamp;
    int create_env_pipe; // if in progress of creating the environment
    NativeEnvironment() {
        gcc_bin_timestamp = 0;
        gpp_bin_timestamp = 0;
        clang_bin_timestamp = 0;
        create_env_pipe = 0;
    }
};