/* -*- mode: C++; indent-tabs-mode: nil; c-basic-offset: 4; fill-column: 99; -*- */
/* vim: set ts=4 sw=4 et tw=99:  */
/*
    This file is part of Icecream.

    Copyright (c) 2004 Stephan Kulow <coolo@suse.de>
                  2002, 2003 by Martin Pool <mbp@samba.org>

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/
#ifndef DAEMON_HEAD
#define DAEMON_HEAD
#include "Daemon.h"
#endif // !DAEMON_HEAD
struct timeval last_stat;
unsigned int Daemon::max_kids = 0;
volatile sig_atomic_t Daemon::exit_main_loop = 0;
size_t Daemon::cache_size_limit = 100 * 1024 * 1024;
int Daemon::mem_limit = 100;
bool Daemon::setup_listen_fds()
{
    tcp_listen_fd = -1;

    if (!noremote) { // if we only listen to local clients, there is no point in going TCP
        if ((tcp_listen_fd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
            log_perror("socket()");
            return false;
        }

        int optval = 1;

        if (setsockopt(tcp_listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
            log_perror("setsockopt()");
            return false;
        }

        int count = 5;

        while (count) {
            struct sockaddr_in myaddr;
            myaddr.sin_family = AF_INET;
            myaddr.sin_port = htons(daemon_port);
            myaddr.sin_addr.s_addr = INADDR_ANY;

            if (bind(tcp_listen_fd, (struct sockaddr *)&myaddr,
                     sizeof(myaddr)) < 0) {
                log_perror("bind()");
                sleep(2);

                if (!--count) {
                    return false;
                }

                continue;
            } else {
                break;
            }
        }

        if (listen(tcp_listen_fd, 20) < 0) {
            log_perror("listen()");
            return false;
        }

        fcntl(tcp_listen_fd, F_SETFD, FD_CLOEXEC);
    }

    if ((unix_listen_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        log_perror("socket()");
        return false;
    }

    struct sockaddr_un myaddr;

    memset(&myaddr, 0, sizeof(myaddr));

    myaddr.sun_family = AF_UNIX;

    bool reset_umask = false;
    mode_t old_umask = 0;

    if (getenv("ICECC_TEST_SOCKET") == NULL) {
#ifdef HAVE_LIBCAP_NG
        // We run as system daemon (UID has been already changed).
        if (capng_have_capability( CAPNG_EFFECTIVE, CAP_SYS_CHROOT )) {
#else
        if (getuid() == 0) {
#endif
            string default_socket = "/var/run/icecc/iceccd.socket";
            strncpy(myaddr.sun_path, default_socket.c_str() , sizeof(myaddr.sun_path) - 1);
            myaddr.sun_path[sizeof(myaddr.sun_path) - 1] = '\0';
            if(default_socket.length() > sizeof(myaddr.sun_path) - 1) {
                log_error() << "default socket path too long for sun_path" << endl;	
            }
            if (-1 == unlink(myaddr.sun_path) && errno != ENOENT){
                log_perror("unlink failed") << "\t" << myaddr.sun_path << endl;
            }
            old_umask = umask(0);
            reset_umask = true;
        } else { // Started by user.
            if( getenv( "HOME" )) {
                string socket_path = getenv("HOME");
                socket_path.append("/.iceccd.socket");
                strncpy(myaddr.sun_path, socket_path.c_str(), sizeof(myaddr.sun_path) - 1);
                myaddr.sun_path[sizeof(myaddr.sun_path) - 1] = '\0';
                if(socket_path.length() > sizeof(myaddr.sun_path) - 1) {
                    log_error() << "$HOME/.iceccd.socket path too long for sun_path" << endl;
                }
                if (-1 == unlink(myaddr.sun_path) && errno != ENOENT){
                    log_perror("unlink failed") << "\t" << myaddr.sun_path << endl;
                }
            } else {
                log_error() << "launched by user, but $HOME not set" << endl;
                return false;
            }
        }
    } else {
        string test_socket = getenv("ICECC_TEST_SOCKET");
        strncpy(myaddr.sun_path, test_socket.c_str(), sizeof(myaddr.sun_path) - 1);
        myaddr.sun_path[sizeof(myaddr.sun_path) - 1] = '\0';
        if(test_socket.length() > sizeof(myaddr.sun_path) - 1) {
            log_error() << "$ICECC_TEST_SOCKET path too long for sun_path" << endl;
        }
        if (-1 == unlink(myaddr.sun_path) && errno != ENOENT){
            log_perror("unlink failed") << "\t" << myaddr.sun_path << endl;
        }
    }

    if (bind(unix_listen_fd, (struct sockaddr*)&myaddr, sizeof(myaddr)) < 0) {
        log_perror("bind()");

        if (reset_umask) {
            umask(old_umask);
        }

        return false;
    }

    if (reset_umask) {
        umask(old_umask);
    }

    if (listen(unix_listen_fd, 20) < 0) {
        log_perror("listen()");
        return false;
    }

    fcntl(unix_listen_fd, F_SETFD, FD_CLOEXEC);

    return true;
}

void Daemon::determine_system()
{
    struct utsname uname_buf;

    if (uname(&uname_buf)) {
        log_perror("uname call failed");
        return;
    }

    if (nodename.length() && (nodename != uname_buf.nodename)) {
        custom_nodename  = true;
    }

    if (!custom_nodename) {
        nodename = uname_buf.nodename;
    }

    machine_name = determine_platform();
}

string Daemon::determine_nodename()
{
    if (custom_nodename && !nodename.empty()) {
        return nodename;
    }

    // perhaps our host name changed due to network change?
    struct utsname uname_buf;

    if (!uname(&uname_buf)) {
        nodename = uname_buf.nodename;
    }

    return nodename;
}

bool Daemon::send_scheduler(const Msg& msg)
{
    if (!scheduler) {
        log_error() << "scheduler dead ?!" << endl;
        return false;
    }

    if (!scheduler->send_msg(msg)) {
        log_error() << "sending to scheduler failed.." << endl;
        close_scheduler();
        return false;
    }

    return true;
}

bool Daemon::reannounce_environments()
{
    log_error() << "reannounce_environments " << endl;
    LoginMsg lmsg(0, nodename, "");
    lmsg.envs = available_environmnents(envbasedir);
    return send_scheduler(lmsg);
}

void Daemon::close_scheduler()
{
    if (!scheduler) {
        return;
    }

    delete scheduler;
    scheduler = 0;
    delete discover;
    discover = 0;
    next_scheduler_connect = time(0) + 20 + (rand() & 31);
    static bool fast_reconnect = getenv( "ICECC_TESTS" ) != NULL;
    if( fast_reconnect )
        next_scheduler_connect = time(0) + 3;
}

bool Daemon::maybe_stats(bool send_ping)
{
    struct timeval now;
    gettimeofday(&now, 0);

    time_t diff_sent = (now.tv_sec - last_stat.tv_sec) * 1000 + (now.tv_usec - last_stat.tv_usec) / 1000;

    if (diff_sent >= max_scheduler_pong * 1000) {
        StatsMsg msg;
        unsigned int memory_fillgrade;
        unsigned long idleLoad = 0;
        unsigned long niceLoad = 0;

        fill_stats(idleLoad, niceLoad, memory_fillgrade, &msg, clients.active_processes);

        time_t diff_stat = (now.tv_sec - last_stat.tv_sec) * 1000 + (now.tv_usec - last_stat.tv_usec) / 1000;
        last_stat = now;

        /* icecream_load contains time in milliseconds we have used for icecream */
        /* idle time could have been used for icecream, so claim it */
        icecream_load += idleLoad * diff_stat / 1000;

        /* add the time of our childrens, but only the time since the last run */
        struct rusage ru;

        if (!getrusage(RUSAGE_CHILDREN, &ru)) {
            uint32_t ice_msec = ((ru.ru_utime.tv_sec - icecream_usage.tv_sec) * 1000
                                 + (ru.ru_utime.tv_usec - icecream_usage.tv_usec) / 1000) / num_cpus;

            /* heuristics when no child terminated yet: account 25% of total nice as our clients */
            if (!ice_msec && current_kids) {
                ice_msec = (niceLoad * diff_stat) / (4 * 1000);
            }

            icecream_load += ice_msec * diff_stat / 1000;

            icecream_usage.tv_sec = ru.ru_utime.tv_sec;
            icecream_usage.tv_usec = ru.ru_utime.tv_usec;
        }

        int idle_average = icecream_load;

        if (diff_sent) {
            idle_average = icecream_load * 1000 / diff_sent;
        }

        if (idle_average > 1000) {
            idle_average = 1000;
        }

        msg.load = ((700 * (1000 - idle_average)) + (300 * memory_fillgrade)) / 1000;

        if (memory_fillgrade > 600) {
            msg.load = 1000;
        }

        if (idle_average < 100) {
            msg.load = 1000;
        }

#ifdef HAVE_SYS_VFS_H
        struct statfs buf;
        int ret = statfs(envbasedir.c_str(), &buf);

        if (!ret && long(buf.f_bavail) < ((long(max_kids + 1 - current_kids) * 4 * 1024 * 1024) / buf.f_bsize)) {
            msg.load = 1000;
        }

#endif

        // Matz got in the urine that not all CPUs are always feed
        mem_limit = std::max(int(msg.freeMem / std::min(std::max(max_kids, 1U), 4U)), min_mem_limit);

        if (abs(int(msg.load) - current_load) >= 100 || send_ping) {
            if (!send_scheduler(msg)) {
                return false;
            }
        }

        icecream_load = 0;
        current_load = msg.load;
    }

    return true;
}

string Daemon::dump_internals() const
{
    string result;

    result += "Node Name: " + nodename + "\n";
    result += "  Remote name: " + remote_name + "\n";

    for (map<int, MsgChannel *>::const_iterator it = fd2chan.begin(); it != fd2chan.end(); ++it)  {
        result += "  fd2chan[" + toString(it->first) + "] = " + it->second->dump() + "\n";
    }

    for (Clients::const_iterator it = clients.begin(); it != clients.end(); ++it)  {
        result += "  client " + toString(it->second->client_id) + ": " + it->second->dump() + "\n";
    }

    if (cache_size) {
        result += "  Cache Size: " + toString(cache_size) + "\n";
    }

    result += "  Architecture: " + machine_name + "\n";

    for (map<string, NativeEnvironment>::const_iterator it = native_environments.begin();
            it != native_environments.end(); ++it) {
        result += "  NativeEnv (" + it->first + "): " + it->second.name
            + (it->second.create_env_pipe ? " (creating)" : "" ) + "\n";
    }

    if (!envs_last_use.empty()) {
        result += "  Now: " + toString(time(0)) + "\n";
    }

    for (map<string, time_t>::const_iterator it = envs_last_use.begin();
            it != envs_last_use.end(); ++it)  {
        result += "  envs_last_use[" + it->first  + "] = " + toString(it->second) + "\n";
    }

    result += "  Current kids: " + toString(current_kids) + " (max: " + toString(max_kids) + ")\n";

    if (scheduler) {
        result += "  Scheduler protocol: " + toString(scheduler->protocol) + "\n";
    }

    StatsMsg msg;
    unsigned int memory_fillgrade = 0;
    unsigned long idleLoad = 0;
    unsigned long niceLoad = 0;

    fill_stats(idleLoad, niceLoad, memory_fillgrade, &msg, clients.active_processes);
    result += "  cpu: " + toString(idleLoad) + " idle, "
              + toString(niceLoad) + " nice\n";
    result += "  load: " + toString(msg.loadAvg1 / 1000.) + ", icecream_load: "
              + toString(icecream_load) + "\n";
    result += "  memory: " + toString(memory_fillgrade)
              + " (free: " + toString(msg.freeMem) + ")\n";

    return result;
}

int Daemon::scheduler_get_internals()
{
    trace() << "handle_get_internals " << dump_internals() << endl;
    return send_scheduler(StatusTextMsg(dump_internals())) ? 0 : 1;
}

int Daemon::scheduler_use_cs(UseCSMsg *msg)
{
    Client *c = clients.find_by_client_id(msg->client_id);
    trace() << "handle_use_cs " << msg->job_id << " " << msg->client_id
            << " " << c << " " << msg->hostname << " " << remote_name <<  endl;

    if (!c) {
        if (send_scheduler(JobDoneMsg(msg->job_id, 107, JobDoneMsg::FROM_SUBMITTER))) {
            return 1;
        }

        return 1;
    }

    if (msg->hostname == remote_name && int(msg->port) == daemon_port) {
        c->usecsmsg = new UseCSMsg(msg->host_platform, "127.0.0.1", daemon_port, msg->job_id, true, 1,
                                   msg->matched_job_id);
        c->status = Client::PENDING_USE_CS;
    } else {
        c->usecsmsg = new UseCSMsg(msg->host_platform, msg->hostname, msg->port,
                                   msg->job_id, true, 1, msg->matched_job_id);

        if (!c->channel->send_msg(*msg)) {
            handle_end(c, 143);
            return 0;
        }

        c->status = Client::WAITCOMPILE;
    }

    c->job_id = msg->job_id;

    return 0;
}

int Daemon::scheduler_no_cs(NoCSMsg *msg)
{
    Client *c = clients.find_by_client_id(msg->client_id);
    trace() << "handle_no_cs " << msg->job_id << " " << msg->client_id
            << " " << c << " " <<  endl;

    if (!c) {
        if (send_scheduler(JobDoneMsg(msg->job_id, 107, JobDoneMsg::FROM_SUBMITTER))) {
            return 1;
        }

        return 1;
    }

    c->usecsmsg = new UseCSMsg(string(), "127.0.0.1", daemon_port, msg->job_id, true, 1, 0);
    c->status = Client::PENDING_USE_CS;

    c->job_id = msg->job_id;

    return 0;

}

bool Daemon::handle_transfer_env(Client *client, Msg *_msg)
{
    log_error() << "handle_transfer_env" << endl;

    assert(client->status != Client::TOINSTALL &&
           client->status != Client::TOCOMPILE &&
           client->status != Client::WAITCOMPILE);
    assert(client->pipe_to_child < 0);

    EnvTransferMsg *emsg = static_cast<EnvTransferMsg *>(_msg);
    string target = emsg->target;

    if (target.empty()) {
        target =  machine_name;
    }

    int sock_to_stdin = -1;
    FileChunkMsg *fmsg = 0;

    pid_t pid = start_install_environment(envbasedir, target, emsg->name, client->channel,
                                          sock_to_stdin, fmsg, user_uid, user_gid, nice_level);

    client->status = Client::TOINSTALL;
    client->outfile = emsg->target + "/" + emsg->name;
    current_kids++;

    if (pid > 0) {
        log_error() << "got pid " << pid << endl;
        client->pipe_to_child = sock_to_stdin;
        client->child_pid = pid;

        if (!handle_file_chunk_env(client, fmsg)) {
            pid = 0;
        }
    }

    if (pid <= 0) {
        handle_transfer_env_done(client);
    }

    delete fmsg;
    return pid > 0;
}

bool Daemon::handle_transfer_env_done(Client *client)
{
    log_error() << "handle_transfer_env_done" << endl;

    assert(client->outfile.size());
    assert(client->status == Client::TOINSTALL);

    size_t installed_size = finalize_install_environment(envbasedir, client->outfile,
                            client->child_pid, user_uid, user_gid);

    if (client->pipe_to_child >= 0) {
        installed_size = 0;
        close(client->pipe_to_child);
        client->pipe_to_child = -1;
    }

    client->status = Client::UNKNOWN;
    string current = client->outfile;
    client->outfile.clear();
    client->child_pid = -1;
    assert(current_kids > 0);
    current_kids--;

    log_error() << "installed_size: " << installed_size << endl;

    if (installed_size) {
        cache_size += installed_size;
        envs_last_use[current] = time(NULL);
        log_error() << "installed " << current << " size: " << installed_size
                    << " all: " << cache_size << endl;
    }

    check_cache_size(current);

    bool r = reannounce_environments(); // do that before the file compiles

    // we do that here so we're not given out in case of full discs
    if (!maybe_stats(true)) {
        r = false;
    }

    return r;
}

void Daemon::check_cache_size(const string &new_env)
{
    time_t now = time(NULL);

    while (cache_size > cache_size_limit) {
        string oldest;
        // I don't dare to use (time_t)-1
        time_t oldest_time = time(NULL) + 90000;
        string oldest_native_env_key;

        for (map<string, time_t>::const_iterator it = envs_last_use.begin();
                it != envs_last_use.end(); ++it) {
            trace() << "considering cached environment: " << it->first << " " << it->second << " " << oldest_time << endl;
            // ignore recently used envs (they might be in use _right_ now)
            int keep_timeout = 200;
            string native_env_key;

            // If it is a native environment, allow removing it only after a longer period,
            // unless there are many native environments.
            for (map<string, NativeEnvironment>::const_iterator it2 = native_environments.begin();
                    it2 != native_environments.end(); ++it2) {
                if (it2->second.name == it->first) {
                    native_env_key = it2->first;

                    if (native_environments.size() < 5) {
                        keep_timeout = 24 * 60 * 60;    // 1 day
                    }

                    if (it2->second.create_env_pipe) {
                        keep_timeout = 365 * 24 * 60 * 60; // do not remove if it's still being created
                    }
                    break;
                }
            }

            if (it->second < oldest_time && now - it->second > keep_timeout) {
                bool env_currently_in_use = false;

                for (Clients::const_iterator it2 = clients.begin(); it2 != clients.end(); ++it2)  {
                    if (it2->second->status == Client::TOCOMPILE
                            || it2->second->status == Client::TOINSTALL
                            || it2->second->status == Client::WAITFORCHILD) {

                        assert(it2->second->job);
                        string envforjob = it2->second->job->targetPlatform() + "/"
                                           + it2->second->job->environmentVersion();

                        if (envforjob == it->first) {
                            env_currently_in_use = true;
                        }
                    }
                }

                if (!env_currently_in_use) {
                    oldest_time = it->second;
                    oldest = it->first;
                    oldest_native_env_key = native_env_key;
                }
            }
        }

        if (oldest.empty() || oldest == new_env) {
            break;
        }

        size_t removed;

        if (!oldest_native_env_key.empty()) {
            removed = remove_native_environment(oldest);
            native_environments.erase(oldest_native_env_key);
            trace() << "removing " << oldest << " " << oldest_time << " " << removed << endl;
        } else {
            removed = remove_environment(envbasedir, oldest);
            trace() << "removing " << envbasedir << "/" << oldest << " " << oldest_time
                    << " " << removed << endl;
        }

        cache_size -= min(removed, cache_size);
        envs_last_use.erase(oldest);
    }
}

bool Daemon::handle_get_native_env(Client *client, GetNativeEnvMsg *msg)
{
    string env_key;
    map<string, time_t> extrafilestimes;
    env_key = msg->compiler;

    for (list<string>::const_iterator it = msg->extrafiles.begin();
            it != msg->extrafiles.end(); ++it) {
        env_key += ':';
        env_key += *it;
        struct stat st;

        if (stat(it->c_str(), &st) != 0) {
            log_error() << "Extra file " << *it << " for environment not found." << endl;
            client->channel->send_msg(EndMsg());
            handle_end(client, 122);
            return false;
        }

        extrafilestimes[*it] = st.st_mtime;
    }

    if (native_environments[env_key].name.length()) {
        const NativeEnvironment &env = native_environments[env_key];

        if (!compilers_uptodate(env.gcc_bin_timestamp, env.gpp_bin_timestamp, env.clang_bin_timestamp)
                || env.extrafilestimes != extrafilestimes
                || access(env.name.c_str(), R_OK) != 0) {
            trace() << "native_env needs rebuild" << endl;
            cache_size -= remove_native_environment(env.name);
            envs_last_use.erase(env.name);
            if (env.create_env_pipe) {
                if ((-1 == close(env.create_env_pipe)) && (errno != EBADF)){
                    log_perror("close failed");
                }
                // TODO kill the still running icecc-create-env process?
            }
            native_environments.erase(env_key);   // invalidates 'env'
        }
    }

    trace() << "get_native_env " << native_environments[env_key].name
            << " (" << env_key << ")" << endl;

    client->status = Client::WAITCREATEENV;
    client->pending_create_env = env_key;

    if (native_environments[env_key].name.length()) { // already available
        return finish_get_native_env(client, env_key);
    } else {
        NativeEnvironment &env = native_environments[env_key]; // also inserts it
        if (!env.create_env_pipe) { // start creating it only if not already in progress
            env.extrafilestimes = extrafilestimes;
            trace() << "start_create_env " << env_key << endl;
            env.create_env_pipe = start_create_env(envbasedir, user_uid, user_gid, msg->compiler, msg->extrafiles);
        } else {
            trace() << "waiting for already running create_env " << env_key << endl;
        }
    }
    return true;
}

bool Daemon::finish_get_native_env(Client *client, string env_key)
{
    assert(client->status == Client::WAITCREATEENV);
    assert(client->pending_create_env == env_key);
    UseNativeEnvMsg m(native_environments[env_key].name);

    if (!client->channel->send_msg(m)) {
        handle_end(client, 138);
        return false;
    }

    envs_last_use[native_environments[env_key].name] = time(NULL);
    client->status = Client::GOTNATIVE;
    client->pending_create_env.clear();
    return true;
}

bool Daemon::create_env_finished(string env_key)
{
    assert(native_environments.count(env_key));
    NativeEnvironment &env = native_environments[env_key];

    trace() << "create_env_finished " << env_key << endl;
    assert(env.create_env_pipe);
    size_t installed_size = finish_create_env(env.create_env_pipe, envbasedir, env.name);
    env.create_env_pipe = 0;

    // we only clean out cache on next target install
    cache_size += installed_size;
    trace() << "cache_size = " << cache_size << endl;

    if (!installed_size) {
        bool repeat = true;
        while(repeat) {
            repeat = false;
            for (Clients::const_iterator it = clients.begin(); it != clients.end(); ++it)  {
                if (it->second->pending_create_env == env_key) {
                    it->second->channel->send_msg(EndMsg());
                    handle_end(it->second, 121);
                    // The handle_end call invalidates our iterator, so break out of the loop,
                    // but try again just in case, until there's no match.
                    repeat = true;
                    break;
                }
            }
        }
        return false;
    }

    save_compiler_timestamps(env.gcc_bin_timestamp, env.gpp_bin_timestamp, env.clang_bin_timestamp);
    envs_last_use[env.name] = time(NULL);
    check_cache_size(env.name);

    for (Clients::const_iterator it = clients.begin(); it != clients.end(); ++it) {
        if (it->second->pending_create_env == env_key)
            finish_get_native_env(it->second, env_key);
    }
    return true;
}

bool Daemon::handle_job_done(Client *cl, JobDoneMsg *m)
{
    if (cl->status == Client::CLIENTWORK) {
        clients.active_processes--;
    }

    cl->status = Client::JOBDONE;
    JobDoneMsg *msg = static_cast<JobDoneMsg *>(m);
    trace() << "handle_job_done " << msg->job_id << " " << msg->exitcode << endl;

    if (!m->is_from_server()
            && (m->user_msec + m->sys_msec) <= m->real_msec) {
        icecream_load += (m->user_msec + m->sys_msec) / num_cpus;
    }

    assert(msg->job_id == cl->job_id);
    cl->job_id = 0; // the scheduler doesn't have it anymore
    return send_scheduler(*msg);
}

void Daemon::handle_old_request()
{
    while ((current_kids + clients.active_processes) < std::max((unsigned int)1, max_kids)) {

        Client *client = clients.get_earliest_client(Client::LINKJOB);

        if (client) {
            trace() << "send JobLocalBeginMsg to client" << endl;

            if (!client->channel->send_msg(JobLocalBeginMsg())) {
                log_warning() << "can't send start message to client" << endl;
                handle_end(client, 112);
            } else {
                client->status = Client::CLIENTWORK;
                clients.active_processes++;
                trace() << "pushed local job " << client->client_id << endl;

                if (!send_scheduler(JobLocalBeginMsg(client->client_id, client->outfile))) {
                    return;
                }
            }

            continue;
        }

        client = clients.get_earliest_client(Client::PENDING_USE_CS);

        if (client) {
            trace() << "pending " << client->dump() << endl;

            if (client->channel->send_msg(*client->usecsmsg)) {
                client->status = Client::CLIENTWORK;
                /* we make sure we reserve a spot and the rest is done if the
                 * client contacts as back with a Compile request */
                clients.active_processes++;
            } else {
                handle_end(client, 129);
            }

            continue;
        }

        /* we don't want to handle TOCOMPILE jobs as long as our load
           is too high */
        if (current_load >= 1000) {
            break;
        }

        client = clients.get_earliest_client(Client::TOCOMPILE);

        if (client) {
            CompileJob *job = client->job;
            assert(job);
            int sock = -1;
            pid_t pid = -1;

            trace() << "request for job " << job->jobID() << endl;

            string envforjob = job->targetPlatform() + "/" + job->environmentVersion();
            envs_last_use[envforjob] = time(NULL);
            pid = handle_connection(envbasedir, job, client->channel, sock, mem_limit, user_uid, user_gid);
            trace() << "handle connection returned " << pid << endl;

            if (pid > 0) {
                current_kids++;
                client->status = Client::WAITFORCHILD;
                client->pipe_to_child = sock;
                client->child_pid = pid;

                if (!send_scheduler(JobBeginMsg(job->jobID()))) {
                    log_info() << "failed sending scheduler about " << job->jobID() << endl;
                }
            } else {
                handle_end(client, 117);
            }

            continue;
        }

        break;
    }
}

bool Daemon::handle_compile_done(Client *client)
{
    assert(client->status == Client::WAITFORCHILD);
    assert(client->child_pid > 0);
    assert(client->pipe_to_child >= 0);

    JobDoneMsg *msg = new JobDoneMsg(client->job->jobID(), -1, JobDoneMsg::FROM_SERVER);
    assert(msg);
    assert(current_kids > 0);
    current_kids--;

    unsigned int job_stat[8];
    int end_status = 151;

    if (read(client->pipe_to_child, job_stat, sizeof(job_stat)) == sizeof(job_stat)) {
        msg->in_uncompressed = job_stat[JobStatistics::in_uncompressed];
        msg->in_compressed = job_stat[JobStatistics::in_compressed];
        msg->out_compressed = msg->out_uncompressed = job_stat[JobStatistics::out_uncompressed];
        end_status = msg->exitcode = job_stat[JobStatistics::exit_code];
        msg->real_msec = job_stat[JobStatistics::real_msec];
        msg->user_msec = job_stat[JobStatistics::user_msec];
        msg->sys_msec = job_stat[JobStatistics::sys_msec];
        msg->pfaults = job_stat[JobStatistics::sys_pfaults];
    }

    close(client->pipe_to_child);
    client->pipe_to_child = -1;
    string envforjob = client->job->targetPlatform() + "/" + client->job->environmentVersion();
    envs_last_use[envforjob] = time(NULL);

    bool r = send_scheduler(*msg);
    handle_end(client, end_status);
    delete msg;
    return r;
}

bool Daemon::handle_compile_file(Client *client, Msg *msg)
{
    CompileJob *job = dynamic_cast<CompileFileMsg *>(msg)->takeJob();
    assert(client);
    assert(job);
    client->job = job;

    if (client->status == Client::CLIENTWORK) {
        assert(job->environmentVersion() == "__client");

        if (!send_scheduler(JobBeginMsg(job->jobID()))) {
            trace() << "can't reach scheduler to tell him about compile file job "
                    << job->jobID() << endl;
            return false;
        }

        // no scheduler is not an error case!
    } else {
        client->status = Client::TOCOMPILE;
    }

    return true;
}

bool Daemon::handle_verify_env(Client *client, VerifyEnvMsg *msg)
{
    assert(msg);
    bool ok = verify_env(client->channel, envbasedir, msg->target, msg->environment, user_uid, user_gid);
    trace() << "Verify environment done, " << (ok ? "success" : "failure") << ", environment " << msg->environment
            << " (" << msg->target << ")" << endl;
    VerifyEnvResultMsg resultmsg(ok);

    if (!client->channel->send_msg(resultmsg)) {
        log_error() << "sending verify end result failed.." << endl;
        return false;
    }

    return true;
}

bool Daemon::handle_blacklist_host_env(Client *client, Msg *msg)
{
    // just forward
    assert(dynamic_cast<BlacklistHostEnvMsg *>(msg));
    assert(client);

    if (!scheduler) {
        return false;
    }

    return send_scheduler(*msg);
}

void Daemon::handle_end(Client *client, int exitcode)
{
#ifdef ICECC_DEBUG
    trace() << "handle_end " << client->dump() << endl;
    trace() << dump_internals() << endl;
#endif
    fd2chan.erase(client->channel->fd);

    if (client->status == Client::TOINSTALL && client->pipe_to_child >= 0) {
        close(client->pipe_to_child);
        client->pipe_to_child = -1;
        handle_transfer_env_done(client);
    }

    if (client->status == Client::CLIENTWORK) {
        clients.active_processes--;
    }

    if (client->status == Client::WAITCOMPILE && exitcode == 119) {
        /* the client sent us a real good bye, so forget about the scheduler */
        client->job_id = 0;
    }

    /* Delete from the clients map before send_scheduler, which causes a
       double deletion. */
    if (!clients.erase(client->channel)) {
        log_error() << "client can't be erased: " << client->channel << endl;
        flush_debug();
        log_error() << dump_internals() << endl;
        flush_debug();
        assert(false);
    }

    if (scheduler && client->status != Client::WAITFORCHILD) {
        int job_id = client->job_id;

        if (client->status == Client::TOCOMPILE) {
            job_id = client->job->jobID();
        }

        if (client->status == Client::WAITFORCS) {
            job_id = client->client_id; // it's all we have
            exitcode = CLIENT_WAS_WAITING_FOR_CS; // this is the message
        }

        if (job_id > 0) {
            JobDoneMsg::from_type flag = JobDoneMsg::FROM_SUBMITTER;

            switch (client->status) {
            case Client::TOCOMPILE:
                flag = JobDoneMsg::FROM_SERVER;
                break;
            case Client::UNKNOWN:
            case Client::GOTNATIVE:
            case Client::JOBDONE:
            case Client::WAITFORCHILD:
            case Client::LINKJOB:
            case Client::TOINSTALL:
            case Client::WAITCREATEENV:
                assert(false);   // should not have a job_id
                break;
            case Client::WAITCOMPILE:
            case Client::PENDING_USE_CS:
            case Client::CLIENTWORK:
            case Client::WAITFORCS:
                flag = JobDoneMsg::FROM_SUBMITTER;
                break;
            }

            trace() << "scheduler->send_msg( JobDoneMsg( " << client->dump() << ", " << exitcode << "))\n";

            if (!send_scheduler(JobDoneMsg(job_id, exitcode, flag))) {
                trace() << "failed to reach scheduler for remote job done msg!" << endl;
            }
        } else if (client->status == Client::CLIENTWORK) {
            // Clientwork && !job_id == LINK
            trace() << "scheduler->send_msg( JobLocalDoneMsg( " << client->client_id << ") );\n";

            if (!send_scheduler(JobLocalDoneMsg(client->client_id))) {
                trace() << "failed to reach scheduler for local job done msg!" << endl;
            }
        }
    }

    delete client;
}

void Daemon::clear_children()
{
    while (!clients.empty()) {
        Client *cl = clients.first();
        handle_end(cl, 116);
    }

    while (current_kids > 0) {
        int status;
        pid_t child;

        while ((child = waitpid(-1, &status, 0)) < 0 && errno == EINTR) {}

        current_kids--;
    }

    // they should be all in clients too
    assert(fd2chan.empty());

    fd2chan.clear();
    new_client_id = 0;
    trace() << "cleared children\n";
}

bool Daemon::handle_get_cs(Client *client, Msg *msg)
{
    GetCSMsg *umsg = dynamic_cast<GetCSMsg *>(msg);
    assert(client);
    client->status = Client::WAITFORCS;
    umsg->client_id = client->client_id;
    trace() << "handle_get_cs " << umsg->client_id << endl;

    if (!scheduler) {
        /* now the thing is this: if there is no scheduler
           there is no point in trying to ask him. So we just
           redefine this as local job */
        client->usecsmsg = new UseCSMsg(umsg->target, "127.0.0.1", daemon_port,
                                        umsg->client_id, true, 1, 0);
        client->status = Client::PENDING_USE_CS;
        client->job_id = umsg->client_id;
        return true;
    }

    return send_scheduler(*umsg);
}

int Daemon::handle_cs_conf(ConfCSMsg *msg)
{
    max_scheduler_pong = msg->max_scheduler_pong;
    max_scheduler_ping = msg->max_scheduler_ping;
    return 0;
}

bool Daemon::handle_local_job(Client *client, Msg *msg)
{
    client->status = Client::LINKJOB;
    client->outfile = dynamic_cast<JobLocalBeginMsg *>(msg)->outfile;
    return true;
}

bool Daemon::handle_file_chunk_env(Client *client, Msg *msg)
{
    /* this sucks, we can block when we're writing
       the file chunk to the child, but we can't let the child
       handle MsgChannel itself due to MsgChannel's stupid
       caching layer inbetween, which causes us to lose partial
       data after the M_END msg of the env transfer.  */

    assert(client && client->status == Client::TOINSTALL);

    if (msg->type == M_FILE_CHUNK && client->pipe_to_child >= 0) {
        FileChunkMsg *fcmsg = static_cast<FileChunkMsg *>(msg);
        ssize_t len = fcmsg->len;
        off_t off = 0;

        while (len) {
            ssize_t bytes = write(client->pipe_to_child, fcmsg->buffer + off, len);

            if (bytes < 0 && errno == EINTR) {
                continue;
            }

            if (bytes == -1) {
                log_perror("write to transfer env pipe failed. ");

                delete msg;
                msg = 0;
                handle_end(client, 137);
                return false;
            }

            len -= bytes;
            off += bytes;
        }

        return true;
    }

    if (msg->type == M_END) {
        close(client->pipe_to_child);
        client->pipe_to_child = -1;
        return handle_transfer_env_done(client);
    }

    if (client->pipe_to_child >= 0) {
        handle_end(client, 138);
    }

    return false;
}

bool Daemon::handle_activity(Client *client)
{
    assert(client->status != Client::TOCOMPILE);

    Msg *msg = client->channel->get_msg();

    if (!msg) {
        handle_end(client, 118);
        return false;
    }

    bool ret = false;

    if (client->status == Client::TOINSTALL && client->pipe_to_child >= 0) {
        ret = handle_file_chunk_env(client, msg);
    }

    if (ret) {
        delete msg;
        return ret;
    }

    switch (msg->type) {
    case M_GET_NATIVE_ENV:
        ret = handle_get_native_env(client, dynamic_cast<GetNativeEnvMsg *>(msg));
        break;
    case M_COMPILE_FILE:
        ret = handle_compile_file(client, msg);
        break;
    case M_TRANFER_ENV:
        ret = handle_transfer_env(client, msg);
        break;
    case M_GET_CS:
        ret = handle_get_cs(client, msg);
        break;
    case M_END:
        handle_end(client, 119);
        ret = false;
        break;
    case M_JOB_LOCAL_BEGIN:
        ret = handle_local_job(client, msg);
        break;
    case M_JOB_DONE:
        ret = handle_job_done(client, dynamic_cast<JobDoneMsg *>(msg));
        break;
    case M_VERIFY_ENV:
        ret = handle_verify_env(client, dynamic_cast<VerifyEnvMsg *>(msg));
        break;
    case M_BLACKLIST_HOST_ENV:
        ret = handle_blacklist_host_env(client, msg);
        break;
    default:
        log_error() << "not compile: " << (char)msg->type << "protocol error on client "
                    << client->dump() << endl;
        client->channel->send_msg(EndMsg());
        handle_end(client, 120);
        ret = false;
    }

    delete msg;
    return ret;
}

int Daemon::answer_client_requests()
{
#ifdef ICECC_DEBUG

    if (clients.size() + current_kids) {
        log_info() << dump_internals() << endl;
    }

    log_info() << "clients " << clients.dump_per_status() << " " << current_kids
               << " (" << max_kids << ")" << endl;

#endif

    /* reap zombis */
    int status;

    while (waitpid(-1, &status, WNOHANG) < 0 && errno == EINTR) {}

    handle_old_request();

    /* collect the stats after the children exited icecream_load */
    if (scheduler) {
        maybe_stats();
    }

    fd_set listen_set;
    struct timeval tv;

    FD_ZERO(&listen_set);
    int max_fd = 0;

    if (tcp_listen_fd != -1) {
        FD_SET(tcp_listen_fd, &listen_set);
        max_fd = tcp_listen_fd;
    }

    FD_SET(unix_listen_fd, &listen_set);

    if (unix_listen_fd > max_fd) { // very likely
        max_fd = unix_listen_fd;
    }

    for (map<int, MsgChannel *>::const_iterator it = fd2chan.begin();
            it != fd2chan.end();) {
        int i = it->first;
        MsgChannel *c = it->second;
        ++it;
        /* don't select on a fd that we're currently not interested in.
           Avoids that we wake up on an event we're not handling anyway */
        Client *client = clients.find_by_channel(c);
        assert(client);
        int current_status = client->status;
        bool ignore_channel = current_status == Client::TOCOMPILE
                              || current_status == Client::WAITFORCHILD;

        if (!ignore_channel && (!c->has_msg() || handle_activity(client))) {
            if (i > max_fd) {
                max_fd = i;
            }

            FD_SET(i, &listen_set);
        }

        if (current_status == Client::WAITFORCHILD
                && client->pipe_to_child != -1) {
            if (client->pipe_to_child > max_fd) {
                max_fd = client->pipe_to_child;
            }

            FD_SET(client->pipe_to_child, &listen_set);
        }
    }

    if (scheduler) {
        FD_SET(scheduler->fd, &listen_set);

        if (max_fd < scheduler->fd) {
            max_fd = scheduler->fd;
        }
    } else if (discover && discover->listen_fd() >= 0) {
        /* We don't explicitely check for discover->get_fd() being in
        the selected set below.  If it's set, we simply will return
        and our call will make sure we try to get the scheduler.  */
        FD_SET(discover->listen_fd(), &listen_set);

        if (max_fd < discover->listen_fd()) {
            max_fd = discover->listen_fd();
        }
    }

    for (map<string, NativeEnvironment>::const_iterator it = native_environments.begin();
            it != native_environments.end(); ++it) {
        if (it->second.create_env_pipe) {
            FD_SET(it->second.create_env_pipe, &listen_set);
            if (max_fd < it->second.create_env_pipe)
                max_fd = it->second.create_env_pipe;
        }
    }

    tv.tv_sec = max_scheduler_pong;
    tv.tv_usec = 0;

    int ret = select(max_fd + 1, &listen_set, NULL, NULL, &tv);

    if (ret < 0 && errno != EINTR) {
        log_perror("select");
        return 5;
    }
    // Reset debug if needed, but only if we aren't waiting for any child processes to finish,
    // otherwise their debug output could end up reset in the middle (and flush log marks used
    // by tests could be written out before debug output from children).
    if( current_kids == 0 ) {
        reset_debug_if_needed();
    }

    if (ret > 0) {
        bool had_scheduler = scheduler;

        if (scheduler && FD_ISSET(scheduler->fd, &listen_set)) {
            while (!scheduler->read_a_bit() || scheduler->has_msg()) {
                Msg *msg = scheduler->get_msg();

                if (!msg) {
                    log_error() << "scheduler closed connection" << endl;
                    close_scheduler();
                    clear_children();
                    return 1;
                }

                ret = 0;

                switch (msg->type) {
                case M_PING:

                    if (!IS_PROTOCOL_27(scheduler)) {
                        ret = !send_scheduler(PingMsg());
                    }

                    break;
                case M_USE_CS:
                    ret = scheduler_use_cs(static_cast<UseCSMsg *>(msg));
                    break;
                case M_NO_CS:
                    ret = scheduler_no_cs(static_cast<NoCSMsg *>(msg));
                    break;
                case M_GET_INTERNALS:
                    ret = scheduler_get_internals();
                    break;
                case M_CS_CONF:
                    ret = handle_cs_conf(static_cast<ConfCSMsg *>(msg));
                    break;
                default:
                    log_error() << "unknown scheduler type " << (char)msg->type << endl;
                    ret = 1;
                }

                delete msg;

                if (ret) {
                    return ret;
                }
            }
        }

        int listen_fd = -1;

        if (tcp_listen_fd != -1 && FD_ISSET(tcp_listen_fd, &listen_set)) {
            listen_fd = tcp_listen_fd;
        }

        if (FD_ISSET(unix_listen_fd, &listen_set)) {
            listen_fd = unix_listen_fd;
        }

        if (listen_fd != -1) {
            struct sockaddr cli_addr;
            socklen_t cli_len = sizeof cli_addr;
            int acc_fd = accept(listen_fd, &cli_addr, &cli_len);

            if (acc_fd < 0) {
                log_perror("accept error");
            }

            if (acc_fd == -1 && errno != EINTR) {
                log_perror("accept failed:");
                return EXIT_CONNECT_FAILED;
            }

            MsgChannel *c = Service::createChannel(acc_fd, &cli_addr, cli_len);

            if (!c) {
                return 0;
            }

            trace() << "accepted " << c->fd << " " << c->name << endl;

            Client *client = new Client;
            client->client_id = ++new_client_id;
            client->channel = c;
            clients[c] = client;

            fd2chan[c->fd] = c;

            while (!c->read_a_bit() || c->has_msg()) {
                if (!handle_activity(client)) {
                    break;
                }

                if (client->status == Client::TOCOMPILE
                        || client->status == Client::WAITFORCHILD) {
                    break;
                }
            }
        } else {
            for (map<int, MsgChannel *>::const_iterator it = fd2chan.begin();
                    max_fd && it != fd2chan.end();)  {
                int i = it->first;
                MsgChannel *c = it->second;
                Client *client = clients.find_by_channel(c);
                assert(client);
                ++it;

                if (client->status == Client::WAITFORCHILD
                        && client->pipe_to_child >= 0
                        && FD_ISSET(client->pipe_to_child, &listen_set)) {
                    max_fd--;

                    if (!handle_compile_done(client)) {
                        return 1;
                    }
                }

                if (FD_ISSET(i, &listen_set)) {
                    assert(client->status != Client::TOCOMPILE);

                    while (!c->read_a_bit() || c->has_msg()) {
                        if (!handle_activity(client)) {
                            break;
                        }

                        if (client->status == Client::TOCOMPILE
                                || client->status == Client::WAITFORCHILD) {
                            break;
                        }
                    }

                    max_fd--;
                }
            }

            for (map<string, NativeEnvironment>::iterator it = native_environments.begin();
                 it != native_environments.end(); ) {
                if (it->second.create_env_pipe && FD_ISSET(it->second.create_env_pipe, &listen_set)) {
                    if(!create_env_finished(it->first))
                    {
                        native_environments.erase(it++);
                        continue;
                    }
                }
                ++it;
            }

        }

        if (had_scheduler && !scheduler) {
            clear_children();
            return 2;
        }

    }

    return 0;
}

bool Daemon::reconnect()
{
    if (scheduler) {
        return true;
    }

    if (!discover && next_scheduler_connect > time(0)) {
        trace() << "Delaying reconnect." << endl;
        return false;
    }

#ifdef ICECC_DEBUG
    trace() << "reconn " << dump_internals() << endl;
#endif

    if (!discover || (NULL == (scheduler = discover->try_get_scheduler()) && discover->timed_out())) {
        delete discover;
        discover = new DiscoverSched(netname, max_scheduler_pong, schedname, scheduler_port);
    }

    if (!scheduler) {
        log_warning() << "scheduler not yet found/selected." << endl;
        return false;
    }

    delete discover;
    discover = 0;
    sockaddr_in name;
    socklen_t len = sizeof(name);
    int error = getsockname(scheduler->fd, (struct sockaddr*)&name, &len);

    if (!error) {
        remote_name = inet_ntoa(name.sin_addr);
    } else {
        remote_name = string();
    }

    log_info() << "Connected to scheduler (I am known as " << remote_name << ")" << endl;
    current_load = -1000;
    gettimeofday(&last_stat, 0);
    icecream_load = 0;

    LoginMsg lmsg(daemon_port, determine_nodename(), machine_name);
    lmsg.envs = available_environmnents(envbasedir);
    lmsg.max_kids = max_kids;
    lmsg.noremote = noremote;
    return send_scheduler(lmsg);
}

int Daemon::working_loop()
{
    for (;;) {
        reconnect();

        int ret = answer_client_requests();

        if (ret) {
            trace() << "answer_client_requests returned " << ret << endl;
            close_scheduler();
        }

        if (exit_main_loop) {
            close_scheduler();
            clear_children();
            break;
        }
    }
    return 0;
}