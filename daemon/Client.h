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
#ifndef DAEMON_HEADS
#define DAEMON_HEADS
#include "heads.h"
#endif // DAEMON_HEADS
#include <cassert>
struct Client {
public:
    /*
     * UNKNOWN: Client was just created - not supposed to be long term
     * GOTNATIVE: Client asked us for the native env - this is the first step
     * PENDING_USE_CS: We have a CS from scheduler and need to tell the client
     *          as soon as there is a spot available on the local machine
     * JOBDONE: This was compiled by a local client and we got a jobdone - awaiting END
     * LINKJOB: This is a local job (aka link job) by a local client we told the scheduler about
     *          and await the finish of it
     * TOINSTALL: We're receiving an environment transfer and wait for it to complete.
     * TOCOMPILE: We're supposed to compile it ourselves
     * WAITFORCS: Client asked for a CS and we asked the scheduler - waiting for its answer
     * WAITCOMPILE: Client got a CS and will ask him now (it's not me)
     * CLIENTWORK: Client is busy working and we reserve the spot (job_id is set if it's a scheduler job)
     * WAITFORCHILD: Client is waiting for the compile job to finish.
     * WAITCREATEENV: We're waiting for icecc-create-env to finish.
     */
    enum Status { UNKNOWN, GOTNATIVE, PENDING_USE_CS, JOBDONE, LINKJOB, TOINSTALL, TOCOMPILE,
                  WAITFORCS, WAITCOMPILE, CLIENTWORK, WAITFORCHILD, WAITCREATEENV,
                  LASTSTATE = WAITCREATEENV
                } status;
    Client() {
        job_id = 0;
        channel = 0;
        job = 0;
        usecsmsg = 0;
        client_id = 0;
        status = UNKNOWN;
        pipe_to_child = -1;
        child_pid = -1;
    }

    static string status_str(Status status) {
        switch (status) {
        case UNKNOWN:
            return "unknown";
        case GOTNATIVE:
            return "gotnative";
        case PENDING_USE_CS:
            return "pending_use_cs";
        case JOBDONE:
            return "jobdone";
        case LINKJOB:
            return "linkjob";
        case TOINSTALL:
            return "toinstall";
        case TOCOMPILE:
            return "tocompile";
        case WAITFORCS:
            return "waitforcs";
        case CLIENTWORK:
            return "clientwork";
        case WAITCOMPILE:
            return "waitcompile";
        case WAITFORCHILD:
            return "waitforchild";
        case WAITCREATEENV:
            return "waitcreateenv";
        }

        assert(false);
        return string(); // shutup gcc
    }

    ~Client() {
        status = (Status) - 1;
        delete channel;
        channel = 0;
        delete usecsmsg;
        usecsmsg = 0;
        delete job;
        job = 0;

        if (pipe_to_child >= 0) {
            if (-1 == close(pipe_to_child) && (errno != EBADF)){
                log_perror("close failed");
            }
        }

    }
    uint32_t job_id;
    string outfile; // only useful for LINKJOB or TOINSTALL
    MsgChannel *channel;
    UseCSMsg *usecsmsg;
    CompileJob *job;
    int client_id;
    int pipe_to_child; // pipe to child process, only valid if WAITFORCHILD or TOINSTALL
    pid_t child_pid;
    string pending_create_env; // only for WAITCREATEENV

    string dump() const {
        string ret = status_str(status) + " " + channel->dump();

        switch (status) {
        case LINKJOB:
            return ret + " CID: " + toString(client_id) + " " + outfile;
        case TOINSTALL:
            return ret + " " + toString(client_id) + " " + outfile;
        case WAITFORCHILD:
            return ret + " CID: " + toString(client_id) + " PID: " + toString(child_pid) + " PFD: " + toString(pipe_to_child);
        case WAITCREATEENV:
            return ret + " " + toString(client_id) + " " + pending_create_env;
        default:

            if (job_id) {
                string jobs;

                if (usecsmsg) {
                    jobs = " CS: " + usecsmsg->hostname;
                }

                return ret + " CID: " + toString(client_id) + " ID: " + toString(job_id) + jobs;
            } else {
                return ret + " CID: " + toString(client_id);
            }
        }

        return ret;
    }
};