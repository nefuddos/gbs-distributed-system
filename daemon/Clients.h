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

//#define ICECC_DEBUG 1
#ifndef _GNU_SOURCE
// getopt_long
#define _GNU_SOURCE 1
#endif
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <getopt.h>

#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/un.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <pwd.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/utsname.h>

#ifdef HAVE_ARPA_NAMESER_H
#  include <arpa/nameser.h>
#endif

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#include <arpa/inet.h>

#ifdef HAVE_RESOLV_H
#  include <resolv.h>
#endif
#include <netdb.h>

#ifndef RUSAGE_SELF
#  define RUSAGE_SELF (0)
#endif
#ifndef RUSAGE_CHILDREN
#  define RUSAGE_CHILDREN (-1)
#endif

#ifdef HAVE_LIBCAP_NG
#  include <cap-ng.h>
#endif

#include <deque>
#include <map>
#include <algorithm>
#include <set>
#include <fstream>
#include <string>

#include "ncpus.h"
#include "exitcode.h"
#include "serve.h"
#include "workit.h"
#include "logging.h"
#include <comm.h>
#include "load.h"
#include "environment.h"
#include "platform.h"
#include "util.h"

static std::string pidFilePath;
static volatile sig_atomic_t exit_main_loop = 0;

#ifndef __attribute_warn_unused_result__
#define __attribute_warn_unused_result__
#endif

#include<Client.h>

class Clients : public map<MsgChannel*, Client*>
{
public:
    Clients() {
        active_processes = 0;
    }
    unsigned int active_processes;

    Client *find_by_client_id(int id) const {
        for (const_iterator it = begin(); it != end(); ++it)
            if (it->second->client_id == id) {
                return it->second;
            }

        return 0;
    }

    Client *find_by_channel(MsgChannel *c) const {
        const_iterator it = find(c);

        if (it == end()) {
            return 0;
        }

        return it->second;
    }

    Client *find_by_pid(pid_t pid) const {
        for (const_iterator it = begin(); it != end(); ++it)
            if (it->second->child_pid == pid) {
                return it->second;
            }

        return 0;
    }

    Client *first() {
        iterator it = begin();

        if (it == end()) {
            return 0;
        }

        Client *cl = it->second;
        return cl;
    }

    string dump_status(Client::Status s) const {
        int count = 0;

        for (const_iterator it = begin(); it != end(); ++it) {
            if (it->second->status == s) {
                count++;
            }
        }

        if (count) {
            return toString(count) + " " + Client::status_str(s) + ", ";
        }

        return string();
    }

    string dump_per_status() const {
        string s;

        for (Client::Status i = Client::UNKNOWN; i <= Client::LASTSTATE;
                i = Client::Status(int(i) + 1)) {
            s += dump_status(i);
        }

        return s;
    }
    Client *get_earliest_client(Client::Status s) const {
        // TODO: possibly speed this up in adding some sorted lists
        Client *client = 0;
        int min_client_id = 0;

        for (const_iterator it = begin(); it != end(); ++it) {
            if (it->second->status == s && (!min_client_id || min_client_id > it->second->client_id)) {
                client = it->second;
                min_client_id = client->client_id;
            }
        }

        return client;
    }
};