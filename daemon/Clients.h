#ifndef DAEMON_HEADS
#define DAEMON_HEADS
#include "heads.h"
#endif // DAEMON_HEADS
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