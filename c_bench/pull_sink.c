/*
 * pull_sink.c — NNG PULL0 server that listens and receives exactly N messages.
 *
 * Receives count messages then exits cleanly, so the caller can use its exit
 * as a signal that all messages have been received and can reuse the port.
 *
 * Usage: pull_sink <url> <msg_count>
 * Build: gcc -O2 -o pull_sink pull_sink.c -lnng
 */
#include <stdio.h>
#include <stdlib.h>

#include <nng/nng.h>
#include <nng/protocol/pipeline0/pull.h>

int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s <url> <msg_count>\n", argv[0]);
        return 1;
    }

    const char *url   = argv[1];
    int         count = atoi(argv[2]);

    nng_socket sock;
    int        rv;

    if ((rv = nng_pull0_open(&sock)) != 0) {
        fprintf(stderr, "nng_pull0_open: %s\n", nng_strerror(rv));
        return 1;
    }

    if ((rv = nng_listen(sock, url, NULL, 0)) != 0) {
        fprintf(stderr, "nng_listen(%s): %s\n", url, nng_strerror(rv));
        return 1;
    }

    for (int i = 0; i < count; i++) {
        nng_msg *msg;
        if ((rv = nng_recvmsg(sock, &msg, 0)) != 0) {
            fprintf(stderr, "nng_recvmsg[%d]: %s\n", i, nng_strerror(rv));
            return 1;
        }
        nng_msg_free(msg);
    }

    nng_close(sock);
    return 0;
}
