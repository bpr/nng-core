/*
 * push_client.c — NNG PUSH0 client that dials once and sends N messages.
 *
 * Unlike nngcat (one process per message), this holds a single persistent
 * connection for the full run, matching the Rust criterion benchmark pattern.
 *
 * Usage: push_client <url> <msg_size_bytes> <msg_count>
 * Build: gcc -O2 -o push_client push_client.c -lnng
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <nng/nng.h>
#include <nng/protocol/pipeline0/push.h>
#include <nng/supplemental/util/platform.h>

int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "usage: %s <url> <msg_size> <msg_count>\n", argv[0]);
        return 1;
    }

    const char *url   = argv[1];
    size_t      sz    = (size_t)atol(argv[2]);
    int         count = atoi(argv[3]);

    nng_socket sock;
    int        rv;

    if ((rv = nng_push0_open(&sock)) != 0) {
        fprintf(stderr, "nng_push0_open: %s\n", nng_strerror(rv));
        return 1;
    }

    if ((rv = nng_dial(sock, url, NULL, 0)) != 0) {
        fprintf(stderr, "nng_dial(%s): %s\n", url, nng_strerror(rv));
        return 1;
    }

    /* Limit NNG's internal send queue to 1 message so each nng_sendmsg call
     * blocks until the previous message has been handed to the TCP writer.
     * Combined with the post-loop sleep, this ensures all bytes are in the
     * kernel TCP buffer (and thus guaranteed for delivery) before nng_close. */
    nng_setopt_int(sock, NNG_OPT_SENDBUF, 1);

    uint8_t *body = malloc(sz);
    if (!body) {
        fprintf(stderr, "malloc(%zu): out of memory\n", sz);
        return 1;
    }
    memset(body, 0xAB, sz);

    for (int i = 0; i < count; i++) {
        nng_msg *msg;
        if ((rv = nng_msg_alloc(&msg, 0)) != 0) {
            fprintf(stderr, "nng_msg_alloc: %s\n", nng_strerror(rv));
            free(body);
            return 1;
        }
        if ((rv = nng_msg_append(msg, body, sz)) != 0) {
            fprintf(stderr, "nng_msg_append: %s\n", nng_strerror(rv));
            nng_msg_free(msg);
            free(body);
            return 1;
        }
        /* nng_sendmsg with flags=0 blocks until the message is queued */
        if ((rv = nng_sendmsg(sock, msg, 0)) != 0) {
            fprintf(stderr, "nng_sendmsg[%d]: %s\n", i, nng_strerror(rv));
            nng_msg_free(msg);
            free(body);
            return 1;
        }
    }

    free(body);

    /* After the last nng_sendmsg the final message is in NNG's send queue.
     * NNG's async TCP writer needs time to move it into the kernel buffer.
     * With SENDBUF=1, this is at most one message.  Sleep 5 s (generous for
     * any payload size ≤ 64 MiB at even 10 MiB/s) so nng_close doesn't
     * discard the last in-flight message.  The sink is timed independently,
     * so this sleep is invisible to the benchmark measurements. */
    nng_msleep(5000);
    nng_close(sock);
    return 0;
}
