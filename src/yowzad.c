/*
 * yowzad - native YowzaDB daemon (TCP, text + length-prefixed binary).
 *
 * TEXT PROTOCOL (one command per line terminated by '\n'):
 *   PUT key value\n
 *   GET key\n
 *   DEL key\n
 * Text values cannot contain '\n' (it ends the command) or ' ' in the
 * key (it separates key from value), and a GET reply is the raw value
 * followed by '\n', so a text value containing '\n' would corrupt the
 * framing of the reply. Text commands are unchanged for compatibility.
 *
 * BINARY PROTOCOL (length-prefixed, for keys and values that may
 * contain '\0', '\n' or spaces):
 *   BPUT <key_len> <val_len>\n<key bytes><value bytes>
 *   BGET <key_len>\n<key bytes>
 * Replies use the text style of this file: BPUT replies "OK\n" or
 * "ERR\n" ("ERR INVALID FORMAT\n" for a malformed header); BGET
 * replies "BVAL <val_len>\n<value bytes>" when found (no trailing
 * '\n' after the value: the length is the framing), or "NOT FOUND\n".
 *
 * BINARY KEYS: BPUT/BGET call the length-based yowza_db_*_raw
 * functions, so a key containing '\0' is fully supported here.
 * (The plain text PUT/GET commands are still line-based and cannot
 * express a '\0' in a key; that behaviour is unchanged.) Binary
 * values are likewise fully supported, including '\0' and '\n',
 * because keys and values are passed with explicit lengths and
 * never treated as C strings.
 *
 * BINARY CAPS (taken from yowza_db.c, not invented here):
 * yowza_db_put rejects key_len > 1024, val_len > 1024, and any
 * 1 + 4 + key_len + 4 + val_len + 1 > 1024 (its fixed 1024-byte
 * bytecode buffer); yowza_db_get rejects 1 + 4 + key_len + 1 > 1024.
 * BPUT/BGET enforce the same caps before calling the C API. A header
 * with key_len or val_len above 1024 individually is not a payload
 * that can be safely skipped, so it gets "ERR INVALID FORMAT\n" and
 * the connection is closed rather than desynchronised.
 *
 * TCP FRAMING: TCP is a byte stream, not a message stream. A single
 * read() may return a partial command, several commands, or both.
 * ConnBuf therefore buffers bytes: lines are only executed once a
 * '\n' is seen, and binary payloads are read with read_exact, which
 * consumes exactly n bytes from the buffer plus the socket (so a
 * payload split across TCP segments, or already buffered behind a
 * header line, is handled). A line cut off by EOF is discarded, never
 * executed as a partial command. Lines longer than MAX_LINE are
 * rejected with "ERR LINE TOO LONG" and discarded up to the next '\n'
 * (so the connection stays in sync); they are never executed in part.
 *
 * CONCURRENCY: one pthread per client connection. The shared database
 * is protected by a single global mutex (db_mutex); a whole command,
 * including writing the GET reply, runs under that mutex, so threads
 * never interleave DB operations. (The GET reply points into the
 * arena, so it must be written before another thread can mutate the
 * DB -- holding the mutex across the write is what guarantees that.)
 * Binary payloads are read BEFORE taking db_mutex, so a slow client
 * cannot stall other clients while holding the lock.
 *
 * This file is excluded from the wasm build (see Makefile: it uses
 * POSIX sockets, stdio and pthreads), so using pthread here is fine.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "yowza_db.h"
#include "storage.h"

#define DEFAULT_PORT 6380
#define READ_CHUNK 1024
/* Maximum command line length, excluding the terminating '\n'. */
#define MAX_LINE 4096

/* Global DB instance, serialized by db_mutex (see header comment). */
static YowzaDB db;
static pthread_mutex_t db_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Write all bytes, or return -1 on error / disconnect. */
static int write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) {
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int write_str(int fd, const char *s) {
    return write_all(fd, s, strlen(s));
}

/* Buffered connection reader. There was no LineBuf in the previous
 * version of this file (handle_client read raw chunks directly); this
 * struct is what makes read_exact possible: bytes already read from
 * the socket but not yet consumed (e.g. a binary payload that arrived
 * in the same segment as its header line) must be served before any
 * new read(), or the payload would be lost. */
typedef struct {
    int fd;
    char buf[READ_CHUNK];
    size_t pos; /* next unconsumed byte in buf */
    size_t len; /* bytes currently in buf */
} ConnBuf;

/* Refill buf when empty. Returns 0 if bytes are available, -1 on
 * EOF/error (an incomplete line or payload is then discarded). */
static int conn_fill(ConnBuf *c) {
    if (c->pos < c->len) return 0;
    ssize_t n = read(c->fd, c->buf, sizeof(c->buf));
    if (n <= 0) return -1;
    c->pos = 0;
    c->len = (size_t)n;
    return 0;
}

/* Read exactly n bytes from the buffered stream into out, across as
 * many socket reads as needed. Returns 0 on success, -1 on EOF/error
 * before n bytes were read (the caller must then drop the command). */
static int read_exact(ConnBuf *c, uint8_t *out, size_t n) {
    size_t copied = 0;
    while (copied < n) {
        if (conn_fill(c) < 0) return -1;
        size_t avail = c->len - c->pos;
        size_t take = (n - copied < avail) ? (n - copied) : avail;
        memcpy(out + copied, c->buf + c->pos, take);
        c->pos += take;
        copied += take;
    }
    return 0;
}

/* Read one '\n'-terminated line into line (NUL-terminated, without the
 * '\n'), up to max bytes. Returns 1 for a complete line, 0 on EOF/error
 * (incomplete line discarded, never executed), 2 for an over-long line
 * (already discarded up to and including its '\n'; the caller reports
 * "ERR LINE TOO LONG" once). A line of exactly max bytes is accepted. */
static int conn_read_line(ConnBuf *c, char *line, size_t max) {
    size_t len = 0;
    int overlong = 0;
    for (;;) {
        if (conn_fill(c) < 0) return 0;
        uint8_t ch = (uint8_t)c->buf[c->pos++];
        if (ch == '\n') {
            if (overlong) return 2;
            line[len] = '\0';
            return 1;
        }
        if (overlong) continue;
        if (len >= max) {
            overlong = 1;
            continue;
        }
        line[len++] = (char)ch;
    }
}

/* Execute one complete line (NUL-terminated, without the trailing '\n').
 * Returns 0 to keep the connection, -1 to close it (oversized binary
 * frame header, or EOF while reading a binary payload).
 * Text commands run wholly under db_mutex, as before. Binary payloads
 * are read BEFORE taking db_mutex, so a slow client cannot stall other
 * clients while holding the lock.
 */
static int handle_command(int client_sock, ConnBuf *conn, char *line) {
    /* Strip a trailing '\r' (CRLF clients); the value itself still
     * cannot contain '\n' (see the protocol limitation above). */
    size_t line_len = strlen(line);
    if (line_len > 0 && line[line_len - 1] == '\r') {
        line[line_len - 1] = '\0';
    }

    if (strncmp(line, "BPUT ", 5) == 0) {
        unsigned int klen = 0, vlen = 0;
        if (sscanf(line + 5, "%u %u", &klen, &vlen) != 2) {
            write_str(client_sock, "ERR INVALID FORMAT\n");
            return 0;
        }
        /* Individually-capped lengths only: a larger header cannot be
         * safely skipped (its payload could be arbitrarily long), so
         * close the connection instead of desynchronising it. */
        if (klen == 0 || klen > 1024 || vlen > 1024) {
            write_str(client_sock, "ERR INVALID FORMAT\n");
            return -1;
        }
        char key_buf[1025];
        uint8_t val_buf[1025];
        if (read_exact(conn, (uint8_t*)key_buf, klen) < 0 ||
            read_exact(conn, val_buf, vlen) < 0) {
            return -1; /* EOF mid-payload: drop the command, close */
        }
        /* Length-based C API: keys may contain '\0', so no memchr
         * rejection and no NUL termination are needed (or correct)
         * here; key_buf is passed with its explicit length. */
        /* Mirror yowza_db_put_raw's fixed 1024-byte bytecode budget. */
        if (1 + 4 + klen + 4 + vlen + 1 > 1024) {
            write_str(client_sock, "ERR\n");
            return 0;
        }
        pthread_mutex_lock(&db_mutex);
        int res = yowza_db_put_raw(&db, key_buf, klen, val_buf, vlen);
        pthread_mutex_unlock(&db_mutex);
        write_str(client_sock, res == 0 ? "OK\n" : "ERR\n");
        return 0;
    }

    if (strncmp(line, "BGET ", 5) == 0) {
        unsigned int klen = 0;
        if (sscanf(line + 5, "%u", &klen) != 1) {
            write_str(client_sock, "ERR INVALID FORMAT\n");
            return 0;
        }
        if (klen == 0 || klen > 1024) {
            write_str(client_sock, "ERR INVALID FORMAT\n");
            return -1;
        }
        char key_buf[1025];
        if (read_exact(conn, (uint8_t*)key_buf, klen) < 0) {
            return -1;
        }
        /* As in BPUT: length-based C API, so '\0' in the key is
         * allowed and no NUL termination is applied. */
        if (1 + 4 + klen + 1 > 1024) {
            write_str(client_sock, "ERR\n");
            return 0;
        }
        pthread_mutex_lock(&db_mutex);
        void *val = NULL;
        size_t val_len = 0;
        int res = yowza_db_get_raw(&db, key_buf, klen, &val, &val_len);
        if (res == 0) {
            /* Length-prefixed reply: no trailing '\n' after the value,
             * so binary values (including '\n' and '\0') are framed. */
            char hdr[64];
            int hdr_len = snprintf(hdr, sizeof(hdr), "BVAL %zu\n", val_len);
            if (hdr_len > 0) {
                write_all(client_sock, hdr, (size_t)hdr_len);
                /* val points into the arena; write it while still
                 * holding db_mutex, as with text GET. */
                write_all(client_sock, val, val_len);
            }
        } else {
            write_str(client_sock, "NOT FOUND\n");
        }
        pthread_mutex_unlock(&db_mutex);
        return 0;
    }

    pthread_mutex_lock(&db_mutex);
    if (strncmp(line, "PUT ", 4) == 0) {
        char *key = line + 4;
        char *space = strchr(key, ' ');
        if (space) {
            *space = '\0';
            char *val = space + 1;
            if (yowza_db_put(&db, key, val, strlen(val)) == 0) {
                write_str(client_sock, "OK\n");
            } else {
                write_str(client_sock, "ERR\n");
            }
        } else {
            write_str(client_sock, "ERR INVALID FORMAT\n");
        }
    } else if (strncmp(line, "GET ", 4) == 0) {
        char *key = line + 4;
        void *val = NULL;
        size_t val_len = 0;
        if (yowza_db_get(&db, key, &val, &val_len) == 0) {
            /* val points into the arena (not malloc); do not free it,
             * and write it while still holding db_mutex. */
            if (write_all(client_sock, val, val_len) == 0) {
                write_str(client_sock, "\n");
            }
        } else {
            write_str(client_sock, "NOT FOUND\n");
        }
    } else if (strncmp(line, "DEL ", 4) == 0) {
        char *key = line + 4;
        if (yowza_db_delete(&db, key) == 0) {
            write_str(client_sock, "OK\n");
        } else {
            write_str(client_sock, "ERR\n");
        }
    } else {
        write_str(client_sock, "ERR UNKNOWN COMMAND\n");
    }
    pthread_mutex_unlock(&db_mutex);
    return 0;
}

/* Read a byte stream, split it into '\n'-terminated lines, and execute
 * only complete lines. Returns when the client disconnects or errors.
 */
static void handle_client(int client_sock) {
    ConnBuf conn;
    conn.fd = client_sock;
    conn.pos = 0;
    conn.len = 0;
    char line[MAX_LINE + 1];

    for (;;) {
        int r = conn_read_line(&conn, line, MAX_LINE);
        if (r == 0) {
            break; /* client disconnected or error; incomplete line dropped */
        }
        if (r == 2) {
            write_str(client_sock, "ERR LINE TOO LONG\n");
            continue;
        }
        if (handle_command(client_sock, &conn, line) < 0) {
            break;
        }
    }
}

static void *client_thread(void *arg) {
    int client_sock = *(int *)arg;
    free(arg);
    handle_client(client_sock);
    close(client_sock);
    return NULL;
}

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    if (argc > 1) {
        port = atoi(argv[1]);
    }

    /* Initialize Database */
    static uint8_t memory_buffer[8388608]; /* 8MB Native Arena */
    StorageDriver storage = storage_posix_create("yowzad_native.wal");

    if (yowza_db_init(&db, memory_buffer, sizeof(memory_buffer), storage) != 0) {
        fprintf(stderr, "Failed to initialize native Yowza DB\n");
        return 1;
    }

    /* Setup TCP Socket */
    int server_sock;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        perror("Socket creation failed");
        return 1;
    }

    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        return 1;
    }

    if (listen(server_sock, 10) < 0) {
        perror("Listen failed");
        return 1;
    }

    printf("[Yowzad C-Native] Listening on port %d\n", port);

    /* One thread per client; the shared DB is serialized by db_mutex. */
    for (;;) {
        int client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_len);
        if (client_sock < 0) {
            perror("Accept failed");
            continue;
        }
        int *arg = malloc(sizeof(int));
        if (!arg) {
            close(client_sock);
            continue;
        }
        *arg = client_sock;
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, arg) != 0) {
            perror("pthread_create failed");
            free(arg);
            close(client_sock);
            continue;
        }
        pthread_detach(tid);
    }

    close(server_sock);
    return 0;
}
