/*
 * SMP (Simple Media Protocol) relay server — pure C, non-blocking.
 * Single-threaded event loop (kqueue on macOS, epoll on Linux).
 * Reference: ~/Dev/livsho/smp/main.go
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <sys/event.h>
#define USE_KQUEUE 1
#else
#include <sys/epoll.h>
#define USE_EPOLL 1
#endif

/* suppress GNU variadic macro extension warning (harmless, we use it intentionally) */
#pragma GCC diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"

/* --- protocol constants --- */

#define SMP_MAGIC       "SMP0"
#define SMP_VERSION     2
#define MODE_PULL       0
#define MODE_PUSH       1
#define STATUS_OK       0
#define STATUS_BAD_HELLO 1
#define STATUS_OCCUPIED 2
#define MSG_HEADER      0x01
#define MSG_PACKET      0x02
#define MSG_KEY         0x03

#define SESSION_ID_LEN  16
#define MAX_PATH_LEN    255
#define MAX_MSG_LEN     (64 * 1024 * 1024)
#define RB_CAP          (MAX_MSG_LEN + 8)
#define WQ_CAP          1024  /* must be power of 2 */
#define MAX_GOP_MSGS    4096
#define GOP_WARN_BYTES  (32 * 1024 * 1024)
#define HEADER_WAIT_SECS 10
#define STREAM_HTAB_SIZE 256  /* must be power of 2 */
#define MAX_CONNS       4096
#define MAX_EVENTS      128

/* --- logging --- */

#define ANSI_RESET      "\033[0m"
#define ANSI_WHITE      "\033[97m"
#define ANSI_GREEN      "\033[1;92m"
#define ANSI_YELLOW     "\033[1;93m"
#define ANSI_RED        "\033[1;91m"
#define ANSI_BLUE       "\033[1;94m"

enum log_level {
    LOG_DEBUG = 0,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
};

static int log_level = LOG_INFO;

static void log_emit(int level, const char *label, const char *color,
                     const char *msg, ...) {
    if (level < log_level)
        return;

    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm);

    fprintf(stderr, "%s%s%s %s%s%s %s", ANSI_WHITE, ts, ANSI_RESET,
            color, label, ANSI_RESET, msg);

    va_list args;
    va_start(args, msg);
    const char *key;
    while ((key = va_arg(args, const char *)) != NULL) {
        const char *val = va_arg(args, const char *);
        fprintf(stderr, " %s%s%s=%s", ANSI_WHITE, key, ANSI_RESET, val);
    }
    va_end(args);
    fprintf(stderr, "\n");
}

#define log_debug(msg, ...) log_emit(LOG_DEBUG, "DEBU", ANSI_BLUE, msg, ##__VA_ARGS__, NULL)
#define log_info(msg, ...)  log_emit(LOG_INFO, "INFO", ANSI_GREEN, msg, ##__VA_ARGS__, NULL)
#define log_warn(msg, ...)  log_emit(LOG_WARN, "WARN", ANSI_YELLOW, msg, ##__VA_ARGS__, NULL)
#define log_error(msg, ...) log_emit(LOG_ERROR, "ERRO", ANSI_RED, msg, ##__VA_ARGS__, NULL)

/* --- msg_t: reference-counted framed message --- */

typedef struct msg {
    uint32_t refcount;
    uint8_t mtype;
    uint32_t wire_len;
    uint8_t wire[];
} msg_t;

static msg_t *msg_new(const uint8_t *payload, uint32_t payload_len) {
    uint32_t total = 4 + payload_len;
    msg_t *m = malloc(sizeof(*m) + total);
    if (!m)
        return NULL;
    m->refcount = 1;
    m->mtype = payload[0];
    m->wire_len = total;
    *(uint32_t *)m->wire = htonl(payload_len);
    memcpy(m->wire + 4, payload, payload_len);
    return m;
}

static void msg_ref(msg_t *m) { m->refcount++; }

static void msg_unref(msg_t *m) {
    if (--m->refcount == 0)
        free(m);
}

/* --- wq_t: write queue --- */

typedef struct {
    msg_t *msgs[WQ_CAP];
    int head, tail, len;
} wq_t;

static bool wq_empty(wq_t *q) { return q->len == 0; }
static bool wq_full(wq_t *q) { return q->len == WQ_CAP; }

static bool wq_push(wq_t *q, msg_t *m) {
    if (wq_full(q))
        return false;
    msg_ref(m);
    q->msgs[q->tail] = m;
    q->tail = (q->tail + 1) & (WQ_CAP - 1);
    q->len++;
    return true;
}

static msg_t *wq_peek(wq_t *q) {
    if (wq_empty(q))
        return NULL;
    return q->msgs[q->head];
}

static void wq_pop(wq_t *q) {
    if (!wq_empty(q)) {
        q->head = (q->head + 1) & (WQ_CAP - 1);
        q->len--;
    }
}

/* --- stream_t: per-path relay state --- */

typedef struct stream stream_t;

typedef struct stream {
    char path[MAX_PATH_LEN + 1];

    msg_t *header;
    msg_t *gop[MAX_GOP_MSGS];
    int gop_head;
    int gop_len;
    uint32_t gop_bytes;
    bool gop_warned;

    bool publisher_active;
    uint8_t session_id[SESSION_ID_LEN];
    bool has_session;
    uint64_t dropped_frames;

    struct conn *sub_head;
    struct conn *pending_head;

    stream_t *next;
} stream_t;

static uint32_t djb2(const char *str) {
    uint32_t h = 5381;
    while (*str)
        h = ((h << 5) + h) ^ (unsigned char)*str++;
    return h;
}

typedef struct {
    stream_t *tab[STREAM_HTAB_SIZE];
    int count;
} stream_table_t;

static stream_table_t streams;

static stream_t *stream_get(const char *path) {
    uint32_t h = djb2(path) & (STREAM_HTAB_SIZE - 1);
    stream_t **pp = &streams.tab[h];
    while (*pp) {
        if (strcmp((*pp)->path, path) == 0)
            return *pp;
        pp = &(*pp)->next;
    }
    stream_t *s = calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    strncpy(s->path, path, MAX_PATH_LEN);
    s->next = streams.tab[h];
    streams.tab[h] = s;
    streams.count++;
    return s;
}

static bool stream_claim(stream_t *s, const uint8_t *sid, bool has_sid,
                         bool *seamless_out) {
    if (s->publisher_active)
        return false;

    bool seamless = has_sid && s->has_session && memcmp(s->session_id, sid, SESSION_ID_LEN) == 0 && s->header != NULL;
    if (!seamless) {
        if (s->header) {
            msg_unref(s->header);
            s->header = NULL;
        }
        for (int i = 0; i < s->gop_len; i++)
            msg_unref(s->gop[i]);
        s->gop_len = 0;
        s->gop_bytes = 0;
        s->gop_warned = false;
    }
    memcpy(s->session_id, sid, SESSION_ID_LEN);
    s->has_session = has_sid;
    s->publisher_active = true;
    *seamless_out = seamless;
    return true;
}

static void stream_release(stream_t *s) { s->publisher_active = false; }

static void stream_set_header(stream_t *s, msg_t *m) {
    if (s->header)
        msg_unref(s->header);
    msg_ref(m);
    s->header = m;
    for (int i = 0; i < s->gop_len; i++)
        msg_unref(s->gop[i]);
    s->gop_len = 0;
    s->gop_bytes = 0;
    s->gop_warned = false;
}

static void stream_gop_append(stream_t *s, msg_t *m) {
    if (s->gop_len < MAX_GOP_MSGS) {
        msg_ref(m);
        s->gop[s->gop_len++] = m;
        s->gop_bytes += m->wire_len;
        if (!s->gop_warned && s->gop_bytes > GOP_WARN_BYTES) {
            log_warn("gop exceeded byte threshold", "path", s->path, NULL);
            s->gop_warned = true;
        }
    }
}

/* --- conn_t: per-connection state machine --- */

typedef enum {
    CS_READ_HANDSHAKE,
    CS_WRITE_RESPONSE,
    CS_PUBLISHER,
    CS_SUBSCRIBER,
    CS_CLOSING,
} conn_state_t;

typedef struct conn {
    int fd;
    conn_state_t state;
    stream_t *stream;
    bool is_publisher;

    /* read side */
    uint8_t rbuf[RB_CAP];
    int rbuf_len;
    int rbuf_off;

    /* publisher message assembly */
    uint8_t *msg_buf;
    int msg_cap;
    int msg_len;
    uint32_t msg_total;

    /* write side */
    wq_t wq;
    uint8_t *wbuf_ptr;
    int wbuf_remaining;

    /* handshake / response buffers */
    uint8_t hs_buf[8 + MAX_PATH_LEN + SESSION_ID_LEN];
    int hs_len;
    uint8_t resp[6];
    int resp_sent;

    /* metadata */
    char peer[64];
    bool seamless;
    time_t added_time;

    /* list links */
    struct conn *next_sub;
    struct conn *next_pending;
    struct conn *next_free;
} conn_t;

/* --- server_t: top-level state --- */

typedef struct {
#ifdef USE_KQUEUE
    int kqfd;
#else
    int epfd;
#endif
    int listenfd;
    conn_t *conns[MAX_CONNS];
    conn_t *free_list;
} server_t;

static server_t server;

/* --- platform-specific event loop --- */

#ifdef USE_KQUEUE
static void ev_init(void) {
    server.kqfd = kqueue();
    if (server.kqfd < 0) {
        perror("kqueue");
        exit(1);
    }
}

static void ev_add(int fd, bool read, bool write) {
    struct kevent kevs[2];
    int n = 0;
    if (read) {
        EV_SET(&kevs[n], fd, EVFILT_READ, EV_ADD, 0, 0, (void *)(intptr_t)fd);
        n++;
    }
    if (write) {
        EV_SET(&kevs[n], fd, EVFILT_WRITE, EV_ADD | EV_DISABLE, 0, 0,
               (void *)(intptr_t)fd);
        n++;
    }
    kevent(server.kqfd, kevs, n, NULL, 0, NULL);
}

static void ev_mod(int fd, bool write) {
    struct kevent kevs[1];
    EV_SET(&kevs[0], fd, EVFILT_WRITE, EV_ADD | (write ? EV_ENABLE : EV_DISABLE), 0, 0,
           (void *)(intptr_t)fd);
    kevent(server.kqfd, kevs, 1, NULL, 0, NULL);
}

static void ev_del(int fd) {
    struct kevent kevs[2];
    EV_SET(&kevs[0], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&kevs[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    kevent(server.kqfd, kevs, 2, NULL, 0, NULL);
}

typedef struct kevent ev_event_t;

static int ev_wait(ev_event_t *events, int max_events) {
    return kevent(server.kqfd, NULL, 0, events, max_events, NULL);
}

static int ev_fd(ev_event_t *e) { return (int)(intptr_t)e->udata; }
static bool ev_readable(ev_event_t *e) { return e->filter == EVFILT_READ; }
static bool ev_writable(ev_event_t *e) { return e->filter == EVFILT_WRITE; }

#else /* USE_EPOLL */

static void ev_init(void) {
    server.epfd = epoll_create1(EPOLL_CLOEXEC);
    if (server.epfd < 0) {
        perror("epoll_create1");
        exit(1);
    }
}

static void ev_add(int fd, bool read, bool write) {
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = (read ? EPOLLIN : 0) | (write ? EPOLLOUT : 0);
    epoll_ctl(server.epfd, EPOLL_CTL_ADD, fd, &ev);
}

static void ev_mod(int fd, bool write) {
    /* epoll always keeps read enabled */
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | (write ? EPOLLOUT : 0);
    epoll_ctl(server.epfd, EPOLL_CTL_MOD, fd, &ev);
}

static void ev_del(int fd) { epoll_ctl(server.epfd, EPOLL_CTL_DEL, fd, NULL); }

typedef struct epoll_event ev_event_t;

static int ev_wait(ev_event_t *events, int max_events) {
    return epoll_wait(server.epfd, events, max_events, -1);
}

static int ev_fd(ev_event_t *e) { return e->data.fd; }
static bool ev_readable(ev_event_t *e) { return e->events & EPOLLIN; }
static bool ev_writable(ev_event_t *e) { return e->events & EPOLLOUT; }

#endif

/* --- connection management --- */

static conn_t *conn_alloc(int fd, struct sockaddr *addr) {
    conn_t *c = NULL;
    if (server.free_list) {
        c = server.free_list;
        server.free_list = c->next_free;
        memset(c, 0, sizeof(*c));
    } else {
        c = calloc(1, sizeof(*c));
    }
    if (!c)
        return NULL;

    c->fd = fd;
    c->state = CS_READ_HANDSHAKE;
    c->msg_cap = 4096;
    c->msg_buf = malloc(c->msg_cap);
    c->added_time = time(NULL);

    if (addr) {
        struct sockaddr_in *sin = (struct sockaddr_in *)addr;
        snprintf(c->peer, sizeof(c->peer), "%s:%d",
                 inet_ntoa(sin->sin_addr), ntohs(sin->sin_port));
    }

    server.conns[fd] = c;
    return c;
}

static void conn_free(conn_t *c) {
    if (!c)
        return;
    if (c->stream && c->is_publisher)
        stream_release(c->stream);
    if (c->stream && !c->is_publisher) {
        /* remove from subscriber or pending list */
        stream_t *st = c->stream;
        conn_t **pp = &st->sub_head;
        while (*pp && *pp != c)
            pp = &(*pp)->next_sub;
        if (*pp)
            *pp = c->next_sub;
        pp = &st->pending_head;
        while (*pp && *pp != c)
            pp = &(*pp)->next_pending;
        if (*pp)
            *pp = c->next_pending;
    }
    while (!wq_empty(&c->wq)) {
        msg_unref(wq_peek(&c->wq));
        wq_pop(&c->wq);
    }
    free(c->msg_buf);
    c->next_free = server.free_list;
    server.free_list = c;
    server.conns[c->fd] = NULL;
}

static void conn_close(conn_t *c) {
    if (c->is_publisher)
        log_warn("closing connection", "peer", c->peer, "is_pub", "yes", NULL);
    else
        log_info("closing connection", "peer", c->peer, "is_pub", "no", NULL);
    ev_del(c->fd);
    close(c->fd);
    conn_free(c);
}

static void conn_enable_write(conn_t *c) {
    if (c->state == CS_PUBLISHER)
        return;
    ev_mod(c->fd, true);
}

static void conn_disable_write(conn_t *c) {
    if (c->state == CS_PUBLISHER)
        return;
    ev_mod(c->fd, false);
}

/* --- handshake parsing --- */

typedef struct {
    uint8_t mode;
    char path[MAX_PATH_LEN + 1];
    uint8_t session_id[SESSION_ID_LEN];
    bool has_session;
} handshake_t;

static int parse_handshake(conn_t *c, handshake_t *hs) {
    uint8_t *buf = c->hs_buf;
    int len = c->hs_len;

    if (len < 8)
        return -1; /* need more data */

    if (memcmp(buf, SMP_MAGIC, 4) != 0)
        return -2; /* bad magic */

    if (buf[4] != SMP_VERSION)
        return -3; /* bad version */

    uint8_t mode = buf[5];
    if (mode != MODE_PULL && mode != MODE_PUSH)
        return -4; /* bad mode */

    uint16_t path_len = ntohs(*(uint16_t *)(buf + 6));
    if (path_len == 0 || path_len > MAX_PATH_LEN)
        return -5; /* bad path length */

    int total_needed = 8 + path_len + SESSION_ID_LEN;
    if (len < total_needed)
        return -1; /* need more data */

    hs->mode = mode;
    memcpy(hs->path, buf + 8, path_len);
    hs->path[path_len] = '\0';
    memcpy(hs->session_id, buf + 8 + path_len, SESSION_ID_LEN);

    hs->has_session = false;
    for (int i = 0; i < SESSION_ID_LEN; i++) {
        if (hs->session_id[i] != 0) {
            hs->has_session = true;
            break;
        }
    }
    return 0;
}

static void complete_handshake(conn_t *c, handshake_t *hs) {
    uint8_t status = STATUS_BAD_HELLO;

    stream_t *st = stream_get(hs->path);
    if (!st) {
        memcpy(c->resp, SMP_MAGIC, 4);
        c->resp[4] = SMP_VERSION;
        c->resp[5] = STATUS_BAD_HELLO;
        c->state = CS_WRITE_RESPONSE;
        ev_mod(c->fd, true);  /* enable write */
        return;
    }

    c->stream = st;

    if (hs->mode == MODE_PUSH) {
        bool seamless;
        if (!stream_claim(st, hs->session_id, hs->has_session, &seamless)) {
            log_warn("push rejected: path occupied", "peer", c->peer, "path", hs->path, NULL);
            memcpy(c->resp, SMP_MAGIC, 4);
            c->resp[4] = SMP_VERSION;
            c->resp[5] = STATUS_OCCUPIED;
            c->state = CS_WRITE_RESPONSE;
            ev_mod(c->fd, true);  /* enable write */
            return;
        }
        c->is_publisher = true;
        c->seamless = seamless;
        status = STATUS_OK;
    } else {
        c->is_publisher = false;
        status = STATUS_OK;
    }

    memcpy(c->resp, SMP_MAGIC, 4);
    c->resp[4] = SMP_VERSION;
    c->resp[5] = status;
    c->state = CS_WRITE_RESPONSE;
    ev_mod(c->fd, true);  /* enable write */
}

/* --- message dispatch --- */

static void dispatch_pub_message(conn_t *c, msg_t *m) {
    stream_t *st = c->stream;

    if (m->mtype == MSG_HEADER) {
        stream_set_header(st, m);
        log_debug("header", "peer", c->peer, "path", st->path, NULL);
    } else if (m->mtype == MSG_KEY || m->mtype == MSG_PACKET) {
        if (m->mtype == MSG_KEY) {
            st->gop_len = 0;
            st->gop_bytes = 0;
            st->gop_warned = false;
        }
        if (st->gop_len > 0 || m->mtype == MSG_KEY) {
            stream_gop_append(st, m);
        }
        /* fanout happens in handle_write path */
    }
}

/* --- subscriber catch-up and fan-out --- */

static void subscriber_catchup(conn_t *c, stream_t *st) {
    if (!st->header)
        return;

    wq_push(&c->wq, st->header);
    for (int i = 0; i < st->gop_len; i++)
        wq_push(&c->wq, st->gop[i]);

    c->next_sub = st->sub_head;
    st->sub_head = c;

    conn_enable_write(c);
}

static void fanout(stream_t *st, msg_t *m) {
    msg_ref(m);

    conn_t *sub = st->sub_head;
    while (sub) {
        conn_t *next = sub->next_sub;
        if (wq_full(&sub->wq)) {
            /* slow subscriber, drop the frame (like Go version does) */
            st->dropped_frames++;
        } else {
            /* queue has space, send the message */
            msg_ref(m);
            wq_push(&sub->wq, m);
            conn_enable_write(sub);
        }
        sub = next;
    }
    msg_unref(m);
}

/* --- I/O handlers --- */

static void handle_read_handshake(conn_t *c) {
    int n = read(c->fd, c->hs_buf + c->hs_len, sizeof(c->hs_buf) - c->hs_len);
    if (n <= 0) {
        if (n < 0 && errno != EAGAIN)
            log_warn("handshake read failed", "peer", c->peer, NULL);
        if (n == 0)
            conn_close(c);
        return;
    }
    c->hs_len += n;

    handshake_t hs;
    int ret = parse_handshake(c, &hs);
    if (ret == -1)
        return; /* need more data */
    if (ret < -1) {
        log_warn("handshake parse failed", "peer", c->peer, NULL);
        c->resp[5] = STATUS_BAD_HELLO;
        c->state = CS_WRITE_RESPONSE;
        return;
    }

    log_info("client connected", "peer", c->peer, "mode",
             (hs.mode == MODE_PUSH ? "push" : "pull"),
             "path", hs.path, NULL);

    complete_handshake(c, &hs);
}

static void handle_read_publisher(conn_t *c) {
    while (1) {
        int n = read(c->fd, c->rbuf + c->rbuf_len,
                     (int)(sizeof(c->rbuf) - c->rbuf_len));
        if (n < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                log_warn("publisher read failed", "peer", c->peer, "errno", strerror(errno), NULL);
            }
            return;
        }
        if (n == 0) {
            log_warn("publisher EOF", "peer", c->peer, NULL);
            conn_close(c);
            return;
        }
        c->rbuf_len += n;

        /* accumulate messages */
        while (c->rbuf_len >= 4 && c->msg_total == 0) {
            c->msg_total = ntohl(*(uint32_t *)c->rbuf);
            if (c->msg_total == 0 || c->msg_total > MAX_MSG_LEN) {
                log_warn("bad message length", "peer", c->peer, NULL);
                conn_close(c);
                return;
            }
            if (c->msg_total > (uint32_t)c->msg_cap) {
                uint8_t *new_buf = realloc(c->msg_buf, c->msg_total);
                if (!new_buf) {
                    conn_close(c);
                    return;
                }
                c->msg_buf = new_buf;
                c->msg_cap = c->msg_total;
            }
            c->rbuf_len -= 4;
            memmove(c->rbuf, c->rbuf + 4, c->rbuf_len);
            c->msg_len = 0;
        }

        while (c->msg_total > 0 && c->msg_len < (int)c->msg_total) {
            int need = c->msg_total - c->msg_len;
            int avail = c->rbuf_len;
            int copy = (need < avail) ? need : avail;
            if (copy > 0) {
                memcpy(c->msg_buf + c->msg_len, c->rbuf, copy);
                c->msg_len += copy;
                c->rbuf_len -= copy;
                memmove(c->rbuf, c->rbuf + copy, c->rbuf_len);
            }
            if (c->msg_len == (int)c->msg_total) {
                msg_t *m = msg_new(c->msg_buf, c->msg_total);
                if (!m) {
                    conn_close(c);
                    return;
                }
                dispatch_pub_message(c, m);
                fanout(c->stream, m);
                msg_unref(m);
                c->msg_total = 0;
                c->msg_len = 0;
            } else {
                break;
            }
        }

        if (c->rbuf_len == 0)
            break;
    }
}

static void handle_write_response(conn_t *c) {
    int n = write(c->fd, c->resp + c->resp_sent, 6 - c->resp_sent);
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            log_warn("response write failed", "peer", c->peer, NULL);
        return;
    }
    c->resp_sent += n;
    if (c->resp_sent == 6) {
        if (c->resp[5] != STATUS_OK) {
            conn_close(c);
            return;
        }

        if (c->is_publisher) {
            conn_disable_write(c);
            c->state = CS_PUBLISHER;
            if (c->seamless) {
                log_info("seamless reconnect", "peer", c->peer, "path",
                         c->stream->path, NULL);
            }
        } else {
            subscriber_catchup(c, c->stream);
            c->state = CS_SUBSCRIBER;
        }
    }
}

static void handle_write_subscriber(conn_t *c) {
    /* sanity check: make sure our fd is still registered to us */
    if (c->fd < 0 || c->fd >= MAX_CONNS || server.conns[c->fd] != c) {
        log_error("subscriber connection fd corruption", "peer", c->peer, NULL);
        return;
    }

    while (!wq_empty(&c->wq) || c->wbuf_remaining > 0) {
        if (c->wbuf_remaining == 0) {
            msg_t *m = wq_peek(&c->wq);
            if (!m)
                break;
            c->wbuf_ptr = m->wire;
            c->wbuf_remaining = m->wire_len;
        }

        int n = write(c->fd, c->wbuf_ptr, c->wbuf_remaining);
        if (n < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                log_info("subscriber disconnected", "peer", c->peer, NULL);
                conn_close(c);
            }
            break;
        }
        if (n == 0) {
            log_warn("subscriber write returned 0", "peer", c->peer, NULL);
            conn_close(c);
            return;
        }

        c->wbuf_ptr += n;
        c->wbuf_remaining -= n;

        if (c->wbuf_remaining == 0) {
            msg_unref(wq_peek(&c->wq));
            wq_pop(&c->wq);
        } else {
            break;
        }
    }

    if (wq_empty(&c->wq) && c->wbuf_remaining == 0) {
        conn_disable_write(c);
        if (c->state == CS_CLOSING) {
            log_warn("closing subscriber", "peer", c->peer, NULL);
            conn_close(c);
        }
    }
}

static void handle_read(conn_t *c) {
    switch (c->state) {
    case CS_READ_HANDSHAKE:
        handle_read_handshake(c);
        break;
    case CS_PUBLISHER:
        handle_read_publisher(c);
        break;
    default:
        break;
    }
}

static void handle_write(conn_t *c) {
    switch (c->state) {
    case CS_WRITE_RESPONSE:
        handle_write_response(c);
        break;
    case CS_SUBSCRIBER:
    case CS_CLOSING:
        handle_write_subscriber(c);
        break;
    default:
        break;
    }
}

/* --- accept new connections --- */

static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        perror("fcntl F_GETFL");
        return;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        perror("fcntl F_SETFL");
}

static void accept_new(void) {
    while (1) {
        struct sockaddr_in addr;
        socklen_t alen = sizeof(addr);
        int fd = accept(server.listenfd, (struct sockaddr *)&addr, &alen);
        if (fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                log_error("accept failed", NULL);
            break;
        }

        if (fd >= MAX_CONNS) {
            log_warn("fd too large, closing", NULL);
            close(fd);
            continue;
        }

        set_nonblock(fd);
        int flag = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        conn_t *c = conn_alloc(fd, (struct sockaddr *)&addr);
        if (!c) {
            close(fd);
            continue;
        }

        ev_add(fd, true, false);
    }
}

/* --- main event loop --- */

int main(int argc, char **argv) {
    int port = 7777;
    if (argc > 1)
        port = atoi(argv[1]);

    memset(&server, 0, sizeof(server));
    memset(&streams, 0, sizeof(streams));

    server.listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (server.listenfd < 0) {
        perror("socket");
        exit(1);
    }

    int opt = 1;
    if (setsockopt(server.listenfd, SOL_SOCKET, SO_REUSEADDR, &opt,
                   sizeof(opt)) < 0) {
        perror("setsockopt");
        exit(1);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(server.listenfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(1);
    }

    if (listen(server.listenfd, 128) < 0) {
        perror("listen");
        exit(1);
    }

    signal(SIGPIPE, SIG_IGN);
    set_nonblock(server.listenfd);
    ev_init();
    ev_add(server.listenfd, true, false);

    log_info("smp listening", "port", argv[1] ? argv[1] : "7777", NULL);

    ev_event_t events[MAX_EVENTS];
    while (1) {
        int nev = ev_wait(events, MAX_EVENTS);
        if (nev < 0) {
            if (errno != EINTR)
                perror("kevent/epoll_wait");
            continue;
        }

        for (int i = 0; i < nev; i++) {
            int fd = ev_fd(&events[i]);
            if (fd == server.listenfd) {
                accept_new();
            } else {
                conn_t *c = server.conns[fd];
                if (!c)
                    continue;
                if (ev_readable(&events[i]))
                    handle_read(c);
                if (server.conns[fd] && ev_writable(&events[i]))
                    handle_write(c);
            }
        }
    }

    return 0;
}
