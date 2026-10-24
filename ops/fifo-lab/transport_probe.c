/* Isolated sender only. Observe libc/GnuTLS boundaries without payload logging. */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static _Atomic int tracked_fd = -1;
static _Atomic int serial;
static _Atomic int tcp_done;
static _Atomic int tls_started;
static _Atomic int tls_done;
static _Atomic int error_done;
static _Atomic int publish_done;
static unsigned char tail[64];
static size_t tail_len;

static double monotonic_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

static void event_at(const char *stage, int value, double at)
{
    int saved = errno;
    char out[256];
    int len = snprintf(out, sizeof(out),
        "TRANSPORT_PROBE {\"stage\":\"%s\",\"mono\":%.9f,\"pid\":%d,"
        "\"connection\":%d,\"fd\":%d,\"value\":%d}\n",
        stage, at, (int)getpid(), atomic_load(&serial), atomic_load(&tracked_fd), value);
    if (len > 0 && (size_t)len < sizeof(out)) {
        ssize_t emitted = write(STDERR_FILENO, out, (size_t)len);
        if (emitted != len) {
            errno = saved;
            return;
        }
    }
    errno = saved;
}

static int is_target(const struct sockaddr *addr, socklen_t len)
{
    if (!addr || len < sizeof(struct sockaddr_in) || addr->sa_family != AF_INET)
        return 0;
    const struct sockaddr_in *a = (const struct sockaddr_in *)addr;
    return ntohs(a->sin_port) == 1936 && ntohl(a->sin_addr.s_addr) == 0x0afa2a01U;
}

int connect(int fd, const struct sockaddr *addr, socklen_t len)
{
    static int (*real_connect)(int, const struct sockaddr *, socklen_t);
    if (!real_connect) real_connect = dlsym(RTLD_NEXT, "connect");
    int target = is_target(addr, len);
    if (target) {
        atomic_store(&tracked_fd, fd);
        atomic_fetch_add(&serial, 1);
        atomic_store(&tcp_done, 0);
        atomic_store(&tls_started, 0);
        atomic_store(&tls_done, 0);
        atomic_store(&error_done, 0);
        atomic_store(&publish_done, 0);
        tail_len = 0;
        event_at("tcp_begin", 0, monotonic_now());
    }
    int ret = real_connect(fd, addr, len);
    int saved = errno;
    double at = monotonic_now();
    if (target && ret == 0 && !atomic_exchange(&tcp_done, 1))
        event_at("tcp_complete", 0, at);
    else if (target && ret < 0 && saved != EINPROGRESS && saved != EINTR)
        event_at("tcp_failure", saved, at);
    errno = saved;
    return ret;
}

int getsockopt(int fd, int level, int opt, void *val, socklen_t *len)
{
    static int (*real_getsockopt)(int, int, int, void *, socklen_t *);
    if (!real_getsockopt) real_getsockopt = dlsym(RTLD_NEXT, "getsockopt");
    int ret = real_getsockopt(fd, level, opt, val, len);
    int saved = errno;
    double at = monotonic_now();
    if (ret == 0 && fd == atomic_load(&tracked_fd) && level == SOL_SOCKET &&
        opt == SO_ERROR && val && len && *len >= sizeof(int)) {
        if (*(int *)val == 0 && !atomic_load(&tcp_done)) {
            struct sockaddr_storage peer;
            socklen_t size = sizeof(peer);
            if (getpeername(fd, (struct sockaddr *)&peer, &size) == 0 &&
                is_target((struct sockaddr *)&peer, size) && !atomic_exchange(&tcp_done, 1))
                event_at("tcp_complete", 0, at);
        } else if (*(int *)val != 0) {
            event_at("tcp_failure", *(int *)val, at);
        }
    }
    errno = saved;
    return ret;
}

int close(int fd)
{
    static int (*real_close)(int);
    if (!real_close) real_close = dlsym(RTLD_NEXT, "close");
    if (fd >= 0 && fd == atomic_load(&tracked_fd)) {
        event_at("socket_close_begin", atomic_load(&tcp_done), monotonic_now());
        int expected = fd;
        atomic_compare_exchange_strong(&tracked_fd, &expected, -1);
    }
    return real_close(fd);
}

int gnutls_handshake(void *session)
{
    static int (*real_handshake)(void *);
    static int (*is_resumed)(void *);
    if (!real_handshake) real_handshake = dlsym(RTLD_NEXT, "gnutls_handshake");
    if (!is_resumed) is_resumed = dlsym(RTLD_NEXT, "gnutls_session_is_resumed");
    if (!atomic_exchange(&tls_started, 1))
        event_at("tls_begin", 0, monotonic_now());
    int ret = real_handshake(session);
    int saved = errno;
    double at = monotonic_now();
    if (ret == 0 && !atomic_exchange(&tls_done, 1))
        event_at("tls_complete", is_resumed(session), at);
    errno = saved;
    return ret;
}

unsigned int gnutls_x509_crt_check_hostname(void *cert, const char *hostname)
{
    static unsigned int (*real_check)(void *, const char *);
    if (!real_check) real_check = dlsym(RTLD_NEXT, "gnutls_x509_crt_check_hostname");
    unsigned int ret = real_check(cert, hostname);
    int saved = errno;
    if (ret && atomic_load(&tls_done))
        event_at("tls_verified", 0, monotonic_now());
    errno = saved;
    return ret;
}

static void observe_error(ssize_t ret, double at)
{
    /* GnuTLS AGAIN=-28 / INTERRUPTED=-52 are ordinary nonblocking progress. */
    if (ret < 0 && ret != -28 && ret != -52 && !atomic_exchange(&error_done, 1))
        event_at("transport_error", (int)ret, at);
}

ssize_t gnutls_record_send(void *session, const void *data, size_t size)
{
    static ssize_t (*real_send)(void *, const void *, size_t);
    if (!real_send) real_send = dlsym(RTLD_NEXT, "gnutls_record_send");
    ssize_t ret = real_send(session, data, size);
    int saved = errno;
    observe_error(ret, monotonic_now());
    errno = saved;
    return ret;
}

ssize_t gnutls_record_recv(void *session, void *data, size_t size)
{
    static ssize_t (*real_recv)(void *, void *, size_t);
    static const char token[] = "NetStream.Publish.Start";
    if (!real_recv) real_recv = dlsym(RTLD_NEXT, "gnutls_record_recv");
    ssize_t ret = real_recv(session, data, size);
    int saved = errno;
    double at = monotonic_now();
    observe_error(ret, at);
    if (ret > 0 && atomic_load(&tls_done) && !atomic_load(&publish_done)) {
        const unsigned char *p = data;
        for (ssize_t i = 0; i < ret; i++) {
            if (tail_len == sizeof(tail)) {
                memmove(tail, tail + 1, sizeof(tail) - 1);
                tail_len--;
            }
            tail[tail_len++] = p[i];
            if (tail_len >= sizeof(token) - 1 &&
                !memcmp(tail + tail_len - (sizeof(token) - 1), token, sizeof(token) - 1)) {
                atomic_store(&publish_done, 1);
                event_at("rtmps_publish_ack", 0, at);
                break;
            }
        }
    }
    errno = saved;
    return ret;
}
