// Child-process-only resolver for the Linux integration test. Preserve the TLS name.
// Rust may assign the requested service port after DNS, so redirect it at connect too.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

int getaddrinfo(const char *name, const char *service, const struct addrinfo *hints,
                struct addrinfo **result) {
    int (*real_lookup)(const char *, const char *, const struct addrinfo *, struct addrinfo **) =
        dlsym(RTLD_NEXT, "getaddrinfo");
    if (!name || (strcmp(name, getenv("TW_TEST_REST")) && strcmp(name, getenv("TW_TEST_WS"))))
        return EAI_NONAME;
    return real_lookup("127.0.0.1", service, hints, result);
}

int connect(int fd, const struct sockaddr *address, socklen_t len) {
    int (*real_connect)(int, const struct sockaddr *, socklen_t) = dlsym(RTLD_NEXT, "connect");
    if (address->sa_family == AF_UNIX) return real_connect(fd, address, len);
    if (address->sa_family == AF_INET && len >= sizeof(struct sockaddr_in)) {
        struct sockaddr_in local;
        memcpy(&local,address,sizeof(local));
        if (local.sin_addr.s_addr == htonl(INADDR_LOOPBACK) &&
            (ntohs(local.sin_port) == 443 || ntohs(local.sin_port) == 8443)) {
            local.sin_port = htons(atoi(getenv("TW_TEST_PORT")));
            return real_connect(fd,(const struct sockaddr *)&local,sizeof(local));
        }
    }
    errno = EACCES;
    return -1;
}
