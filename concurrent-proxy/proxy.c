/*
 * proxy.c - Concurrent Web Proxy with HTTPS and Full Keep-Alive Support
 *
 * Course Name: 14:332:456-Network Centric Programming
 * Assignment 3
 * Student Name: Saket Pabba
 */


#include "csapp.h"

#define THREAD_MODE 1
#define PROCESS_MODE 0
#define MAX_CONNECTIONS 10
#define TIMEOUT_SEC 5

pthread_mutex_t log_mutex;

void *thread_func(void *vargp);
void doit(int connfd, struct sockaddr_in *clientaddr);
void handle_connect(int clientfd, char *host, int port);
void parse_uri(char *uri, char *hostname, char *path, int *port);
void build_http_request(char *request, char *hostname, char *path, rio_t *client_rio, int *keep_alive);
void format_log_entry(char *logstring, struct sockaddr_in *sockaddr, char *uri, int size);
void synchronized_log(char *log_entry);

int main(int argc, char **argv)
{
    int listenfd, connfd, port, mode = PROCESS_MODE;
    socklen_t clientlen;
    struct sockaddr_in clientaddr;

    if (argc < 2 || argc > 3) {
        fprintf(stderr, "Usage: %s [-t | --thread] <port number>\n", argv[0]);
        exit(1);
    }

    if (argc == 3 && (strcmp(argv[1], "-t") == 0 || strcmp(argv[1], "--thread") == 0)) {
        mode = THREAD_MODE;
        port = atoi(argv[2]);
        pthread_mutex_init(&log_mutex, NULL);
        printf("Launching proxy in THREAD mode on port %d...\n", port);
    } else {
        port = atoi(argv[1]);
        printf("Launching proxy in PROCESS mode on port %d...\n", port);
    }

    listenfd = Open_listenfd(port);

    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);

        if (mode == THREAD_MODE) {
            int *connfdp = Malloc(sizeof(int));
            *connfdp = connfd;
            pthread_t tid;
            Pthread_create(&tid, NULL, thread_func, connfdp);
        } else {
            if (Fork() == 0) {
                Close(listenfd);
                doit(connfd, &clientaddr);
                Close(connfd);
                exit(0);
            }
            Close(connfd);
        }
    }
    return 0;
}

void *thread_func(void *vargp) {
    int connfd = *((int *)vargp);
    Free(vargp);
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    getpeername(connfd, (SA *)&clientaddr, &len);
    doit(connfd, &clientaddr);
    Close(connfd);
    return NULL;
}

void doit(int clientfd, struct sockaddr_in *clientaddr) {
    char buf[MAXLINE], method[MAXLINE], uri[MAXLINE], version[MAXLINE];
    char hostname[MAXLINE], path[MAXLINE], request[MAXBUF];
    int serverfd, port;
    rio_t client_rio, server_rio;

    Rio_readinitb(&client_rio, clientfd);
    struct timeval timeout = {TIMEOUT_SEC, 0};

    while (1) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(clientfd, &readfds);

        if (select(clientfd + 1, &readfds, NULL, NULL, &timeout) == 0) {
            printf("[!] Client timed out\n");
            break;
        }

        if (Rio_readlineb(&client_rio, buf, MAXLINE) <= 0)
            break;

        sscanf(buf, "%s %s %s", method, uri, version);

        if (!strcasecmp(method, "CONNECT")) {
            char host[MAXLINE];
            sscanf(uri, "%[^:]:%d", host, &port);
            handle_connect(clientfd, host, port);
            return;
        }

        if (strcasecmp(method, "GET")) {
            printf("[!] Unsupported method: %s\n", method);
            return;
        }

        parse_uri(uri, hostname, path, &port);

        int keep_alive = 0;
        build_http_request(request, hostname, path, &client_rio, &keep_alive);

        serverfd = Open_clientfd(hostname, port);
        Rio_readinitb(&server_rio, serverfd);
        Rio_writen(serverfd, request, strlen(request));

        int total_size = 0, content_length = -1, chunked = 0;

        // Read and forward HTTP response headers
        while (Rio_readlineb(&server_rio, buf, MAXLINE) > 0) {
            Rio_writen(clientfd, buf, strlen(buf));
            total_size += strlen(buf);

            if (!strncasecmp(buf, "Content-Length:", 15)) {
                sscanf(buf + 15, "%d", &content_length);
            } else if (strstr(buf, "Transfer-Encoding: chunked")) {
                chunked = 1;
            } else if (strstr(buf, "Connection: close") || strstr(buf, "connection: close")) {
                keep_alive = 0;
            }

            if (!strcmp(buf, "\r\n")) break;
        }

        if (chunked) {
            while (Rio_readlineb(&server_rio, buf, MAXLINE) > 0) {
                Rio_writen(clientfd, buf, strlen(buf));
                total_size += strlen(buf);
                if (!strcmp(buf, "0\r\n")) break;
            }
            // Read trailing headers
            while (Rio_readlineb(&server_rio, buf, MAXLINE) > 0) {
                Rio_writen(clientfd, buf, strlen(buf));
                total_size += strlen(buf);
                if (!strcmp(buf, "\r\n")) break;
            }
        } else if (content_length > 0) {
            int nleft = content_length;
            while (nleft > 0) {
                int n = Rio_readnb(&server_rio, buf, MAXLINE < nleft ? MAXLINE : nleft);
                if (n <= 0) break;
                Rio_writen(clientfd, buf, n);
                total_size += n;
                nleft -= n;
            }
        } else {
            while ((serverfd > 0) && (Rio_readnb(&server_rio, buf, MAXLINE) > 0)) {
                int n = Rio_readnb(&server_rio, buf, MAXLINE);
                if (n <= 0) break;
                Rio_writen(clientfd, buf, n);
                total_size += n;
            }
        }

        char log_entry[MAXLINE];
        format_log_entry(log_entry, clientaddr, uri, total_size);
        synchronized_log(log_entry);

        Close(serverfd);

        if (!keep_alive)
            break;
    }
}

void handle_connect(int clientfd, char *host, int port) {
    int serverfd = Open_clientfd(host, port);
    if (serverfd < 0) return;
    Rio_writen(clientfd, "HTTP/1.1 200 Connection Established\r\n\r\n", 39);

    fd_set read_set;
    int maxfd = (clientfd > serverfd) ? clientfd : serverfd;
    char buf[MAXBUF];
    int n;

    while (1) {
        FD_ZERO(&read_set);
        FD_SET(clientfd, &read_set);
        FD_SET(serverfd, &read_set);

        if (Select(maxfd + 1, &read_set, NULL, NULL, NULL) < 0) break;

        if (FD_ISSET(clientfd, &read_set)) {
            n = Read(clientfd, buf, MAXBUF);
            if (n <= 0) break;
            Rio_writen(serverfd, buf, n);
        }

        if (FD_ISSET(serverfd, &read_set)) {
            n = Read(serverfd, buf, MAXBUF);
            if (n <= 0) break;
            Rio_writen(clientfd, buf, n);
        }
    }
    Close(serverfd);
}

void parse_uri(char *uri, char *hostname, char *path, int *port) {
    *port = 80;
    char *hostbegin = strstr(uri, "//") ? strstr(uri, "//") + 2 : uri;
    char *pathbegin = strchr(hostbegin, '/');
    if (pathbegin) {
        strcpy(path, pathbegin);
        *pathbegin = '\0';
    } else {
        strcpy(path, "/");
    }

    char *portpos = strchr(hostbegin, ':');
    if (portpos) {
        *portpos = '\0';
        sscanf(portpos + 1, "%d", port);
    }

    strcpy(hostname, hostbegin);
}

void build_http_request(char *request, char *hostname, char *path, rio_t *client_rio, int *keep_alive) {
    char buf[MAXLINE], other_hdrs[MAXBUF] = "";
    sprintf(request, "GET %s HTTP/1.1\r\n", path);
    sprintf(other_hdrs, "Host: %s\r\n", hostname);

    while (Rio_readlineb(client_rio, buf, MAXLINE) > 0) {
        if (!strcmp(buf, "\r\n")) break;

        if (!strncasecmp(buf, "Connection:", 11)) {
            if (strstr(buf, "keep-alive"))
                *keep_alive = 1;
            continue;
        }
        if (!strncasecmp(buf, "Proxy-Connection:", 17)) {
            continue;
        }
        if (strncasecmp(buf, "Host:", 5)) {
            strcat(other_hdrs, buf);
        }
    }

    strcat(other_hdrs, *keep_alive ? "Connection: keep-alive\r\nProxy-Connection: keep-alive\r\n"
                                   : "Connection: close\r\nProxy-Connection: close\r\n");
    strcat(request, other_hdrs);
    strcat(request, "\r\n");
}

void format_log_entry(char *logstring, struct sockaddr_in *sockaddr,
                      char *uri, int size) {
    time_t now;
    char time_str[MAXLINE];
    unsigned long host;
    unsigned char a, b, c, d;

    now = time(NULL);
    strftime(time_str, MAXLINE, "%a %d %b %Y %H:%M:%S %Z", localtime(&now));
    host = ntohl(sockaddr->sin_addr.s_addr);
    a = host >> 24; b = (host >> 16) & 0xff;
    c = (host >> 8) & 0xff; d = host & 0xff;
    sprintf(logstring, "%s: %d.%d.%d.%d %s %d\n", time_str, a, b, c, d, uri, size);
}

void synchronized_log(char *log_entry) {
    pthread_mutex_lock(&log_mutex);
    FILE *logfile = Fopen("proxy.log", "a");
    Fputs(log_entry, logfile);
    Fclose(logfile);
    pthread_mutex_unlock(&log_mutex);
}

