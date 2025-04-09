#include <cassert>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>

#include <argparse/argparse.hpp>

const int PORT = 8000;

enum Command
{
    CMD_INIT,
    CMD_START,
    CMD_END,
};

void receiver(int sock, std::atomic_flag& is_running, std::atomic<size_t>& n_total_received)
{
    std::vector<uint8_t> buf(1024 * 1024);
    size_t n_received = 0;

    while (is_running.test()) {
        uint8_t *ptr = buf.data();
        ssize_t len = buf.size();

        do {
            ssize_t received = read(sock, ptr, len);
            ptr += received;
            len -= received;
            n_received += received;
        } while(len > 0 && is_running.test());
    }

    n_total_received += n_received;

    close(sock);
}

int server()
{
    struct sockaddr_in server_addr, client_addr;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(PORT);
    server_addr.sin_family      = AF_INET;

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        std::cerr << "socket() failed" << std::endl;
        return -1;
    }
    if (bind(server_sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "bind() failed" << std::endl;
        return -1;
    }
    listen(server_sock, SOMAXCONN);

    while (true) {
        socklen_t addr_len = sizeof(client_addr);
        int control_sock = accept(server_sock, (sockaddr *)&client_addr, &addr_len);
        if (control_sock < 0) {
            std::cerr << "accept() failed" << std::endl;
            return -1;
        }

        uint8_t buf[2];
        read(control_sock, buf, 2);
        assert(buf[0] == CMD_INIT);
        int num_streams = buf[1];

        std::cout << "Opening " << num_streams << " streams" << std::endl;

        std::atomic_flag is_running = ATOMIC_FLAG_INIT;
        std::atomic<size_t> n_total_received(0);
        is_running.test_and_set();

        std::vector<std::thread> threads;
        for (int i = 0; i < num_streams; i++) {
            socklen_t addr_len = sizeof(client_addr);
            int sock = accept(server_sock, (sockaddr *)&client_addr, &addr_len);
            if (sock < 0) {
                std::cerr << "accept() failed" << std::endl;
                return -1;
            }

            char host[NI_MAXHOST];
            getnameinfo((sockaddr *)&client_addr, addr_len, host, sizeof(host), nullptr, 0, NI_NUMERICHOST);

            std::cout << "Connected to client at " << host << ":" << client_addr.sin_port << std::endl;

            threads.emplace_back(receiver, sock, std::ref(is_running), std::ref(n_total_received));
        }

        std::cout << "All streams established" << std::endl;

        buf[0] = CMD_START;
        write(control_sock, buf, 1);

        read(control_sock, buf, 1);
        assert(buf[0] == CMD_END);

        is_running.clear();
        for (std::thread &thread : threads) {
            thread.join();
        }

        std::cout << static_cast<double>(n_total_received) / (1000 * 1000 * 1000) << " GB received" << std::endl;

        close(control_sock);
    }

    close(server_sock);

    return 0;
}

void sender(int sock, std::atomic_flag& is_running, std::atomic<size_t> &n_total_sent)
{
    std::vector<uint8_t> buf(1024 * 1024);

    while (is_running.test()) {
        const uint8_t *ptr = buf.data();
        ssize_t len = buf.size();

        do {
            ssize_t sent = write(sock, ptr, len);
            ptr += sent;
            len -= sent;
            n_total_sent += sent;
        } while(len > 0 && is_running.test());
    }

    close(sock);
}

int client(const std::string& server, int num_streams, int duration)
{
    struct addrinfo hints = { 0 }, *server_address;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if(getaddrinfo(server.c_str(), std::to_string(PORT).c_str(), &hints, & server_address) < 0){
        std::cerr << "getaddrinfo() failed " << strerror(errno) << std::endl;
        return -1;
    }

    int control_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (control_sock < 0) {
        std::cerr << "socket() failed: " << strerror(errno) << std::endl;
        return -1;
    }

    if(connect(control_sock, server_address->ai_addr, server_address->ai_addrlen) < 0){
        std::cerr << "connect() failed: " << strerror(errno) << std::endl;
        close(control_sock);
        freeaddrinfo(server_address);
        return -1;
    }

    uint8_t buf[2];
    buf[0] = CMD_INIT;
    buf[1] = num_streams;
    write(control_sock, buf, 2);

    std::atomic_flag is_running = ATOMIC_FLAG_INIT;
    is_running.test_and_set();
    std::atomic<size_t> n_total_sent(0);

    std::cout << "Opening " << num_streams << " streams" << std::endl;

    std::vector<std::thread> threads;
    for (int i = 0; i < num_streams; i++) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "socket() failed: " << strerror(errno) << std::endl;
            return -1;
        }
        if(connect(sock, server_address->ai_addr, server_address->ai_addrlen) < 0){
            std::cerr << "connect() failed: " << strerror(errno) << std::endl;
            return -1;
        }
        threads.emplace_back(sender, sock, std::ref(is_running), std::ref(n_total_sent));
    }

    std::cout << "All streams established" << std::endl;
    std::cout << "Test wil run for " << duration << " seconds" << std::endl;

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < duration; i++) {
        std::cout << "Running..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    is_running.clear();

    auto end = std::chrono::steady_clock::now();

    std::cout << "Shutting down all senders" << std::endl;

    for (std::thread &thread : threads) {
        thread.join();
    }

    std::cout << "Shut down all senders" << std::endl;

    buf[0] = CMD_END;
    write(control_sock, buf, 1);

    auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    std::cout << static_cast<double>(n_total_sent) / (1000 * 1000 * 1000) << " GB sent" << std::endl;
    std::cout << static_cast<double>(n_total_sent) / elapsed_ns.count() * 8 << " Gbps" << std::endl;

    return 0;
}

int main(int argc, char *argv[])
{
    argparse::ArgumentParser program("flood");

    auto &group = program.add_mutually_exclusive_group(true);
    group.add_argument("-s", "--server")
        .help("Launch server")
        .flag();
    group.add_argument("-c", "--client")
        .help("Launch client and connect to server");
    program.add_argument("-P", "--parallel")
        .default_value(1)
        .help("Number of parallel streams")
        .scan<'i', int>();
    program.add_argument("-t", "--time")
        .default_value(10)
        .help("Test duration in seconds")
        .scan<'i', int>();

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    bool is_server = program.is_used("--server");
    bool is_client = program.is_used("--client");

    if (program.is_used("--server")) {
        return server();
    } else if (program.is_used("--client")) {
        return client(program.get<std::string>("--client"), program.get<int>("--parallel"),
                      program.get<int>("--time"));
    }

    return 0;
}
