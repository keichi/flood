#!/usr/bin/env python3

import argparse
import socket
import struct
import tempfile
import time
import multiprocessing


PORT = 8000
DURATION = 10

CMD_INIT = 0
CMD_START = 1
CMD_END = 2


def receiver(sock, is_running, n_received_total):
    print(f"Connected receiver local: {sock.getsockname()} remote: {sock.getpeername()}")

    buf = bytearray(4 * 1024 * 1024)
    n_received = 0

    while is_running.value:
        n_received += sock.recv_into(buf)

    with n_received_total.get_lock():
        n_received_total.value += n_received


def server(args):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind((socket.gethostname(), PORT))
    server_sock.listen()

    print(f"Server started listening at {server_sock.getsockname()}")

    while True:
        control_sock, address = server_sock.accept()

        cmd, num_streams  = struct.unpack(">bh", control_sock.recv(3))
        assert(cmd == CMD_INIT)

        procs = []
        is_running = multiprocessing.Value("i", 1)
        n_received_total = multiprocessing.Value("l", 0)

        for _ in range(num_streams):
            sock, address = server_sock.accept()
            proc = multiprocessing.Process(target=receiver, args=(sock, is_running, n_received_total))
            proc.start()
            procs.append(proc)

        control_sock.send(struct.pack(">b", CMD_START))

        cmd, = struct.unpack(">b", control_sock.recv(1))
        print("Test finished, shutting down receivers")

        assert(cmd == CMD_END)
        is_running.value = 0

        for proc in procs:
            proc.join()

        print(f"Total bytes received: {n_received_total.value / 1000 / 1000 / 1000:.3f} GB")


def sender(sock, is_running, n_sent_total):
    n_sent = 0

    tmp = tempfile.TemporaryFile()
    tmp.truncate(10 * 1024 * 1024)

    while not is_running.value:
        time.sleep(0.01)

    while is_running.value:
        tmp.seek(0)
        sock.sendfile(tmp)
        n_sent += tmp.tell()

    tmp.close()

    with n_sent_total.get_lock():
        n_sent_total.value += n_sent


def client(args):
    control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    control_sock.connect((args.client, PORT))
    control_sock.sendall(struct.pack(">bh", CMD_INIT, args.parallel))

    procs = []
    is_running = multiprocessing.Value("i", 1)
    n_sent_total = multiprocessing.Value("l", 0)

    for _ in range(args.parallel):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((args.client, PORT))
        print(f"Connected sender local: {sock.getsockname()} remote: {sock.getpeername()}")

        proc = multiprocessing.Process(target=sender, args=(sock, is_running, n_sent_total))
        proc.start()
        procs.append(proc)

    cmd, = struct.unpack(">b", control_sock.recv(1))
    assert(cmd == CMD_START)

    print("All streams established, starting measurement")

    start_time = time.monotonic()

    while time.monotonic() < start_time + DURATION:
        print("Running...")
        time.sleep(1)

    is_running.value = 0
    end_time = time.monotonic()

    for proc in procs:
        proc.join()

    control_sock.sendall(struct.pack(">b", CMD_END))

    print(f"Total bytes sent: {n_sent_total.value / 1000 / 1000 / 1000:.3f} GB")

    bw = n_sent_total.value / (end_time - start_time)
    print(f"Effective throughput: {bw / 1000 / 1000 / 1000 * 8:.3f} Gbps")


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--server", action="store_true")
    group.add_argument("-c", "--client")
    parser.add_argument("-P", "--parallel", type=int, default=1)
    args = parser.parse_args()

    if args.server:
        server(args)
    elif args.client:
        client(args)


if __name__ == "__main__":
    main()
