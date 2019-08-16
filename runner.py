import argparse
import time
import struct
import random
import signal
import sys
import traceback
import string

from ring_buffer.ring_buffer import RingBuffer


def main():
    parser = argparse.ArgumentParser(
        description="Shared Memory Ring Buffer producer/consumer example"
    )
    parser.add_argument(
        "mode", choices=["producer", "consumer"], help="Run in producer/consumer mode"
    )
    parser.add_argument("name", help="Name of shared memory region")
    parser.add_argument(
        "size",
        type=int,
        help="Size of the shared memory region. Only required if runnin in producer mode",
    )
    parser.add_argument(
        "delay",
        type=int,
        help="Seconds to wait before consuming/producing next message",
    )

    args = parser.parse_args()
    if args.mode == "producer":
        run_producer(args.name, args.size, args.delay)
    else:
        run_consumer(args.name, args.delay)


def run_producer(name, size, delay):
    buffer = RingBuffer(name=name, size=size, create=True)
    shutdown = False

    def terminate(signum, frame):
        nonlocal shutdown
        print(
            "Received=",
            signal.Signals(signum).name,
            "Attempting graceful shutdown of producer",
        )
        shutdown = True

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)

    try:
        while not shutdown:
            buffer.put(get_next_message())
            print(f"Reader: {buffer.reader_pos()} Writer: {buffer.writer_pos()}")
            time.sleep(delay)

        buffer.destroy()
        print(f"Destroyed shared memory buffer {name}")

    except Exception as ex:
        buffer.destroy()
        print(f"Error {str(ex)}. Destroying shared memory buffer and exiting")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


def run_consumer(name, delay):
    buffer = RingBuffer(name=name, size=0, create=False)
    shutdown = False

    def terminate(signum, frame):
        nonlocal shutdown
        print(
            "Received=",
            signal.Signals(signum).name,
            "Attempting graceful shutdown of consumer",
        )
        shutdown = True

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)

    try:
        while not shutdown:
            b = buffer.get()
            print(f"Message: {decode_message(b)}Reader: {buffer.reader_pos()} Writer: {buffer.writer_pos()}")
            time.sleep(delay)

        buffer.close()
        print(f"Destroyed shared memory buffer {name}")

    except Exception as ex:
        buffer.close()
        print(f"Error {str(ex)}. Destroying shared memory buffer and exiting")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


def get_next_message():
    symbol = "".join([random.choice(string.ascii_letters) for _ in range(4)]).encode()
    price = round(random.uniform(100, 115), 2)
    quantity = random.randint(10, 100)
    return struct.pack("<4sdi", symbol, price, quantity)

def decode_message(buff):
    return struct.unpack_from("<4sdi", buff, 0)


if __name__ == "__main__":
    main()
