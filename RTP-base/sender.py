import sys
import socket
import time
from utils import PacketHeader, compute_checksum

TYPE_START = 0
TYPE_END = 1
TYPE_DATA = 2
TYPE_ACK = 3

MAX_PACKET_SIZE = 1472
HEADER_SIZE = 16
MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE  # 1456 bytes
TIMEOUT = 0.5  # 500ms


def make_packet(pkt_type, seq_num, data=b""):
    if isinstance(data, str):
        data = data.encode('latin1')
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(data), checksum=0)
    pkt = header / data
    header.checksum = compute_checksum(pkt)
    return bytes(header / data)


def parse_ack(raw_bytes):
    if len(raw_bytes) < HEADER_SIZE:
        return None
    header = PacketHeader(raw_bytes[:HEADER_SIZE])
    received_checksum = header.checksum
    header.checksum = 0
    if compute_checksum(header) != received_checksum:
        return None
    if header.type != TYPE_ACK:
        return None
    return header.seq_num


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    addr = (receiver_ip, receiver_port)

    # 1. Đọc toàn bộ dữ liệu từ stdin và cắt thành từng mảnh 1456 bytes
    raw_message = sys.stdin.buffer.read()
    chunks = [
        raw_message[i : i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(raw_message), MAX_PAYLOAD_SIZE)
    ]
    num_packets = len(chunks)

    data_packets = {}
    for idx, chunk in enumerate(chunks):
        seq = idx + 1
        data_packets[seq] = make_packet(TYPE_DATA, seq, chunk)

    # 2. Gửi gói START (seq_num = 0) và chờ ACK(1)
    start_pkt = make_packet(TYPE_START, 0)
    while True:
        s.sendto(start_pkt, addr)
        start_wait = time.time()
        ack_received = False
        while time.time() - start_wait < TIMEOUT:
            remaining = TIMEOUT - (time.time() - start_wait)
            if remaining <= 0:
                break
            s.settimeout(remaining)
            try:
                data, _ = s.recvfrom(MAX_PACKET_SIZE)
                ack_seq = parse_ack(data)
                if ack_seq is not None and ack_seq >= 1:
                    ack_received = True
                    break
            except socket.timeout:
                break
        if ack_received:
            break

    # 3. Truyền các gói DATA bằng cửa sổ trượt (Cumulative ACK)
    base = 1
    next_to_send = 1
    timer_start = time.time()

    while base <= num_packets:
        window_end = min(base + window_size - 1, num_packets)
        while next_to_send <= window_end:
            s.sendto(data_packets[next_to_send], addr)
            next_to_send += 1

        elapsed = time.time() - timer_start
        if elapsed >= TIMEOUT:
            for seq in range(base, window_end + 1):
                s.sendto(data_packets[seq], addr)
            timer_start = time.time()
            continue

        s.settimeout(TIMEOUT - elapsed)
        try:
            data, _ = s.recvfrom(MAX_PACKET_SIZE)
            ack_seq = parse_ack(data)
            if ack_seq is not None and ack_seq > base:
                base = ack_seq
                timer_start = time.time()
        except socket.timeout:
            window_end = min(base + window_size - 1, num_packets)
            for seq in range(base, window_end + 1):
                s.sendto(data_packets[seq], addr)
            timer_start = time.time()

    # 4. Gửi gói END và chờ tối đa 500ms
    end_seq = num_packets + 1
    end_pkt = make_packet(TYPE_END, end_seq)
    s.sendto(end_pkt, addr)
    end_timer = time.time()

    while time.time() - end_timer < TIMEOUT:
        remaining = TIMEOUT - (time.time() - end_timer)
        if remaining <= 0:
            break
        s.settimeout(remaining)
        try:
            data, _ = s.recvfrom(MAX_PACKET_SIZE)
            ack_seq = parse_ack(data)
            if ack_seq is not None and ack_seq >= end_seq + 1:
                break
        except socket.timeout:
            break

    s.close()


def main():
    if len(sys.argv) != 4:
        sys.exit("Usage: python sender.py [Receiver IP] [Receiver Port] [Window Size]")
    receiver_ip = sys.argv[1]
    receiver_port = int(sys.argv[2])
    window_size = int(sys.argv[3])
    sender(receiver_ip, receiver_port, window_size)


if __name__ == "__main__":
    main()