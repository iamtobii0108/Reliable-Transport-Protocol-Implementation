import sys
import socket
from utils import PacketHeader, compute_checksum

TYPE_START = 0
TYPE_END = 1
TYPE_DATA = 2
TYPE_ACK = 3

MAX_PACKET_SIZE = 1472
HEADER_SIZE = 16


def send_ack(sock, addr, seq_num):
    header = PacketHeader(type=TYPE_ACK, seq_num=seq_num, length=0, checksum=0)
    header.checksum = compute_checksum(header)
    sock.sendto(bytes(header), addr)


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    in_connection = False
    sender_addr = None
    expected_seq = 1
    buffer = {}
    delivered_chunks = []

    while True:
        pkt, addr = s.recvfrom(MAX_PACKET_SIZE)
        if len(pkt) < HEADER_SIZE:
            continue

        pkt_header = PacketHeader(pkt[:HEADER_SIZE])
        msg = pkt[HEADER_SIZE : HEADER_SIZE + pkt_header.length]

        pkt_checksum = pkt_header.checksum
        pkt_header.checksum = 0
        if compute_checksum(pkt_header / msg) != pkt_checksum:
            continue

        if pkt_header.type == TYPE_START:
            if not in_connection:
                in_connection = True
                sender_addr = addr
                expected_seq = 1
                send_ack(s, addr, 1)
            elif addr == sender_addr and expected_seq == 1:
                send_ack(s, addr, 1)
            continue

        if not in_connection or addr != sender_addr:
            continue

        if pkt_header.type == TYPE_DATA:
            seq = pkt_header.seq_num
            if seq >= expected_seq + window_size:
                continue

            if seq == expected_seq:
                buffer[seq] = msg
                while expected_seq in buffer:
                    delivered_chunks.append(buffer.pop(expected_seq))
                    expected_seq += 1
                send_ack(s, addr, expected_seq)
            elif seq > expected_seq:
                if seq not in buffer:
                    buffer[seq] = msg
                send_ack(s, addr, expected_seq)
            else:
                send_ack(s, addr, expected_seq)

        elif pkt_header.type == TYPE_END:
            if pkt_header.seq_num == expected_seq:
                send_ack(s, addr, expected_seq + 1)
                sys.stdout.buffer.write(b"".join(delivered_chunks))
                sys.stdout.buffer.flush()
                break

    s.close()


def main():
    if len(sys.argv) != 4:
        sys.exit("Usage: python receiver.py [Receiver IP] [Receiver Port] [Window Size]")
    receiver_ip = sys.argv[1]
    receiver_port = int(sys.argv[2])
    window_size = int(sys.argv[3])
    receiver(receiver_ip, receiver_port, window_size)


if __name__ == "__main__":
    main()