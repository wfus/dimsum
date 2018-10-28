"""Postprocess traces from IP format to SENDER and PACKET LEN."""
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', required=True, type=str,
                    help="Path to original pcap dump stripped with rows of IP")
parser.add_argument('-o', '--output', default='dump.dmp', type=str,
                    help="Output dump file with sender IDs and packet lengths")
args = parser.parse_args()


MAX_INT = 1048575  # defined in u32DomainSize in the old code for some reason...
# Our algorithm takes 32 bit ints so we want to use some sort of cutoff
with open(args.file, 'r') as f:
    with open(args.output, 'w') as outf:
        for line in f.readlines():
            try:
                stripped = line.strip().split(' ')
                sender_ip = int(stripped[1].replace('.', '')) % MAX_INT
                packet_len = int(stripped[-1]) % MAX_INT
                outf.write("%d %d\n" % (sender_ip, packet_len))
            except Exception as read_err:
                print(str(read_err))
