"""Postprocess traces from IP format to SENDER and PACKET LEN."""
MAX_INT = 2147483647
# Our algorithm takes 32 bit ints
with open('equinix-sanjose-tmp.txt', 'r') as f:
    with open('equinix-sanjose.dmp', 'w') as outf:
        for line in f.readlines():
            try:
                stripped = line.strip().split(' ')
                sender_ip = int(stripped[1].replace('.', '')) % MAX_INT
                packet_len = int(stripped[-1]) % MAX_INT
                outf.write("%d %d\n" % (sender_ip, packet_len))
            except Exception as read_err:
                print(str(read_err))
