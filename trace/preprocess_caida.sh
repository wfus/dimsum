# Preprocess CAIDA to only sender and size
tcpdump -q -n -t -r equinix-sanjose.dirB.20121220-140000.UTC.anon.pcap | \
    grep IP > equinix-sanjose-tmp.txt 