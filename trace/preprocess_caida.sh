# Preprocess CAIDA to only sender and size
tcpdump -q -n -t -r $1 | grep IP > $2