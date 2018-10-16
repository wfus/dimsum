from matplotlib import pyplot as plt
import torch
import torch.distributions as ds

# Sample synthetic data from a heavitailed distribution for the sizes
# Sample random ids from 1 to 1000 for the packet senders
N_SAMPLES = 100000000
N_IDS = 10000
FNAME = 'trace/generated_data_1e5_1e9'
size_dist = ds.LogNormal(torch.tensor([0.0]), torch.tensor([2.0]))
id_dist = ds.Uniform(torch.tensor([0]).float(), torch.tensor([N_IDS]).float())

sizes = []
ids = []

for index in range(N_SAMPLES):
    if index % 1000000 == 0:
        print(index)
    packet_size = int(size_dist.sample().long().numpy()[0] + 1)
    packet_id = int(id_dist.sample().long().numpy()[0])
    sizes.append(packet_size)
    ids.append(packet_id)

arr = [str(a) + " " + str(b) + "\n" for a, b in zip(sizes, ids)]
with open(FNAME, 'w') as f:
    f.write("".join(arr))