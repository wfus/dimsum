from matplotlib import pyplot as plt
import torch
import torch.distributions as ds

# Sample synthetic data from a heavitailed distribution for the sizes
# Sample random ids from 1 to 1000 for the packet senders
N_SAMPLES = 10000000
size_dist = ds.LogNormal(torch.tensor([0.0]), torch.tensor([2.0]))
id_dist = ds.Uniform(torch.tensor([0.0]), torch.tensor([10.0]))

sizes = []
ids = []

for _ in range(N_SAMPLES):
    packet_size = int(size_dist.sample().long().numpy()[0] + 1)
    packet_id = int(id_dist.sample().long().numpy()[0])
    sizes.append(packet_size)
    ids.append(packet_id)

arr = [str(a) + " " + str(b) + "\n" for a, b in zip(sizes, ids)]
with open('trace/generated_data', 'w') as f:
    f.write("".join(arr))