#! /usr/bin/python3
import sys
from matplotlib import pyplot as plt
from math import ceil
from collections import Counter


def sample(xs: list[any], n: int) -> tuple(list[any]):
    interval = [i for i in range(n + 1)]
    try:
        xs = sorted(xs, key = lambda x: float(x))
    except:
        xs = sorted(xs)
    indexes = list(map(lambda x: x / n * len(xs), interval))
    indexes[-1] -= 1
    result = []
    for i in indexes:
        result.append(xs[ceil(i)])
    return indexes, result


def plot(graph_type: str, **kwargs) -> None:
    xs = kwargs.get("xs")
    freq = kwargs.get("freq")
    if graph_type == "hist":
        plt.hist(xs, bins=min(len(set(xs)), 50))
    if graph_type == "bar":
        plt.bar(x=xs, height=freq)
    if graph_type == "scatter":
        plt.scatter(x=xs, y=freq)
    if graph_type == "line":
        plt.plot(xs, freq)


inp = []
for line in sys.stdin:
    if line == "" or line == "\n":
        continue
    inp.append(line.strip())

graph_type = inp[0]
directory = inp[1]
command = inp[2]
data = inp[3:]
print("Plotting ...")

if command == "count":
    data.sort(key=lambda x: float(x.split(":")[0]))
    xs = [line.split(":")[0] for line in data]
    freq = list(map(float, [line.split(":")[1] for line in data]))
    plot(graph_type, xs=xs, freq=freq)

elif command == "select":
    try:
        counter = Counter(list(map(float,data)))
    except: 
        counter = Counter(data)
    xs = sorted([i for i in counter.keys()])
    freq =  [counter[key] for key in xs]
    plot(graph_type, xs=xs, freq=freq)

if len(data) < 50 or command == "select":
    plt.xticks(rotation=90)
else:
    ticks, labels = sample(xs, 10)
    plt.xticks(rotation=90, ticks=ticks, labels=labels)

plt.grid()
plt.savefig(directory)
plt.show()
