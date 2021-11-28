import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

objs = {
    "queue": {
        "targets": {
            # TODO: our -> memento로 이름 변경
            "our_queue": {'label': "Memento queue", 'marker': 'o', 'color': 'k', 'style': '-'},
            'our_pipe_queue': {'label': "Memento pipe-queue", 'marker': 'o', 'color': 'firebrick', 'style': '-'},
            'durable_queue': {'label': "Durable queue", 'marker': 'd', 'color': 'hotpink', 'style': '--'},
            'log_queue': {'label': "Log queue", 'marker': 'x', 'color': 'c', 'style': '--'},
            'dss_queue': {'label': "DSS queue", 'marker': 'v', 'color': 'orange', 'style': '--'},
        },
        'bench_kinds': ['pair', 'prob50'],
    },
    # TODO: pipe 실험 가능하게 하기
    # "pipe": {
    #     "targets": {
    #         'our_pipe': {'label': "Memento pipe", 'marker': 'o', 'color': 'k', 'style': '-'},
    #         'crndm_pipe': {'label': "Corundum pipe", 'marker': 'd', 'color': 'hotpink', 'style': '--'},
    #         'pmdk_pipe': {'label': "PMDK pipe", 'marker': 'x', 'color': 'c', 'style': '--'},
    #     },
    #     'bench_kinds': ['pipe'],
    # }

    # TODO: other obj..
}

def draw(title, xlabel, ylabel, datas, output, x_interval=1):
    plt.clf()
    markers_on = (datas[0]['x'] == 1) | (datas[0]['x'] % x_interval == 0)

    for data in datas:
        plt.plot(data['x'], data['y'], label=data['label'], color=data['color'], linestyle=data['style'], marker=data['marker'], markevery=markers_on)
    plt.title(title)
    ax = plt.subplot()
    ax.xaxis.set_major_locator(plt.MultipleLocator(x_interval)) # 눈금선 간격
    plt.grid(True)
    plt.legend()
    plt.xlabel(xlabel, size='large')
    plt.ylabel(ylabel, size='large')

    if not split:
        plt.savefig("{}.png".format(output), dpi=300)
    else:
        plt.savefig("{}_split.png".format(output), dpi=300)

for obj in objs:
    data = pd.read_csv("./out/{}.csv".format(obj))
    data = data.groupby(['target', 'bench kind', 'threads'])['throughput'].mean().div(pow(10, 6)).reset_index(name='throughput')
    data = data.groupby(['target', 'bench kind'])['throughput'].apply(list).reset_index(name="throughput")
    targets = objs[obj]['targets']
    kinds = objs[obj]['bench_kinds']

    # (obj, bench kind) 쌍마다 그래프 하나씩 그림 (e.g. queue-pair, queue-prob50, ..)
    for ix, k in enumerate(kinds):
        if obj == "pipe":
            plot_id = "pipe"
        else:
            plot_id = "{}-{}".format(obj, k)
        plot_lines = []

        # Gathering info
        for t in targets:
            label = targets[t]['label']
            shape = targets[t]['marker']
            color = targets[t]['color']
            style = targets[t]['style']
            marker = targets[t]['marker']
            throughputs = data[(data['target']==t) & (data['bench kind']==k)]
            if throughputs.empty:
                continue
            throughputs = list(throughputs['throughput'])[0]
            plot_lines.append({'x': np.arange(1, len(throughputs)+1), 'y': throughputs, 'label': label, 'marker': shape, 'color': color, 'style':style})
        
        # Draw
        draw(plot_id, 'Threads', 'Throughput (M op/s)', plot_lines, "./out/{}".format(plot_id), 4)
