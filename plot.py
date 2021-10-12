# TODO: 스레드 4개마다 점찍기
import pandas as pd
import matplotlib.pyplot as plt

objs = {
    "queue": {
        "targets": {
            "our_queue": {'line_shape': 'o', 'line_color': 'k', 'line_type': '-'},
            'durable_queue': {'line_shape': 'x', 'line_color': 'hotpink', 'line_type': '--'},
            'log_queue': {'line_shape': 'd', 'line_color': 'c', 'line_type': '--'},
        },
        'bench_kinds': ['prob50', 'pair'],
        'plot_lower_ylim': [(2, 10), (1, 5)],
    },
    "pipe": {
        "targets": {
            # 'our_pipe': {'line_shape': 'o', 'line_color': 'darkblue'},
            # 'crndm_pipe': {'line_shape': 'x', 'line_color': 'c'},
            'pmdk_pipe': {'line_shape': 'd', 'line_color': 'c', 'line_type': '--'},
        },
        'bench_kinds': ['pipe'],
        'plot_lower_ylim': [(2, 10)],
    }

    # TODO: other obj..
}

def draw(title, xlabel, ylabel, datas, output, x_interval=2, split=False, upper_ylim=(0, 0), lower_ylim=(0, 0)):
    plt.clf()
    if not split:
        for data in datas:
            plt.plot(data['x'], data['y'], label=data['label'], color=data['color'], linestyle=data['line_type'])
        plt.title(title)
        ax = plt.subplot()
        ax.xaxis.set_major_locator(plt.MultipleLocator(x_interval)) # 눈금선 간격
        plt.grid(True)
    else:
        f, (upper, lower) = plt.subplots(2, 1, sharex=True, gridspec_kw={'height_ratios': [1, 3]})
        for data in datas:
            upper.plot(data['x'], data['y'], label=data['label'], color=data['color'], linestyle=data['line_type'])
            lower.plot(data['x'], data['y'], label=data['label'], color=data['color'], linestyle=data['line_type'])
        upper.set_title(title)
        upper.xaxis.set_major_locator(plt.MultipleLocator(x_interval)) # 눈금선 간격
        upper.set_ylim(upper_ylim[0], upper_ylim[1])  # 위 plot 범위
        lower.set_ylim(lower_ylim[0], lower_ylim[1])  # 아래 plot 범위
        upper.grid(True)
        lower.grid(True)
    plt.legend()
    plt.xlabel(xlabel, size='large')
    plt.ylabel(ylabel, size='large')

    if not split:
        plt.savefig("{}.pdf".format(output))
    else:
        plt.savefig("{}_split.pdf".format(output))
    plt.show()

for obj in objs:
    data = pd.read_csv("./out/{}.csv".format(obj))
    data = data.groupby(['target', 'bench kind', 'threads'])['throughput'].mean().div(pow(10, 6)).reset_index(name='throughput')
    data = data.groupby(['target', 'bench kind'])['throughput'].apply(list).reset_index(name="throughput")
    targets = objs[obj]['targets']
    kinds = objs[obj]['bench_kinds']
    # (obj, bench kind) 쌍마다 그래프 하나씩 그림 (e.g. queue-pair, queue-prob50, ..)
    for ix, k in enumerate(kinds):
        plot_id = "{}-{}".format(obj, k)
        plot_lines = []
        # Gathering info
        for t in targets:
            shape = targets[t]['line_shape']
            color = targets[t]['line_color']
            line_type = targets[t]['line_type']
            throughputs = data[(data['target']==t) & (data['bench kind']==k)]
            if throughputs.empty:
                continue
            throughputs = list(throughputs['throughput'])[0]
            plot_lines.append({'x': list(range(1, len(throughputs)+1)), 'y': throughputs, 'label': t, 'marker': shape, 'color': color, 'line_type':line_type})
        # Draw
        draw(plot_id, 'threads', 'Throughput (M op/s)', plot_lines, "./out/{}".format(plot_id))
        # Draw split
        th_min, th_max = 65535, -1
        for line in plot_lines:
            th_min = min(th_min, line['y'][0])
            th_max = max(th_max, line['y'][0])
        upper_ylim = (th_min-2, th_max+2)
        lower_ylim = objs[obj]['plot_lower_ylim'][ix]
        draw(plot_id, 'threads', 'Throughput (M op/s)', plot_lines, "./out/{}".format(plot_id), 2, True, upper_ylim, lower_ylim)