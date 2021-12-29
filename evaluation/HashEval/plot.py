import re
import matplotlib.pyplot as plt
import numpy as np
import os.path

N_LATENCY = 7

objs = {
    "hash": {
        "targets": {
            "CCEH": {'label': "CCEH", 'marker': 'x', 'color': 'skyblue', 'style': '-'},
            'Level': {'label': "LEVEL", 'marker': 'o', 'color': 'orange', 'style': '-'},
            # 'Dash': {'label': "Dash", 'marker': '^', 'color': 'green', 'style': '-'},
            'PCLHT': {'label': "PCLHT", 'marker': 'v', 'color': 'gold', 'style': '-'},
            # 'SOFT': {'label': "SOFT", 'marker': 'o', 'color': 'royalblue', 'style': '-'},
            "clevel": {'label': "CLEVEL", 'marker': 's', 'color': 'gray', 'style': '-'},
            "clevel_rust": {'label': "CLEVEL-RUST", 'marker': 'o', 'color': 'black', 'style': '-'},
        },
        'bench_kinds': {
            'throughput': {
                'workloads': {
                    'insert': {'label': "(a) Insert"},
                    'pos_search': {'label': "(b) Pos. Search"},
                    'neg_search': {'label': "(c) Neg. Search"},
                    'delete': {'label': "(d) Delete"},
                    'write_heavy': {'label': "(e) Write heavy"},
                    'balanced': {'label': "(f) Balanced"},
                    'read_heavy': {'label': "(g) Read heavy"},
                },
                'distributions': ['uniform', 'selfsimilar'],

                'x': ['1', '4', '8', '16', '24', '32', '48', '64'],
                'x_label': 'Threads',
                'y_label': 'Throughput (M op/s)',
            },
            'latency': {
                'workloads': {
                    'insert': {'label': "(a) Insert"},
                    'pos_search': {'label': "(b) Pos. Search"},
                    'neg_search': {'label': "(c) Neg. Search"},
                    'delete': {'label': "(d) Delete"},
                },
                'distributions': ['uniform'],

                'x': ['0%', '50%', '90%', '99%', '99.9%', '99.99%', '99.999%'],
                'x_label': 'Percentile',
                'y_label': 'Latency (ns)',
            },
        }
    },
}


def read_throughputs(filepath):
    threads = []
    throughputs = []
    with open(filepath, "r") as f:
        tn = -1
        for i in f.readlines():
            t = re.search('(?<=# Threads: )\d+', i)
            if t:
                threads.append(int(t[0]))
                throughputs.append(None)  # dummy value
                tn += 1
            m = re.search('(?<=Throughput\(Mops/s\): )\d+.\d', i)
            if m:
                throughputs[tn] = float(m[0])
    return threads, throughputs

def read_latency(filepath):
    latency = []
    with open(filepath, "r") as f:
        itf = iter(f)
        for line in itf:
            m = re.search('(?<=Latency\(ns\):)', line)
            if m:
                for i in range(0, N_LATENCY):
                    line = next(itf)  # BEWARE, This could raise StopIteration!
                    ix, lt = line.split()
                    latency.append(int(lt))
    return latency


def draw_ax(bench, ax, datas):
    for data in datas:
        ax.plot(data['x'], data['y'], label=data['label'],
                color=data['color'], linestyle=data['style'], marker=data['marker'])
    if bench == "latency":
        ax.tick_params(labelrotation=45)
        ax.set_yticks(np.arange(1, 4))
        ax.set_yticklabels(['$10^3$', '$10^6$', '$10^9$'], rotation=0)
    ax.grid()
    plt.setp(ax, xlabel=data['xlabel'])

def draw_axes(bench, ylabel, axes_datas):
    fig, axes = plt.subplots(1, len(axes_datas), figsize=(20, 3))
    for i, ax_datas in enumerate(axes_datas):
        draw_ax(bench, axes[i], ax_datas)
    axLine, axLabel = axes[0].get_legend_handles_labels()
    fig.legend(axLine, axLabel,
               loc='upper center', ncol=len(axes_datas[0]), borderaxespad=0.1)
    plt.setp(axes[0], ylabel=ylabel)

# (bench, distribution, workload) 하나당 그래프 하나 생성 e.g. throughput-uniform-insert, throughput-uniform-pos_search, ..
for obj, obj_info in objs.items():
    print(obj)
    targets = obj_info['targets']
    bench_kinds = obj_info['bench_kinds']

    # bench: thourghput-single, thourghput, latency
    for bench, bench_info in bench_kinds.items():

        # distribution: uniform, self-similar
        for dist in bench_info['distributions']:
            plt.clf()
            plot_id = "{}_{}".format(bench, dist)
            bd_datas = []

            # workload: insert, pos_search, ...
            for wl, wl_info in bench_info['workloads'].items():
                wl_datas = []

                # target: CCEH, Level, ... 
                for t, t_plot in targets.items():

                    filepath = "./out/{}/{}/{}/{}.out".format(
                        bench.upper(), dist.upper(), wl, t)

                    if not os.path.isfile(filepath):
                        continue

                    threads = []
                    data = []
                    if bench == "throughput":
                        threads, data = read_throughputs(filepath)
                    elif bench == "latency":
                        threads = [32]
                        data = read_latency(filepath)
                        data = (np.log(data) / np.log(10**3))  # 10*3 단위로 plot
                    else:
                        print("invalid bench: {}", bench)
                        exit()
                    x = bench_info['x']

                    wl_datas.append({'x': x, 'y': data[:len(x)], 'stddev': [
                        0, 0, 0, 0, 0, 0], 'label': t_plot['label'], 'marker': t_plot['marker'], 'color': t_plot['color'], 'style': t_plot['style'], 'xlabel': wl_info['label']})

                # each <bench-dist> may have multiple workloads.
                # therefore, we collect data for all workloads belonging to that <bench-dist>.
                bd_datas.append(wl_datas)

            # draw plt, not save
            draw_axes(bench, bench_info['y_label'], bd_datas)

            # save
            figpath = "./out/{}.png".format(plot_id)
            print(figpath)
            plt.savefig(figpath, bbox_inches='tight', pad_inches=0, dpi=300)
