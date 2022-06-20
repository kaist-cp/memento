from os.path import exists
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import git

objs = {
    "cas": {
        "targets": {
            # "cas": {'data_id': '', 'label': "CAS", 'marker': 'o', 'color': 'red', 'style': '-'},
            "mcas": {'data_id': '', 'label': "mmt-CAS", 'marker': 'd', 'color': 'k', 'style': '-'},
            "pcas": {'data_id': '', 'label': "PCAS", 'marker': 'x', 'color': 'green', 'style': '-'},
            "pmwcas": {'data_id': '', 'label': "PMwCAS", 'marker': 'x', 'color': 'red', 'style': '-'},
        },
    },
}


def draw_legend(line, label, figpath):
    plt.clf()
    legendFig = plt.figure("Legend plot")
    legendFig.legend(line, label, loc='center',
                     ncol=len(line))
    legendFig.savefig(figpath, bbox_inches='tight')
    print(figpath)


def draw(xlabel, ylabel, datas, output, x_interval=4):
    plt.clf()
    plt.figure(figsize=(4, 3))
    markers_on = (datas[0]['x'] == 1) | (datas[0]['x'] % x_interval == 0)

    for data in datas:
        # plt.errorbar(data['x'], data['y'], data['stddev'], label=data['label'], color=data['color'],
        #              linestyle=data['style'], marker=data['marker'], markevery=markers_on)
        plt.plot(data['x'], data['y'], label=data['label'], color=data['color'],
                 linestyle=data['style'], marker=data['marker'], markevery=markers_on)
    ax = plt.subplot()
    ax.xaxis.set_major_locator(plt.MultipleLocator(x_interval))
    plt.grid(True)
    plt.xlabel(xlabel, size='large')
    if ylabel != '':
        plt.ylabel(ylabel, size='large')
    figpath = "{}.png".format(output)
    plt.tight_layout()
    plt.savefig(figpath, bbox_inches='tight', pad_inches=0.02, dpi=300)
    print(figpath)
    figpath = "{}.svg".format(output)
    plt.tight_layout()
    plt.savefig(figpath, bbox_inches='tight', pad_inches=0.02, dpi=300)
    print(figpath)
    return ax


for obj in objs:
    targets = objs[obj]['targets']

    # preprocess data
    data = pd.DataFrame()
    for t in targets:

        data_id = objs[obj]['targets'][t]['data_id']

        repo = git.Repo(search_parent_directories=True)
        data_path = ''
        for commit in repo.iter_commits():
            data_path = "./out/{}_{}.csv".format(t, commit.hexsha[:7])
            if exists(data_path):
                break
        if data_id != '':
            data_path = "./out/{}_{}.csv".format(t, data_id)

        print("read {} for target {}".format(data_path, t))
        data = data.append(pd.read_csv(data_path))

    # get stddev
    stddev = data.groupby(['target', 'threads'])['throughput'].std(
        ddof=0).div(pow(10, 6)).reset_index(name='stddev')
    stddev = stddev.groupby(['target'])[
        'stddev'].apply(list).reset_index(name="stddev")

    # get throughput
    data = data.groupby(['target', 'threads'])[
        'throughput'].mean().div(pow(10, 6)).reset_index(name='throughput')
    threads = np.array(list(set(data['threads'])))
    data = data.groupby(['target'])['throughput'].apply(
        list).reset_index(name="throughput")

    # draw graph.

    plot_id = "{}-throughput".format(obj)
    plot_lines = []

    # Gathering info
    for t in targets:
        label = targets[t]['label']
        shape = targets[t]['marker']
        color = targets[t]['color']
        style = targets[t]['style']
        marker = targets[t]['marker']
        throughputs = data[(data['target'] == t)]
        stddev_t = stddev[(stddev['target'] == t)]

        if throughputs.empty:
            continue
        throughputs = list(throughputs['throughput'])[0]
        stddev_t = list(stddev_t['stddev'])[0]

        if len(threads) > len(throughputs):
            gap = len(threads)-len(throughputs)
            throughputs += [None]*gap
            stddev_t += [0]*gap
        plot_lines.append({'x': threads, 'y': throughputs,
                           'stddev': stddev_t, 'label': label, 'marker': shape, 'color': color, 'style': style})

    # Draw
    ylabel = 'Throughput (M op/s)'
    ax = draw('Threads', ylabel,
              plot_lines, "./out/{}".format(plot_id), 8)

    axLine, axLabel = ax.get_legend_handles_labels()
    draw_legend(axLine, axLabel, "./out/{}-legend.png".format(obj))
    draw_legend(axLine, axLabel, "./out/{}-legend.svg".format(obj))
