from os.path import exists
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import git
import traceback
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

objs = {
    "list": {
        "targets": {
            "memento_list": {'data_id': '', 'label': "List-mmt", 'marker': 'o', 'color': 'k', 'style': '-'},
            'Tracking': {'data_id': '', 'label': "Tracking", 'marker': 's', 'color': 'hotpink', 'style': '--'},
            'Capsules': {'data_id': '', 'label': "Capusles", 'marker': 's', 'color': 'c', 'style': '--'},
            'Capsules-Opt': {'data_id': '', 'label': "Capusles-Opt", 'marker': 's', 'color': 'orange', 'style': '--'},
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
    plt.tight_layout()
    figpath = "{}.png".format(output)
    plt.savefig(figpath, bbox_inches='tight', pad_inches=0.02, dpi=300)
    print(figpath)
    figpath = "{}.svg".format(output)
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
            data_path_target = "./out/{}_{}.csv".format(t, commit.hexsha[:7])
            if exists(data_path_target):
                data_path = data_path_target
                break
        if data_id != '':
            data_path = "./out/{}_{}.csv".format(t, data_id)
        if data_path == '':
            data_path = "./out/{}.csv".format(t)

        print("read {} for target {}".format(data_path, t))
        data = data.append(pd.read_csv(data_path))

    # get throughput
    data = data.groupby(['target', 'insert', 'threads'])[
        'throughput'].mean().div(pow(10, 6)).reset_index(name='throughput')
    threads = np.array(list(set(data['threads'])))
    data = data.groupby(['target', 'insert'])['throughput'].apply(
        list).reset_index(name="throughput")

    # draw graph per (obj, insert %) pairs.
    insert_ratios = set(data['insert'])
    for ix, ins_rt in enumerate(insert_ratios):
        if ins_rt == 0.15:
            workload = 'read-intensive'
        elif ins_rt == 0.35:
            workload = 'update-intensive'
        else:
            exit(1)
        plot_id = "{}-throughput-{}".format(obj, workload)
        plot_lines = []

        # Gathering info
        for t in targets:
            label = targets[t]['label']
            shape = targets[t]['marker']
            color = targets[t]['color']
            style = targets[t]['style']
            marker = targets[t]['marker']
            throughputs = data[(data['target'] == t) &
                               (data['insert'] == ins_rt)]

            if throughputs.empty:
                continue
            throughputs = list(throughputs['throughput'])[0]

            if len(threads) > len(throughputs):
                gap = len(threads)-len(throughputs)
                throughputs += [None]*gap
            plot_lines.append({'x': threads, 'y': throughputs,
                              'label': label, 'marker': shape, 'color': color, 'style': style})

        # Draw
        if workload == 'read-intensive':
            ylabel = 'Throughput (M op/s)'
        else:
            ylabel = ''
        ax = draw('Threads', ylabel,
                  plot_lines, "./out/{}".format(plot_id), 8)
    axLine, axLabel = ax.get_legend_handles_labels()
    print(axLabel)
    draw_legend(axLine, axLabel, "./out/{}-legend.png".format(obj))
    draw_legend(axLine, axLabel, "./out/{}-legend.svg".format(obj))
