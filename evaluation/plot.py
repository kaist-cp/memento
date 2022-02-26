from os.path import exists
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import git
import traceback

# plot 하지 않을 target은 주석처리 해야함
# dava_id: 사용할 데이터 id. 지정된게 없으면 최신 commit에서 뽑은 데이터를 사용
objs = {
    "queue": {
        "targets": {
            "memento_queue": {'data_id': '', 'label': "Memento queue", 'marker': 'o', 'color': 'k', 'style': '-'},
            "memento_queue_lp": {'data_id': '', 'label': "Memento queue-lp", 'marker': 'o', 'color': 'k', 'style': ':'},
            "memento_queue_general": {'data_id': '', 'label': "Memento queue-general", 'marker': 'o', 'color': 'k', 'style': '--'},
            "memento_queue_pbcomb": {'data_id': '', 'label': "Memento queue-pbcomb", 'marker': 'o', 'color': 'k', 'style': '-.'},
            'durable_queue': {'data_id': '', 'label': "Durable queue", 'marker': 'd', 'color': 'hotpink', 'style': '--'},
            'log_queue': {'data_id': '', 'label': "Log queue", 'marker': 'x', 'color': 'c', 'style': '--'},
            'dss_queue': {'data_id': '', 'label': "DSS queue", 'marker': 'v', 'color': 'orange', 'style': '--'},
            'pbcomb_queue': {'data_id': '', 'label': "PBComb queue", 'marker': '>', 'color': 'red', 'style': '--'},
            'pmdk_queue': {'data_id': '', 'label': "PMDK queue", 'marker': 's', 'color': 'skyblue', 'style': '--'},
            'crndm_queue': {'data_id': '', 'label': "Corundum queue", 'marker': '^', 'color': 'green', 'style': '--'},
        },
    },

    # TODO: other obj
}


def draw(xlabel, ylabel, datas, output, x_interval=4):
    plt.clf()
    markers_on = (datas[0]['x'] == 1) | (datas[0]['x'] % x_interval == 0)

    for data in datas:
        plt.errorbar(data['x'], data['y'], data['stddev'], label=data['label'], color=data['color'],
                     linestyle=data['style'], marker=data['marker'], markevery=markers_on)
    ax = plt.subplot()
    ax.xaxis.set_major_locator(plt.MultipleLocator(x_interval))  # 눈금선 간격
    plt.grid(True)
    plt.legend()
    plt.xlabel(xlabel, size='large')
    plt.ylabel(ylabel, size='large')
    fig_path = "{}.png".format(output)
    plt.savefig(fig_path, dpi=300)
    print(fig_path)


for obj in objs:
    targets = objs[obj]['targets']

    # preprocess data
    data = pd.DataFrame()
    for t in targets:

        # 사용할 데이터 선택
        data_id = objs[obj]['targets'][t]['data_id']

        # 사용할 데이터가 지정되지 않았으면, 최신 commit에서 뽑은 데이터를 사용: {hash}_{date}
        repo = git.Repo(search_parent_directories=True)
        data_path = ''
        for commit in repo.iter_commits():
            data_path = "./out/{}_{}.csv".format(t, commit.hexsha[:7])
            if exists(data_path):
                break

        print("read {} for target {}".format(data_path, t))
        data = data.append(pd.read_csv(data_path))

    # get stddev
    stddev = data.groupby(['target', 'bench kind', 'threads'])['throughput'].std(
        ddof=0).div(pow(10, 6)).reset_index(name='stddev')
    stddev = stddev.groupby(['target', 'bench kind'])[
        'stddev'].apply(list).reset_index(name="stddev")

    # get throughput
    data = data.groupby(['target', 'bench kind', 'threads'])[
        'throughput'].mean().div(pow(10, 6)).reset_index(name='throughput')
    threads = np.array(list(set(data['threads'])))
    data = data.groupby(['target', 'bench kind'])['throughput'].apply(
        list).reset_index(name="throughput")

    # draw graph: (obj, bench kind) 쌍마다 그래프 하나씩 그림 (e.g. queue-pair, queue-prob50, ..)
    kinds = set(data['bench kind'])
    for ix, k in enumerate(kinds):
        plot_id = "{}-throughput-{}".format(obj, k)
        plot_lines = []

        # Gathering info
        for t in targets:
            label = targets[t]['label']
            shape = targets[t]['marker']
            color = targets[t]['color']
            style = targets[t]['style']
            marker = targets[t]['marker']
            throughputs = data[(data['target'] == t) &
                               (data['bench kind'] == k)]
            stddev_t = stddev[(stddev['target'] == t) &
                              (stddev['bench kind'] == k)]

            if throughputs.empty:
                continue
            throughputs = list(throughputs['throughput'])[0]
            stddev_t = list(stddev_t['stddev'])[0]

            plot_lines.append({'x': threads, 'y': throughputs,
                              'stddev': stddev_t, 'label': label, 'marker': shape, 'color': color, 'style': style})

        # Draw
        draw('Threads', 'Throughput (M op/s)',
             plot_lines, "./out/{}".format(plot_id), 4)
