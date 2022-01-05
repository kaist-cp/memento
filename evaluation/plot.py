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
            # "memento_queue_unopt": {'data_id': '', 'label': "Memento queue-unopt", 'marker': 'o', 'color': 'firebrick', 'style': '-'},
            "memento_queue_general": {'data_id': '', 'label': "Memento queue-general", 'marker': 'o', 'color': 'k', 'style': '--'},
            # # 'memento_pipe_queue': {'data_id': '', 'label': "Memento pipe-queue", 'marker': 'o', 'color': 'firebrick', 'style': '-'},
            'durable_queue': {'data_id': '', 'label': "Durable queue", 'marker': 'd', 'color': 'hotpink', 'style': '--'},
            'log_queue': {'data_id': '', 'label': "Log queue", 'marker': 'x', 'color': 'c', 'style': '--'},
            'dss_queue': {'data_id': '', 'label': "DSS queue", 'marker': 'v', 'color': 'orange', 'style': '--'},
        },
    },

    # TODO: pipe 필요시 실험 가능하게 하기
    # "pipe": {
    #     "targets": {
    #         'memento_pipe': {'data_id': '', 'label': "Memento pipe", 'marker': 'o', 'color': 'k', 'style': '-'},
    #         'crndm_pipe': {'data_id': '', 'label': "Corundum pipe", 'marker': 'd', 'color': 'hotpink', 'style': '--'},
    #         'pmdk_pipe': {'data_id': '', 'label': "PMDK pipe", 'marker': 'x', 'color': 'c', 'style': '--'},
    #     },
    # }

    # TODO: other obj
}

def draw(title, xlabel, ylabel, datas, output, x_interval=1):
    plt.clf()
    markers_on = (datas[0]['x'] == 1) | (datas[0]['x'] % x_interval == 0)

    for data in datas:
        plt.errorbar(data['x'], data['y'], data['stddev'], color=data['color'], linestyle='None', marker='^', markevery=markers_on)
        plt.plot(data['x'], data['y'], label=data['label'], color=data['color'], linestyle=data['style'], marker=data['marker'], markevery=markers_on)
    plt.title(title)
    ax = plt.subplot()
    ax.xaxis.set_major_locator(plt.MultipleLocator(x_interval)) # 눈금선 간격
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
        if data_id == '':
            # 사용할 데이터가 지정되지 않았으면, 최신 commit에서 뽑은 데이터를 사용
            data_id = git.Repo(search_parent_directories=True).head.object.hexsha[:7]

        # 읽을 csv에는 1~32 스레드 데이터가 다 있어야함
        data = data.append(pd.read_csv("./out/{}_{}.csv".format(t, data_id)))

    # get stddev
    stddev = data.groupby(['target', 'bench kind', 'threads'])['throughput'].std(ddof=0).div(pow(10, 6)).reset_index(name='stddev')
    stddev = stddev.groupby(['target', 'bench kind'])['stddev'].apply(list).reset_index(name="stddev")

    # get throughput
    data = data.groupby(['target', 'bench kind', 'threads'])['throughput'].mean().div(pow(10, 6)).reset_index(name='throughput')
    data = data.groupby(['target', 'bench kind'])['throughput'].apply(list).reset_index(name="throughput")

    # draw graph: (obj, bench kind) 쌍마다 그래프 하나씩 그림 (e.g. queue-pair, queue-prob50, ..)
    kinds = set(data['bench kind'])
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
            stddev_t = stddev[(stddev['target']==t) & (stddev['bench kind']==k)]

            if throughputs.empty:
                continue
            throughputs = list(throughputs['throughput'])[0]
            stddev_t = list(stddev_t['stddev'])[0]

            plot_lines.append({'x': np.arange(1, len(throughputs)+1), 'y': throughputs, 'stddev': stddev_t, 'label': label, 'marker': shape, 'color': color, 'style':style})

        # Draw
        draw(plot_id, 'Threads', 'Throughput (M op/s)', plot_lines, "./out/{}".format(plot_id), 4)
