import pandas as pd
import matplotlib.pyplot as plt

objs = {
    "queue": {
        "targets": {
            "our_queue": {'line_shape': 'o', 'line_color': 'darkblue'},
            'durable_queue': {'line_shape': 'x', 'line_color': 'c'},
            'log_queue': {'line_shape': 'd', 'line_color': 'k'},
        },
        'bench_kinds': ['prob50', 'pair'],
    },
    # "pipe": {
    #     "targets": {
    #         'our_pipe': {'line_shape': 'o', 'line_color': 'darkblue'},
    #         'crndm_pipe': {'line_shape': 'x', 'line_color': 'c'},
    #         'pmdk_pipe': {'line_shape': 'd', 'line_color': 'k'},
    #     },
    #     'bench_kinds': ['pipe']
    # }

    # TODO: other obj..
}

def draw(title, xlabel, ylabel, datas, output):
    plt.clf()
    plt.title(title)
    for data in datas:
        plt.plot(data['x'], data['y'], label=data['label'], marker=data['marker'], color=data['color'])
    # 라벨 설정
    plt.xlabel(xlabel, size='large')
    plt.ylabel(ylabel, size='large')
    plt.legend()
    # 눈금선 간격을 1로 설정
    ax = plt.subplot()
    ax.xaxis.set_major_locator(plt.MultipleLocator(1))
    # 저장
    plt.savefig("{}.pdf".format(output))
    plt.show()

for obj in objs:
    data = pd.read_csv("./out/{}.csv".format(obj))
    data = data.groupby(['target', 'bench kind'])['throughput'].apply(list).reset_index(name="throughput")

    targets = objs[obj]['targets']
    kinds = objs[obj]['bench_kinds']
    for k in kinds:
        # (obj, bench kind) 쌍마다 그래프 하나씩 그림 (e.g. queue의 pair 테스트)
        plot_id = "{}-{}".format(obj, k)
        plot_lines = []
        for t in targets:
            shape = targets[t]['line_shape']
            color = targets[t]['line_color']
            throughputs = data[(data['target']==t) & (data['bench kind']==k)]
            if throughputs.empty:
                continue
            throughputs = list(throughputs['throughput'])[0]
            plot_lines.append({'x': list(range(1, len(throughputs)+1)), 'y': throughputs, 'label': t, 'marker': shape, 'color': color})
        draw(title=plot_id, xlabel='threads', ylabel='Throughput (M op/s)', datas=plot_lines, output="./out/{}".format(plot_id))
