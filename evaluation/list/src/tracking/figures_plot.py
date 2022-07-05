
import sqlite3
import os.path
from os.path import isdir
from os import mkdir
import numpy as np
import matplotlib as mpl
mpl.use('Agg')

import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import sys
import math

font = {'weight' : 'normal',
            'size'   : 12}
mpl.rc('font', **font)

def to_string(ds, flush, th):
    return ds + "::" + str(flush) + "::" + str(th)

def to_string2(ds, flush, th, cr):
    return ds + "::" + str(flush) + "::" + str(th) + "::" + cr

def to_string3(ds, flush, th, cr, socket):
    return ds + "::" + str(flush) + "::" + str(th) + "::" + cr + "::" + socket

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def RepresentsFloat(s):
    try: 
        float(s)
        return True
    except ValueError:
        return False

def avg(l):
    return sum(l)/len(l)

def read_from_file(filename, dataraw, data):
    with open(filename) as f:
        ds = ''
        th = 0
        flush = ''
        for line in f:
            if 'automatic' in line:
                flush = 'automatic'
            elif 'manual' in line:
                flush = 'manual'
            elif 'Test' in line:
                ds = line.split()[1]
                th = int(line.split()[5])
            elif(RepresentsInt(line)):
                key = to_string(ds, flush, th)
                exp = to_string("", flush, th)
                if exp not in experiments:
                    experiments.append(exp)
                if ds not in datastructures:
                    datastructures.append(ds)
                if key not in dataraw:
                    dataraw[key] = []
                # dataraw[key].append(int(line)/1000000.0)
                dataraw[key].append(int(line))
            elif (RepresentsFloat(line)):
                key = to_string(ds, flush, th)
                exp = to_string("", flush, th)
                if exp not in experiments:
                    experiments.append(exp)
                if ds not in datastructures:
                    datastructures.append(ds)
                if key not in dataraw:
                    dataraw[key] = []
                dataraw[key].append(float(line))
    for key in dataraw:
        # if (max(dataraw[key])-min(dataraw[key]))/avg(dataraw[key]) > 0.1:
        #     print( str( int( 100*(max(dataraw[key])-min(dataraw[key]))/avg(dataraw[key]) ) ) + "% " + key )
        data[key] = avg(dataraw[key])

def plot_line_x_thread(title,subdir, name, ds_list, th_list, flush, inserts_percentage, deletes_percentage, max_work, key_range, word):

    suffix_name = "[" + str("{0:.2f}").format(inserts_percentage) + "." +  str("{0:.2f}").format(deletes_percentage) + "." + str(key_range) + "]";

    datastructures = []
    experiments = []

    global throughput
    throughput = {}
    throughputRaw = {}

    read_from_file(
                "results/linked_list_results" + suffix_name + ".txt", 
            throughputRaw, 
            throughput)

    data = throughput

    series = {}
    for ds in ds_list:
        series[ds] = []
        for th in th_list:
            if to_string(ds, flush, th) not in data:
                print("setting " + to_string(ds, flush, th) + " to 0")
                series[ds].append(0)
            else:
                # series[ds].append(data[to_string(ds, flush, th)])
                if word == '':
                    series[ds].append(
                        float(data[to_string(ds, flush, th)]) / 1000000.0
                    )
                else:
                    series[ds].append(
                        float(data[to_string(ds, flush, th)])
                    )
    # create plot
    fig, ax = plt.subplots()
    bar_width = 0.15
    opacity = 0.8
    rects = {}
    width = 0
    low_ds = ''
    med_ds = ''
    X = np.arange(9)
     
    offset = 0
    # jet = plt.cm.jet
    # colors = jet(np.linspace(0, 2, len(ds)+2))
    # for ds, color  in zip(ds_list, colors):
    for ds  in ds_list:
        mark = "o"
        ds_2 = ds
        if "Tracking" in ds:
            mark = "X"
            # ds_2 = "ISB-Opt2-Template2-RDCPOpt"
            ds_2 = "Tracking"
            if "nopsync" in ds:
                ds_2 +=" [no psync]"
            elif "nolowpwbs" in ds:
                ds_2 +="[no low pwbs]"
            elif "nopwbs" in ds:
                ds_2 +="[no pwbs]"
            elif "lowpwbs" in ds:
                ds_2 +="[only low pwbs]"
            elif "medpwbs" in ds:
                ds_2 +="[only medium pwbs]"
            elif "Flushes-Low" in ds:
                ds_2 +="[Low]"
            elif "Flushes-Medium" in ds:
                ds_2 +="[Medium]"
        elif "Capsules-Opt" in ds:
            mark = "^"
            ds_2 = "Capsules-Opt"
            if "nopsync" in ds:
                ds_2 +=" [no psync]"
            elif "nolowpwbs" in ds:
                ds_2 +="[no low pwbs]"
            elif "nopwbs" in ds:
                ds_2 +="[no pwbs]"
            elif "lowpwbs" in ds:
                ds_2 +="[only low pwbs]"
            elif "medpwbs" in ds:
                ds_2 +="[only medium pwbs]"
            elif "highpwbs" in ds:
                ds_2 +="[only high pwbs]"
            elif "Flushes-Low" in ds:
                ds_2 +="[Low]"
            elif "Flushes-Medium" in ds:
                ds_2 +="[Medium]"
            elif "Flushes-High" in ds:
                ds_2 +="[High]"
        elif "Capsules" in ds:
            mark = "v"
            ds_2 = "Capsules"
        # elif "Romulus" in ds:
        #     mark = "d"
        #     ds_2 = "Romulus"
        # elif "RedoOpt" in ds:
        #     mark = "*"

        if word == '_num_pwbs':
            if "-Low" in ds and "Tracking" in ds:
                fig = plt.figure()
                ax = fig.add_axes([0,0,1,1])
                width = 4
                offset = -2;
            elif "-Low" in ds:
                offset = 2

            if "-Low" in ds: 
                if 'Tracking' in ds:
                    ax.bar(th_list+offset, series[ds], width, color = 'lightsalmon')
                else:
                    ax.bar(th_list+offset, series[ds], width, color = 'lightskyblue')
                low_ds = ds
            elif "-Medium" in ds: 
                if 'Tracking' in ds:
                    ax.bar(th_list+offset, series[ds], width, bottom=series[low_ds], color = 'tomato')
                else:
                    ax.bar(th_list+offset, series[ds], width, bottom=series[low_ds], color = 'deepskyblue')
                med_ds = ds
            elif "-High" in ds:
                if 'Tracking' in ds:
                    ax.bar(th_list+offset, series[ds], width, bottom= np.add(np.array(series[low_ds]), np.array(series[med_ds])), color = 'r')
                else:
                    ax.bar(th_list+offset, series[ds], width, bottom= np.add(np.array(series[low_ds]), np.array(series[med_ds])), color = 'b')
        else:
            rects[ds] = plt.plot(th_list, series[ds],
                alpha = opacity,
                marker = mark,
                label = ds_2)
    
     
    plt.xlabel('Number of threads')
    # plt.ylabel('Throughput (Mop/s)')

    if word == '_num_flushes':
        plt.ylabel('pwbs/operation')
    elif word == '_num_fences':
        plt.ylabel('psyncs/operation')
    else:
        plt.ylabel('Throughput \n (millions operations/second)')
    # plt.title('Throughput of ' + benchmark)
    plt.title(title)
    # plt.xticks(index + (len(th_list)-2)*bar_width, th_list)

    if low_ds == '':
        plt.legend(loc="best")
    else:
        ax.legend(loc="best", labels=['Tracking [Low]', 'Tracking [Medium]', 'Capsules-Opt [Low]', 'Capsules-Opt [Medium]', 'Capsules-Opt [High]'])

    
    # if ("500" in title and "1500" not in title) or ("1000" in title and ("Read" in title or word=='_num_flushes')):
    #     plt.legend(loc="best")
     
    # plt.tight_layout()
    #plt.show()

    ax.set_xticks(th_list)


    if not os.path.isdir(outdir+subdir):
        os.makedirs(outdir+subdir)

    # plt.savefig(outdir+subdir+name+suffix_name+word+".png",bbox_inches='tight')

    plt.savefig(outdir+subdir+name+".png",bbox_inches='tight')
    plt.close('all')
    # print "done"

def write_dict(filename, dd):
    if not os.path.isdir(outdir):
        os.makedirs(outdir)

    file = open(outdir + filename, 'w') 
    file.write('experiment\t\tthroughput\n')
    for key, value in dd.items():
        file.write(key + '\t\t' + str(value) + '\n')
    file.close() 


def plot_preprocess1():
    prof_num_flushes = []
    prof_num_fences = []

    for alg in prof_algorithms:
        prof_num_flushes.append(alg +'-Flushes')
        prof_num_fences.append(alg +'-Fence')

    for key_range in key_ranges:
        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure2a', algorithms, threads, 'manual', 0.15, 0.15, 0, key_range, '')        
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure3a', algorithms, threads, 'manual', 0.35, 0.35, 0, key_range, '')        

        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure2b', prof_num_fences, threads, 'manual', 0.15, 0.15, 0, key_range, '_num_fences')
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure3b', prof_num_fences, threads, 'manual', 0.35, 0.35, 0, key_range, '_num_fences')        

        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure2d', prof_num_flushes, threads, 'manual', 0.15, 0.15, 0, key_range, '_num_flushes')
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure3d', prof_num_flushes, threads, 'manual', 0.35, 0.35, 0, key_range, '_num_flushes')        

def plot_preprocess2():
    for key_range in key_ranges:
        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure2c', algorithms, threads, 'manual', 0.15, 0.15, 0, key_range, '')        
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure3c', algorithms, threads, 'manual', 0.35, 0.35, 0, key_range, '')        

def plot_preprocess3():
    for key_range in key_ranges:
        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure2f', algorithms, threads, 'manual', 0.15, 0.15, 0, key_range, '')        
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure3f', algorithms, threads, 'manual', 0.35, 0.35, 0, key_range, '')        

def plot_preprocess4():
    for key_range in key_ranges:
        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure4a', algorithms, threads, 'manual', 0.15, 0.15, 0, key_range, '')        
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure4b', algorithms, threads, 'manual', 0.35, 0.35, 0, key_range, '')        

def plot_preprocess5():
    for key_range in key_ranges:
        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure4c', algorithms, threads, 'manual', 0.15, 0.15, 0, key_range, '')        
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure4d', algorithms, threads, 'manual', 0.35, 0.35, 0, key_range, '')        


def plot_preprocess6():
    for key_range in key_ranges:
        plot_line_x_thread('Read Intensive - KeyRange:' + str(key_range), '', 'Figure2e', prof_algorithms, threads, 'manual', 0.15, 0.15, 0, key_range, '_num_pwbs')
        plot_line_x_thread('Update Intensive - KeyRange:' + str(key_range), '', 'Figure3e', prof_algorithms, threads, 'manual', 0.35, 0.35, 0, key_range, '_num_pwbs')        

datastructures = []
experiments = []
throughput = {}
throughputRaw = {}

threads = np.array([1, 12, 24, 36, 48, 60, 72, 84, 96])
# key_ranges = (500, 1500, 1000, 2000, 4000)
key_ranges = (500,)

outdir = "Figures/"
algorithms = [  'Tracking',
                'Capsules-Opt',
                'Capsules',
                # 'RomulusLR',
                # 'RedoOpt',
                ]

prof_algorithms = [ 'Tracking', 
                    'Capsules-Opt', 
                    ]
plot_preprocess1()

algorithms = [ 'Tracking', 
               'Capsules-Opt', 
               'Tracking-nopsync', 
               'Capsules-Opt-nopsync', 
                ]
plot_preprocess2()

algorithms = [ 'Tracking', 
               'Capsules-Opt', 
               'Tracking-nolowpwbs',
               'Tracking-nopwbs',                   # this is the same with Tracking-nolownomedpwbs
               'Capsules-Opt-nolowpwbs', 
               'Capsules-Opt-nolownomedpwbs', 
               'Capsules-Opt-nopwbs', 
                ]
plot_preprocess3()

algorithms = [ 'Tracking', 
               'Tracking-nopwbs',
               'Tracking-lowpwbs',
               'Tracking-medpwbs',
                ]
plot_preprocess4()


algorithms = [ 'Capsules-Opt', 
               'Capsules-Opt-nopwbs',
               'Capsules-Opt-lowpwbs',
               'Capsules-Opt-medpwbs',
               'Capsules-Opt-highpwbs',
                ]
plot_preprocess5()


prof_algorithms = [ 'Tracking-Flushes-Low', 
                    'Tracking-Flushes-Medium', 
                    'Capsules-Opt-Flushes-Low', 
                    'Capsules-Opt-Flushes-Medium', 
                    'Capsules-Opt-Flushes-High', 
                    ]
plot_preprocess6()

# ---------------------------------------------------------------------

