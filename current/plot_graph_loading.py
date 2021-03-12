import sys
import matplotlib as mpl
mpl.use('pdf')
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf
import matplotlib.gridspec as gridspec
import numpy as np
from datetime import datetime


events = [('Starting the loading', 'Sorting'),
          ('Start creating the tree...','NODEMGR'),
          #('Start sorting...','Sorting'),
          ('Start inserting...','Binary Tables'),
          ('Start process generating new permutation','New Perm')]


def label_value(line):
    idx = line.find('=')
    label = line[0:idx].strip()
    value = line[idx+1:].strip()
    return label, value


def plot(parsedEvents, x, ys, ylims, labels, namefile, sample):
    if sample < 1.0:
        elsToSample = len(x) * sample
        inter = int(len(x) / elsToSample)
        x = x[::inter]

    parseEventsMin = []
    parseEventsLbs = []
    parseEventsMin.append(0)
    parseEventsLbs.append('Encoding')
    for p in parsedEvents:
        parseEventsMin.append(int(p[0]))
        parseEventsLbs.append(p[1])
        # After 'Create Tree' stops (only sampling)
        if p[1] == 'NODEMGR':
            break

    pdf = matplotlib.backends.backend_pdf.PdfPages(namefile)

    fig = plt.figure()
    gs = gridspec.GridSpec(2, 1)

    # First I plot CPU load and Memory consumption in two graphs
    for i in range(2):
        y = ys[i]
        ylim = ylims[i]
        label = labels[i]
        if sample < 1.0:
            y = y[::inter]
        # subfig = fig.add_subplot(210 + i + 1)
        subfig = plt.subplot(gs[i])
        subfig.plot(x, y)
        subfig.set_ylabel(label)
        for label in subfig.get_yticklabels():
            label.set_fontsize(10)  # Size here overrides font_prop
        subfig.fill_between(x, y, color='blue')
        subfig.vlines(parseEventsMin, 0, ylim, linestyles='dotted')
        subfig.set_xlim([0,np.max(x)])
        subfig.set_ylim([0, ylim])
        if i == 1:
            plt.xticks(parseEventsMin, parseEventsLbs, rotation=0, fontsize=8)
        else:
            subfig.set_xlabel('Minutes', labelpad=0)
            plt.xticks(np.arange(0, max(x), 200), fontsize=10)
    gs.tight_layout(fig, h_pad=0.5)
    pdf.savefig(fig)
    plt.close()

    # Now I want to plot one more graph with the stacked I/O
    fig = plt.figure()
    diskr = ys[2]
    diskw = ys[3]
    if sample < 1.0:
        diskr = diskr[::inter]
        diskw = diskw[::inter]
    diskr = np.array(diskr)
    diskw = np.array(diskw)
    plt.plot(x, diskr, label='Read')  # Bytes read
    plt.plot(x, diskr + diskw, label='Write')  # Bytes written
    plt.fill_between(x, diskw + diskr, color='green')
    plt.fill_between(x, diskr, color='blue')
    plt.vlines(parseEventsMin, 0, np.max(diskr + diskw), linestyles='dotted')
    plt.ylabel('I/O (MB)')
    plt.xlabel('Minutes')
    plt.xlim([0,np.max(x)])
    plt.legend()
    pdf.savefig(fig)
    plt.close()
    pdf.close()


time = []
diskr = []
diskw = []
cpu = []
mem = []
i = 0
prev_diskr = 0
prev_diskw = 0

timeline = []
start = None  # Start time
end = None  # End time
maxmem = 0
done = []
for line in open(sys.argv[1], "rt"):
    if start is None:
        start = line[20:39]
    end = line[20:39]

    if "STATS" not in line:
        # Check whether the string corresponds to an event
        idx = 0
        for e in events:
            if e[0] in line and not e[0] in done:
                timeline.append((line[20:39], line[42:-1], idx))
                done.append(e[0])
            idx += 1
        continue

    line = line[:-1]
    tokens = line.split('\t')
    # Get time
    t = tokens[0][20:39]
    # time.append(t)
    time.append(i / 60)
    i += 10
    cpu_l, cpu_v = label_value(tokens[2])
    mem_l, mem_v = label_value(tokens[1])
    diskr_l, diskr_v = label_value(tokens[5])
    diskw_l, diskw_v = label_value(tokens[6])

    diskr.append(int(diskr_v) / 10)
    diskw.append(int(diskw_v) / 10)
    cpu.append(int(cpu_v))
    curmem = int(mem_v)
    mem.append(curmem)
    if (curmem > maxmem):
        maxmem = curmem
# assuming at least 4G, and doubling until maxmem seems reasonable
memlimit = 4
while maxmem > 1.3 * memlimit:
    memlimit += memlimit

# Parse the various events
start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
parsedEvents = []
for t in timeline:
    time_event = datetime.strptime(t[0], '%Y-%m-%d %H:%M:%S')
    duration = time_event - start
    minutes = int(duration.total_seconds() / 60)
    parsedEvents.append((minutes, events[t[2]][1], duration))

plot(parsedEvents, time, [cpu, mem, diskr, diskw], [100, memlimit, 0, 0],
     ['CPU Load (%)', 'RAM (GB)', 'I/O MB read', 'I/O MB written'],
     'output.pdf', 0.01)

# Print some general statistics
print('Start time:', start.strftime('%Y-%m-%d %H:%M:%S'), 'End time:', end.strftime('%Y-%m-%d %H:%M:%S'), 'Duration:', end - start)
for p in parsedEvents:
    print(p[1], '=>', p[0])
