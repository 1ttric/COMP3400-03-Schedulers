#!/usr/bin/python3

import collections
import queue
import math
import json
import csv
import argparse

class Sorts:
	def FCFS(processes):
		return sorted(processes, key=lambda p: (p.arrivaltime, p.PID))

	def SJF(processes):
		return sorted(processes, key=lambda p: (p.bursttime - p.timecompleted, p.arrivaltime, p.PID))

	def priority(processes):
		return sorted(processes, key=lambda p: (p.priority, p.arrivaltime, p.PID))

class Process:
	def __init__(self, PID=None, arrivaltime=None, bursttime=None, priority=None):
		self.PID = PID
		self.arrivaltime = arrivaltime
		self.bursttime = bursttime
		self.priority = priority

		self.timecompleted = 0

	def __repr__(self):
		return "<Process P{}, AT{}, BT{}, P{}>".format(self.PID, self.arrivaltime, self.bursttime, self.timecompleted, self.priority)

	def __str__(self):
		return self.__repr__()

def FCFS(processes):
	arrivals = collections.defaultdict(lambda: [])
	for p in processes:
		p.timecompleted = 0
		arrivals[p.arrivaltime].append(p)

	activeprocess = None
	waitqueue = []
	t1 = sum(p.bursttime for p in processes)

	for t in range(t1):
		waitqueue.extend(arrivals[t])
		activeprocess = Sorts.FCFS(waitqueue)[0]
		activeprocess.timecompleted += 1
		yield activeprocess
		if activeprocess.timecompleted == activeprocess.bursttime:
			waitqueue.remove(activeprocess)
			activeprocess = None

def SRT(processes):
	arrivals = collections.defaultdict(lambda: [])
	for p in processes:
		p.timecompleted = 0
		arrivals[p.arrivaltime].append(p)

	activeprocess = None
	waitqueue = []
	t1 = sum(p.bursttime for p in processes)

	for t in range(t1):
		waitqueue.extend(arrivals[t])
		activeprocess = Sorts.SJF(waitqueue)[0]
		activeprocess.timecompleted += 1
		yield activeprocess
		if activeprocess.timecompleted == activeprocess.bursttime:
			waitqueue.remove(activeprocess)
			activeprocess = None

def SJF(processes):
	arrivals = collections.defaultdict(lambda: [])
	for p in processes:
		p.timecompleted = 0
		arrivals[p.arrivaltime].append(p)

	activeprocess = None
	waitqueue = []
	t1 = sum(p.bursttime for p in processes)

	for t in range(t1):
		new_arrivals = arrivals[t]
		if activeprocess is None or new_arrivals:
			waitqueue.extend(new_arrivals)
			activeprocess = Sorts.SJF(waitqueue)[0]
		activeprocess.timecompleted += 1
		yield activeprocess
		if activeprocess.timecompleted == activeprocess.bursttime:
			waitqueue.remove(activeprocess)
			activeprocess = None

def priority(processes):
	arrivals = collections.defaultdict(lambda: [])
	for p in processes:
		p.timecompleted = 0
		arrivals[p.arrivaltime].append(p)

	activeprocess = None
	waitqueue = []
	t1 = sum(p.bursttime for p in processes)

	for t in range(t1):
		waitqueue.extend(arrivals[t])
		activeprocess = Sorts.priority(waitqueue)[0]
		activeprocess.timecompleted += 1
		yield activeprocess
		if activeprocess.timecompleted == activeprocess.bursttime:
			waitqueue.remove(activeprocess)
			activeprocess = None

def RR_fixed(processes, quantum):
	arrivals = collections.defaultdict(lambda: [])
	for p in processes:
		p.timecompleted = 0
		arrivals[p.arrivaltime].append(p)

	activeprocess = None
	waitqueue = queue.Queue()
	t1 = sum(math.ceil(p.bursttime/quantum)*quantum for p in processes)

	for t in range(t1):
		for p in Sorts.FCFS(arrivals[t]):
			waitqueue.put(p)
		if not t % quantum:
			if activeprocess is not None:
				waitqueue.put(activeprocess)
			activeprocess = waitqueue.get()
		if activeprocess is not None:
			activeprocess.timecompleted += 1
		yield activeprocess
		if activeprocess is not None and activeprocess.timecompleted == activeprocess.bursttime:
			activeprocess = None

def RR_variable(processes, quantum):
	arrivals = collections.defaultdict(lambda: [])
	for p in processes:
		p.timecompleted = 0
		arrivals[p.arrivaltime].append(p)

	activeprocess = None
	waitqueue = queue.Queue()
	t1 = sum(p.bursttime for p in processes)
	lastboundary = 0

	for t in range(t1):
		for p in Sorts.FCFS(arrivals[t]):
			waitqueue.put(p)
		if activeprocess is None or t % quantum == lastboundary % quantum:
			if activeprocess is not None:
				waitqueue.put(activeprocess)
			activeprocess = waitqueue.get()
		activeprocess.timecompleted += 1
		yield activeprocess
		if activeprocess.timecompleted == activeprocess.bursttime:
			activeprocess = None
			lastboundary = t+1

# Result should be a list of Process instances, representing a Gantt chart
def plot(result):
	#Unit width calculated from longest PID
	unit_width = max(len(str(p.PID)) for p in result) + 3
	widths = []
	activeprocess = result[0]
	processbegin = 0
	for idx, p in enumerate(result):
		if p != activeprocess:
			widths.append((activeprocess, processbegin, idx))
			activeprocess = p
			processbegin = idx
	widths.append((activeprocess, processbegin, len(result)-1))

	print("+", end="")
	for p, begin, end in widths:
		width = unit_width*(end-begin)
		print("-"*width+"+", end="")

	print("\n|", end="")
	for p, begin, end in widths:
		width = unit_width*(end-begin)
		print(("{: ^" + str(width) + "}").format("P"+str(p.PID))+"|", end="")

	print("\n+", end="")
	for p, begin, end in widths:
		width = unit_width*(end-begin)
		print("-"*width+"+", end="")

	print()
	for p, begin, end in widths:
		width = unit_width*(end-begin)
		print(("{: <" + str(width+1) + "}").format(begin), end="")
	print(widths[-1][-1])

def analyze(result):
	processes = set(result)
	end_times = {p: [idx for idx, p2 in enumerate(result) if p is p2][-1] for p in processes}
	turnaround_times = {p: end_times[p] - p.arrivaltime for p in processes}
	wait_times = {p: len([p2 for p2 in result[p.arrivaltime: end_times[p]+1] if p2 is not p]) for p in processes}

	j = json.dumps({
		"turnaround_times": {"P%d"%p.PID: v for p, v in turnaround_times.items()},
		"wait_times": {"P%d"%p.PID: v for p, v in wait_times.items()},
		}, indent=4)
	return j

def parseCSV(fname):
	with open(fname, "r") as csvfile:
		reader = csv.reader(csvfile, delimiter=",")

		processes = []
		for PID, AT, BT, P in reader:
			PID, AT, BT, P = int(PID), int(AT), int(BT), int(P)
			p = Process(PID, AT, BT, P)
			processes.append(p)
		return processes

def main():
	parser = argparse.ArgumentParser(description="Draw Gantt diagram and calculate statistics for a certain process scheduling algorithm")
	parser.add_argument("input", type=str, help="The path to a csv file containing [PID, AT, BT, Priority] lines for each process")
	parser.add_argument("--FCFS", action='store_true', help="Print an analysis based on the FCFS algorithm")
	parser.add_argument("--SRT", action='store_true', help="Print an analysis based on the SRT algorithm")
	parser.add_argument("--SJF", action='store_true', help="Print an analysis based on the SJF algorithm")
	parser.add_argument("--priority", action='store_true', help="Print an analysis based on the priority algorithm")
	parser.add_argument("--fRR", action='store_true', help="Print an analysis based on the fixed RR algorithm")
	parser.add_argument("--vRR", action='store_true', help="Print an analysis based on the variable RR algorithm")
	args = parser.parse_args()

	processes = parseCSV(args.input)

	if args.FCFS:
		result = list(FCFS(processes))
		print("\nFCFS:")
		plot(result)
		print(analyze(result))
	if args.SRT:
		result = list(SRT(processes))
		print("\nSRT:")
		plot(result)
		print(analyze(result))
	if args.SJF:
		result = list(SJF(processes))
		print("\nSJF:")
		plot(result)
		print(analyze(result))
	if args.priority:
		result = list(priority(processes))
		print("\nPriority:")
		plot(result)
		print(analyze(result))
	if args.fRR:
		result = list(RR_fixed(processes))
		print("\nRound-robin fixed:")
		plot(result)
		print(analyze(result))
	if args.vRR:
		result = list(RR_variable(processes))
		print("\nRound-robin variable:")
		plot(result)
		print(analyze(result))

if __name__ == "__main__":
	main()