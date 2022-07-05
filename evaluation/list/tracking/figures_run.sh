#!/bin/bash

# Define size and path to NV memory
export VMMALLOC_POOL_SIZE=$((64*1024*1024*1024))
export VMMALLOC_POOL_DIR="/mnt/pmem_fsdax0/"

duration=3			# duration=10 (for paper)
iterations=3        # iterations=10 (for paper)

# Create results folder
mkdir -p results
rm -rf results/*

# for key_range in 500 1500 1000 2000 4000; do
for key_range in 500; do
	# --------------------------------------
	inserts_percentage=0.15
	deletes_percentage=0.15
	filename="results/linked_list_results[$inserts_percentage.$deletes_percentage.$key_range].txt"
	echo "Running manual linked list flush experiments for $duration seconds each and random work $max_work." >> $filename

	inserts_percentage=0.35
	deletes_percentage=0.35
	filename="results/linked_list_results[$inserts_percentage.$deletes_percentage.$key_range].txt"
	echo "Running manual linked list flush experiments for $duration seconds each and random work $max_work." >> $filename
	# --------------------------------------
	
	printf "\n================================================================\n"
	printf "READ-INTENSIVE BENCHMARK (15%% INSERTS, 15%% DELETES, 70%% FINDS)\n"
	printf "================================================================\n"
	printf "\nRunning recoverable experiments for $duration seconds each.\n"
	for list in Tracking Capsules-Opt; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_exp_read $list $threads $duration
			done
		done
	done

	for list in Capsules; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/capsules_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no psyncs for $duration seconds each.\n"
	for list in Tracking-nopsync Capsules-Opt-nopsync; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nopsync_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no pwbs for $duration seconds each.\n"
	for list in Tracking-nopwbs Capsules-Opt-nopwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nopwbs_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no low pwbs for $duration seconds each.\n"
	for list in Tracking-nolowpwbs Capsules-Opt-nolowpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nolowpwbs_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no low and no medium pwbs for $duration seconds each.\n"
	for list in Tracking-nolownomedpwbs Capsules-Opt-nolownomedpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nolownomedpwbs_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only low pwbs for $duration seconds each.\n"
	for list in Tracking-lowpwbs Capsules-Opt-lowpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_lowpwbs_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only med pwbs for $duration seconds each.\n"
	for list in Tracking-medpwbs Capsules-Opt-medpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_medpwbs_exp_read $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only high pwbs for $duration seconds each.\n"
	for list in Tracking-highpwbs Capsules-Opt-highpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_highpwbs_exp_read $list $threads $duration
			done
		done
	done

	# =================================================================================

	printf "\n================================================================\n"
	printf "UPDATE-INTENSIVE BENCHMARK (35%% INSERTS, 35%% DELETES, 30%% FINDS)\n"
	printf "================================================================\n"
	printf "\nRunning recoverable experiments for $duration seconds each.\n"
	# for list in Tracking Capsules-Opt; do
	for list in Tracking Capsules-Opt; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_exp_update $list $threads $duration
			done
		done
	done

	for list in Capsules; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/capsules_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no psyncs for $duration seconds each.\n"
	for list in Tracking-nopsync Capsules-Opt-nopsync; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nopsync_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no pwbs for $duration seconds each.\n"
	for list in Tracking-nopwbs Capsules-Opt-nopwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nopwbs_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no low pwbs for $duration seconds each.\n"
	for list in Tracking-nolowpwbs Capsules-Opt-nolowpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nolowpwbs_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with no low and no medium pwbs for $duration seconds each.\n"
	for list in Tracking-nolownomedpwbs Capsules-Opt-nolownomedpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_nolownomedpwbs_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only low pwbs for $duration seconds each.\n"
	for list in Tracking-lowpwbs Capsules-Opt-lowpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_lowpwbs_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only med pwbs for $duration seconds each.\n"
	for list in Tracking-medpwbs Capsules-Opt-medpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_medpwbs_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only high pwbs for $duration seconds each.\n"
	for list in Tracking-highpwbs Capsules-Opt-highpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_highpwbs_exp_update $list $threads $duration
			done
		done
	done

	printf "\nRunning recoverable experiments with only high pwbs for $duration seconds each.\n"
	for list in Tracking-highpwbs Capsules-Opt-highpwbs; do
		for threads in 1 12 24 36 48 60 72 84 96; do
			for (( i=1; i<=$iterations; i++ )); do
				LD_PRELOAD=libvmmalloc.so.1 ./bin/LLRecoverable_highpwbs_exp_update $list $threads $duration
			done
		done
	done
done