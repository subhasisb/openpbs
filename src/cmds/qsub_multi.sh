#!/bin/bash

if [ $# -lt 3 ]; then
	echo "syntax: $0 <num-threads> <jobs-per-thread> <server-port-range>"
	exit 1
fi

function submit_jobs {
	port=$1
	njobs=$2

	export PBS_SERVER_INSTANCES=:$port
	echo "New thread submitting to Port = $port, jobs=$njobs"

	for i in $(seq 1 $njobs)
	do
		qsub -- /bin/date > /dev/null
	done
}

if [ "$1" = "submit" ]; then
	port=$2
	njobs=$3
	submit_jobs $port $njobs
	exit 0
fi

nthreads=$1
njobs=$2
port_start=`echo $3 | cut -f1 -d"-"`
port_end=`echo $3 | cut -f2 -d"-"`

echo "parameters supplied: nthreads=$nthreads, njobs=$njobs, port_start=$port_start, port_end=$port_end"

#assign each new thread a new port in a round robin fashion to distribute almost evenly
#qsub background daemons will be created for each different server port, so connections would be persistent

start_time=`date +%s%3N`

port=$port_start
for i in $(seq 1 $nthreads)
do
	setsid $0 submit $port $njobs &
	port=$((port + 1))
	if [ $port -gt $port_end ]; then
		port=$port_start
	fi
done

wait

end_time=`date +%s%3N`

diff=`bc -l <<< "scale=3; ($end_time - $start_time) / 1000"`
total_jobs=`bc -l <<< "$njobs * $nthreads"`
perf=`bc -l <<< "scale=3; $total_jobs / $diff"`

echo "Time(ms) started=$start_time, ended=$end_time"
echo "Total jobs submitted=$total_jobs, time taken(secs.ms)=$diff, jobs/sec=$perf"

