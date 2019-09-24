#!/bin/bash

if [ $# -lt 2 ]; then
	echo "syntax: $0 <num-threads> <jobs-per-thread>"
	exit 1
fi

. /etc/pbs.conf

function submit_jobs {
	svr=$1
	njobs=$2

	if [ "${svr}" = "${PBS_SERVER}" ]; then
		echo "New thread submitting to default Server, jobs=$njobs"
	else
		echo "New thread submitting to Server = $svr, jobs=$njobs"
		export PBS_SERVER_INSTANCES=$svr
	fi

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

if [ "x${PBS_SERVER_INSTANCES}" = "x" ]; then
	echo "Multiserver not configured. Submitting to single server"
	servers=($PBS_SERVER)
else
	IFS=', ' read -r -a servers <<< "$PBS_SERVER_INSTANCES"
fi

num_svrs=${#servers[@]}

echo "parameters supplied: nthreads=$nthreads, njobs=$njobs, num_servers=$num_svrs"

#assign each new thread a new port in a round robin fashion to distribute almost evenly
#qsub background daemons will be created for each different server port, so connections would be persistent

script_path=$0
if [ "${script_path::1}" != "/" ]; then
	script_path=`pwd`/$script_path
fi

echo "Script path=$script_path"

start_time=`date +%s%3N`
j=0
for i in $(seq 1 $nthreads)
do 
    port=`echo ${servers[j]} | awk -F":" '{print $2}'`
	if [ -z "$port" ]; then
		port=15001
	fi
	
	setsid $script_path submit ${servers[j]} $njobs &

	j=$((j + 1))
	if [ $j -ge $num_svrs ]; then
		j=0
	fi
done

wait

end_time=`date +%s%3N`

diff=`bc -l <<< "scale=3; ($end_time - $start_time) / 1000"`
total_jobs=`bc -l <<< "$njobs * $nthreads"`
perf=`bc -l <<< "scale=3; $total_jobs / $diff"`

echo "Time(ms) started=$start_time, ended=$end_time"
echo "Total jobs submitted=$total_jobs, time taken(secs.ms)=$diff, jobs/sec=$perf"

