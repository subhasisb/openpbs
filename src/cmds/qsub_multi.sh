#!/bin/bash

if [ $# -lt 3 ]; then
	echo "syntax: $0 <num-threads> <jobs-per-thread> <server-port-range>"
	exit 1
fi

function submit_jobs {
	port=$1
	njobs=$2

	export PBS_BATCH_SERVICE_PORT=$port
	echo "Port = $port"

	for i in $(seq 1 $njobs)
	do
		qsub -- /bin/date > /dev/null
	done
}

nthreads=$1
njobs=$2
port_start=`echo $3 | cut -f1 -d"-"`
port_end=`echo $3 | cut -f2 -d"-"`

echo "parameters supplied: nthreads=$nthreads, njobs=$njobs, port_start=$port_start, port_end=$port_end"

#assign each new thread a new port in a round robin fashion to distribute almost evenly
#qsub background daemons will be created for each different server port, so connections would be persistent

start_time=`date +%s`

port=$port_start
for i in $(seq 1 $nthreads)
do
	echo "Starting thread $i with port $port"
	submit_jobs $port $njobs &
	port=$((port + 1))
	if [ $port -gt $port_end ]; then
		port=$port_start
	fi
done

echo "Waiting for clients to finish"
wait

end_time=`date +%s`
time_taken=$((end_time - start_time))

echo "Done. End_time=$end_time, Start_time=$start_time, time_taken=$time_taken"
