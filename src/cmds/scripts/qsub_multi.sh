#!/bin/bash

if [ $# -lt 2 ]; then
    echo "syntax: $0 <num-threads> <jobs-per-thread>"
    exit 1
fi

function submit_jobs {
    svr=$1
    port=$2
    njobs=$3
    PBS_SERVER_INSTANCES="${svr}:${port}"
    
    if [ $svr != `hostname -s` ]; then
        scp $0 ${svr}:~/
        ssh $svr sh /root/qsub_multi.sh submit $svr $port $njobs
    else
        echo "New thread submitting to $PBS_SERVER_INSTANCES, jobs=$njobs"
        for i in $(seq 1 $njobs)
        do
            PBS_SERVER_INSTANCES="${svr}:${port}" /opt/pbs/bin/qsub -koe -- /bin/date > /dev/null
        done
    fi
}

if [ "$1" = "submit" ]; then
    server=$2
    port=$3
    njobs=$4
    submit_jobs $server $port $njobs 
    exit 0
fi

nthreads=$1
njobs=$2

echo "parameters supplied: nthreads=$nthreads, njobs=$njobs"

#assign each new thread a new port in a round robin fashion to distribute almost evenly
#qsub background daemons will be created for each different server port, so connections would be persistent

if [ -z $PBS_CONF_FILE ]; then
    PBS_CONF_FILE=/etc/pbs.conf
fi
. $PBS_CONF_FILE
        
# Parse PBS_SERVER_INSTANCES into an array of svrname:port values
if [ -z $PBS_SERVER_INSTANCES ]; then
    h=`hostname`
    port=15001
    arr=($h $port)
    arrlen=1
else
    OIFS=$IFS
    IFS=','
    read -a arr <<< $PBS_SERVER_INSTANCES
    IFS=$OIFS
    arrlen=0
    for i in ${arr[@]}; do arrlen=`expr $arrlen + 1`; done
fi
            
start_time=`date +%s%3N`

for i in $(seq 1 $nthreads)
do
    arridx=`expr $i % $arrlen`
    OIFS=$IFS
    IFS=':'
    read -a sarr <<< ${arr[arridx]}
    svr=${sarr[0]}
    port=${sarr[1]}
    if [ -z $port ]; then port=15001; fi
    if [ -z $svr ]; then svr=`hostname`; fi
    #echo "Server: " $svr "Port: " $port "njobs: " $njobs
    setsid $0 submit $svr $port $njobs &
done

wait

end_time=`date +%s%3N`

diff=`bc -l <<< "scale=3; ($end_time - $start_time) / 1000"`
total_jobs=`bc -l <<< "$njobs * $nthreads"`
perf=`bc -l <<< "scale=3; $total_jobs / $diff"`

echo "Time(ms) started=$start_time, ended=$end_time"
echo "Total jobs submitted=$total_jobs, time taken(secs.ms)=$diff, jobs/sec=$perf"