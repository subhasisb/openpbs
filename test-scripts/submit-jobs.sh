#!/bin/bash

_curdir=$(dirname $(readlink -f $0))
_self=${_curdir}/$(basename $0)
cd ${_curdir}

if [ $# -gt 2 ]; then
	echo "syntax: $0 <no. jobs-per-svr>"
	exit 1
fi

if [ "x$1" == "xsubmit" ]; then
	njobs=$2

	. /etc/pbs.conf
	export PBS_SERVER_INSTANCES=${PBS_SERVER}:${PBS_BATCH_SERVICE_PORT}

	for i in $(seq 1 $njobs)
	do
		/opt/pbs/bin/qsub -koe -- /bin/date > /dev/null
	done
	exit 0
fi

if [ "x$1" == "xsvr-submit" ]; then
	njobs=$2
	users=$(awk -F: '/^(pbsu|tst)/ {print $1}' /etc/passwd)
	_tu=$(awk -F: '/^(pbsu|tst)/ {print $1}' /etc/passwd | wc -l)
	_jpu=$(( njobs / _tu ))

	for usr in ${users}
	do
		( setsid sudo -Hiu ${usr} ${_self} submit ${_jpu} ) &
	done
	wait
	exit 0
fi

if [ "x$1" == "xhost-submit" ]; then
	njobs=$2
	svrs=$(ls -1 /tmp/pbs 2>/dev/null | grep pbs-server | sort -u)
	if [ "x${svrs}" == "x" ]; then
		exit 0
	fi

	for svr in ${svrs}
	do
		( podman exec ${svr} ${_self} svr-submit ${njobs} ) &
	done
	wait
	exit 0
fi

njobs=$1
servers="$(podman exec pbs-server-1 grep PBS_SERVER_INSTANCES /etc/pbs.conf 2>/dev/null | cut -d= -f2)"
if [ "x${servers}" == "x" ]; then
	servers="pbs-server-1"
fi
_svrs=( ${servers//,/ } )
_ts=${#_svrs[*]}
_jps=$(( njobs / _ts ))

echo "Total jobs: ${njobs}, Total svrs: ${_ts}, Jobs per Svr: ${_jps}"
for host in $(cat nodes)
do
	( ssh ${host} ${_self} host-submit ${_jps} ) &
done
wait
