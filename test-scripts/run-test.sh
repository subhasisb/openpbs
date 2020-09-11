#!/bin/bash

curdir=$(dirname $(readlink -f $0))
cd ${curdir}

resultdir=${curdir}/results/$1
concmd="podman exec pbs-server-1 "

###############################
# utility funcs
###############################
function wait_jobs() {
	local _ct

	while true
	do
		_ct=$(${concmd} /opt/pbs/bin/qstat -Bf 2>/dev/null | grep total_jobs | awk -F' = ' '{ print $2 }')
		if [ "x${_ct}" == "x0" ]; then
			echo "total_jobs: ${_ct}"
			break
		fi
		echo "total_jobs: ${_ct}"
		sleep 5
	done
}

function collect_logs() {
	local host svr svrs _d fsvr="" _destd=${resultdir}/$1

	mkdir -p ${_destd}
	for host in $(cat ${curdir}/nodes)
	do
		svrs=$(ssh ${host} ls -1 /tmp/pbs 2>/dev/null | grep pbs-server | sort -u)
		if [ "x${svrs}" == "x" ]; then
			continue
		fi
		for _d in ${svrs}
		do
			echo "Saving logs from ${_d}"
			if [ -d /tmp/pbs/${_d} ]; then
				cp -rf /tmp/pbs/${_d}/server_logs ${_destd}/${_d}-server-logs
				truncate -s0 /tmp/pbs/${_d}/server_logs/*
				cp -rf /tmp/pbs/${_d}/server_priv/accounting ${_destd}/${_d}-acc-logs
				truncate -s0 /tmp/pbs/${_d}/server_priv/accounting/*
				if [ "x${fsvr}" == "x" ]; then
					cp -rf /tmp/pbs/${_d}/sched_logs ${_destd}/sched_logs
					truncate -s0 /tmp/pbs/${_d}/sched_logs/*
					fsvr=${_d}
				fi
			else
				scp -qr ${host}:/tmp/pbs/${_d}/server_logs ${_destd}/${_d}-server-logs
				ssh ${host} truncate -s0 /tmp/pbs/${_d}/server_logs/\*
				scp -qr ${host}:/tmp/pbs/${_d}/server_priv/accounting ${_destd}/${_d}-acc-logs
				ssh ${host} truncate -s0 /tmp/pbs/${_d}/server_priv/accounting/\*
				if [ "x${fsvr}" == "x" ]; then
					scp -qr ${host}:/tmp/pbs/${_d}/sched_logs ${_destd}/sched_logs
					ssh ${host} truncate -s0 /tmp/pbs/${_d}/sched_logs/\*
					fsvr=${_d}
				fi
			fi
		done
	done
}

function get_total_ncpus() {
	echo $(${concmd} /opt/pbs/bin/pbsnodes -av | grep resources_available.ncpus | awk '{print $3}' | paste -sd+ | bc)
}
###############################
# end of utility funcs
###############################


############################
# test funcs
############################
function test_with_sched_off() {
	local _ct

	${concmd} /opt/pbs/bin/qmgr -c "s s scheduling=0"
	_ct=$(get_total_ncpus)
	${curdir}/submit-jobs.sh $(( _ct * 3 ))
	${concmd} /opt/pbs/bin/qmgr -c "s s scheduling=1"
	wait_jobs
	collect_logs sched_off
}

function test_with_sched_on() {
	local _ct

	${concmd} /opt/pbs/bin/qmgr -c "s s scheduling=1"
	_ct=$(get_total_ncpus)
	${curdir}/submit-jobs.sh $(( _ct * 3 ))
	wait_jobs
	collect_logs sched_on
}
############################
# end test funcs
############################

test_with_sched_off
test_with_sched_on
