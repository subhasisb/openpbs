/*
 * Copyright (C) 1994-2020 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *
 * This file is part of both the OpenPBS software ("OpenPBS")
 * and the PBS Professional ("PBS Pro") software.
 *
 * Open Source License Information:
 *
 * OpenPBS is free software. You can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * OpenPBS is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Commercial License Information:
 *
 * PBS Pro is commercially licensed software that shares a common core with
 * the OpenPBS software.  For a copy of the commercial license terms and
 * conditions, go to: (http://www.pbspro.com/agreement.html) or contact the
 * Altair Legal Department.
 *
 * Altair's dual-license business model allows companies, individuals, and
 * organizations to create proprietary derivative works of OpenPBS and
 * distribute them - whether embedded or bundled with other software -
 * under a commercial license agreement.
 *
 * Use of Altair's trademarks, including but not limited to "PBS™",
 * "OpenPBS®", "PBS Professional®", and "PBS Pro™" and Altair's logos is
 * subject to Altair's trademark licensing policies.
 */


/**
 * @file	pbs_comm.c
 *
 * @brief
 * 		Implements the router (pbs_comm) process in the TCP network
 *
 * @par	Functionality:
 *          Reads its own router name and port from the pbs config file.
 *          Also reads names of other routers from the pbs config file.
 *          It then calls tpp_router_init to initialize itself as a
 *          router process, and sleeps in a while loop.
 *
 *          It protects itself from SIGPIPEs generated by sends() inside
 *          the library by setting the disposition to ignore.
 *
 */
#include <pbs_config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <signal.h>
#include <avltree.h>

#include <grp.h>
#include <sys/resource.h>

#include <ctype.h>

#include "pbs_ifl.h"
#include "pbs_internal.h"
#include "log.h"
#include "tpp.h"
#include "server_limits.h"
#include "pbs_version.h"
#include "pbs_undolr.h"
#include "auth.h"

char daemonname[PBS_MAXHOSTNAME+8];
extern char	*msg_corelimit;
extern char	*msg_init_chdir;
int lockfds;
int already_forked = 0;
#define PBS_COMM_LOGDIR "comm_logs"

char	        server_host[PBS_MAXHOSTNAME+1];   /* host_name of server */
char	        primary_host[PBS_MAXHOSTNAME+1];   /* host_name of primary */

static int stalone = 0;	/* is program running not as a service ? */
static int get_out = 0;
static int hupped = 0;

/*
 * Server failover role
 */
enum failover_state {
	FAILOVER_NONE,		/* Only Server, no failover */
	FAILOVER_PRIMARY,       /* Primary in failover configuration */
	FAILOVER_SECONDARY,	/* Secondary in failover */
	FAILOVER_CONFIG_ERROR,	/* error in configuration */
	FAILOVER_NEITHER,  /* failover configured, but I am neither primary/secondary */
};

/**
 * @brief
 *		are_we_primary - determines the failover role, are we the Primary
 *		Server, the Secondary Server or the only Server (no failover)
 *
 * @return	int: failover server role
 * @retval  FAILOVER_NONE	- failover not configured
 * @retval  FAILOVER_PRIMARY	- Primary Server
 * @retval  FAILOVER_SECONDARY	- Secondary Server
 * @retval  FAILOVER_CONFIG_ERROR	- error in pbs.conf configuration
 * @retval  FAILOVER_NEITHER	- failover configured, but I am neither primary/secondary
 */
enum failover_state are_we_primary(void)
{
	char hn1[PBS_MAXHOSTNAME+1];

	/* both secondary and primary should be set or neither set */
	if ((pbs_conf.pbs_secondary == NULL) && (pbs_conf.pbs_primary == NULL))
		return FAILOVER_NONE;

	if ((pbs_conf.pbs_secondary == NULL) || (pbs_conf.pbs_primary == NULL))
		return FAILOVER_CONFIG_ERROR;

	if (get_fullhostname(pbs_conf.pbs_primary, primary_host, (sizeof(primary_host) - 1))==-1) {
		log_err(-1, "comm_main", "Unable to get full host name of primary");
		return FAILOVER_CONFIG_ERROR;
	}

	if (strcmp(primary_host, server_host) == 0)
		return FAILOVER_PRIMARY;   /* we are the listed primary */

	if (get_fullhostname(pbs_conf.pbs_secondary, hn1, (sizeof(hn1) - 1))==-1) {
		log_err(-1, "comm_main", "Unable to get full host name of secondary");
		return FAILOVER_CONFIG_ERROR;
	}
	if (strcmp(hn1, server_host) == 0)
		return FAILOVER_SECONDARY;  /* we are the secondary */

	return FAILOVER_NEITHER; /* failover configured, but I am neither primary nor secondary */
}

/**
 * @brief
 *		usage - prints the usage in terminal if the user mistype in terminal
 *
 * @param[in]	prog	- program name which will be typed in the terminal along wih arguments.
 *
 * @return	void
 */
void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-r other_pbs_comms][-t threads][-N]\n"
			"       %s --version\n", prog, prog);
}

/**
 * @brief
 * 		Termination signal handler for the pbs_comm daemon
 *
 * 		Handles termination signals like SIGTERM and sets a global
 * 		variable to exit the daemon
 *
 * @param[in] sig - name of signal caught
 *
 * @return	void
 */
static void
stop_me(int sig)
{
	char buf[100];

	/* set global variable to stop loop */
	get_out = 1;
	sprintf(buf, "Caught signal %d\n", sig);
	fprintf(stderr, "%s\n", buf);
	log_err(-1, __func__, buf);
}

/**
 * @brief
 * 		HUP handler for the pbs_comm daemon
 *
 * 		Handles the SIGHUP signal and sets a global
 * 		variable to reload the daemon configuration
 *
 * @param[in]	sig	- name of signal caught
 *
 * @return	void
 */
static void
hup_me(int sig)
{
	char buf[100];

	/* set global variable to stop loop */
	hupped = 1;
	sprintf(buf, "Caught signal %d\n", sig);
	fprintf(stderr, "%s\n", buf);
	log_err(-1, __func__, buf);
}

/**
 * @brief
 * 		lock out the lockfile for this daemon
 * 			$PBS_HOME/server_priv/comm.lock
 *
 * @param[in] fds - lockfile fd to lock
 * @param[in] op  - F_WRLCK to lock, F_UNLCK to unlock
 *
 * @return	void
 */
static void
lock_out(int fds, int op)
{
	int i;
	int j;
	struct flock flock;
	char buf[100];

	j = 1; /* not fail over, try lock one time */

	(void) lseek(fds, (off_t) 0, SEEK_SET);
	flock.l_type = op;
	flock.l_whence = SEEK_SET;
	flock.l_start = 0;
	flock.l_len = 0;
	for (i = 0; i < j; i++) {
		if (fcntl(fds, F_SETLK, &flock) != -1) {
			if (op == F_WRLCK) {
				/* if write-lock, record pid in file */
				(void) ftruncate(fds, (off_t) 0);
				(void) sprintf(buf, "%d\n", getpid());
				(void) write(fds, buf, strlen(buf));
			}
			return;
		}
		sleep(2);
	}

	(void) strcpy(log_buffer, "another PBS comm router running at the same port");
	fprintf(stderr, "pbs_comm: %s\n", log_buffer);
	exit(1);
}

/**
 * @brief
 * 		set limits for the pbs_comm process
 *
 * @return    - Success/failure
 * @retval  0 - Success
 * @retval -1 - Failure
 *
 */
static void
set_limits()
{
#ifdef  RLIMIT_CORE
	int      char_in_cname = 0;

	if (pbs_conf.pbs_core_limit) {
		char *pc = pbs_conf.pbs_core_limit;
		while (*pc != '\0') {
			if (!isdigit(*pc)) {
				/* there is a character in core limit */
				char_in_cname = 1;
				break;
			}
			pc++;
		}
	}
#endif	/* RLIMIT_CORE */

#if defined(RLIM64_INFINITY)
	{
		struct rlimit64 rlimit;

		rlimit.rlim_cur = TPP_MAXOPENFD;
		rlimit.rlim_max = TPP_MAXOPENFD;

		if (setrlimit64(RLIMIT_NOFILE, &rlimit) == -1) {
			log_err(errno, __func__, "could not set max open files limit");
		}

		rlimit.rlim_cur = RLIM64_INFINITY;
		rlimit.rlim_max = RLIM64_INFINITY;
		(void)setrlimit64(RLIMIT_CPU,   &rlimit);
		(void)setrlimit64(RLIMIT_FSIZE, &rlimit);
		(void)setrlimit64(RLIMIT_DATA,  &rlimit);
		(void)setrlimit64(RLIMIT_STACK, &rlimit);
#ifdef	RLIMIT_RSS
		(void)setrlimit64(RLIMIT_RSS  , &rlimit);
#endif	/* RLIMIT_RSS */
#ifdef	RLIMIT_VMEM
		(void)setrlimit64(RLIMIT_VMEM  , &rlimit);
#endif	/* RLIMIT_VMEM */
#ifdef	RLIMIT_CORE
		if (pbs_conf.pbs_core_limit) {
			struct rlimit64 corelimit;
			corelimit.rlim_max = RLIM64_INFINITY;
			if (strcmp("unlimited", pbs_conf.pbs_core_limit) == 0)
				corelimit.rlim_cur = RLIM64_INFINITY;
			else if (char_in_cname == 1) {
				log_record(PBSEVENT_ERROR, PBS_EVENTCLASS_SERVER, LOG_WARNING,
					__func__, msg_corelimit);
				corelimit.rlim_cur = RLIM64_INFINITY;
			} else
				corelimit.rlim_cur =
					(rlim64_t)atol(pbs_conf.pbs_core_limit);
			(void)setrlimit64(RLIMIT_CORE, &corelimit);
		}
#endif	/* RLIMIT_CORE */
	}

#else	/* setrlimit 32 bit */
	{
		struct rlimit rlimit;

		rlimit.rlim_cur = TPP_MAXOPENFD;
		rlimit.rlim_max = TPP_MAXOPENFD;
		if (setrlimit(RLIMIT_NOFILE, &rlimit) == -1) {
			log_err(errno, __func__, "could not set max open files limit");
		}
		rlimit.rlim_cur = RLIM_INFINITY;
		rlimit.rlim_max = RLIM_INFINITY;
		(void)setrlimit(RLIMIT_CPU, &rlimit);
#ifdef	RLIMIT_RSS
		(void)setrlimit(RLIMIT_RSS, &rlimit);
#endif	/* RLIMIT_RSS */
#ifdef	RLIMIT_VMEM
		(void)setrlimit(RLIMIT_VMEM, &rlimit);
#endif	/* RLIMIT_VMEM */
#ifdef	RLIMIT_CORE
		if (pbs_conf.pbs_core_limit) {
			struct rlimit corelimit;
			corelimit.rlim_max = RLIM_INFINITY;
			if (strcmp("unlimited", pbs_conf.pbs_core_limit) == 0)
				corelimit.rlim_cur = RLIM_INFINITY;
			else if (char_in_cname == 1) {
				log_record(PBSEVENT_ERROR, PBS_EVENTCLASS_SERVER, LOG_WARNING,
					(char *) __func__, msg_corelimit);
				corelimit.rlim_cur = RLIM_INFINITY;
			} else
				corelimit.rlim_cur =
					(rlim_t)atol(pbs_conf.pbs_core_limit);

			(void)setrlimit(RLIMIT_CORE, &corelimit);
		}
#endif	/* RLIMIT_CORE */
#ifndef linux
		(void)setrlimit(RLIMIT_FSIZE, &rlimit);
		(void)setrlimit(RLIMIT_DATA,  &rlimit);
		(void)setrlimit(RLIMIT_STACK, &rlimit);
#else
		if (getrlimit(RLIMIT_STACK, &rlimit) != -1) {
			if((rlimit.rlim_cur != RLIM_INFINITY) && (rlimit.rlim_cur < MIN_STACK_LIMIT)) {
				rlimit.rlim_cur = MIN_STACK_LIMIT;
				rlimit.rlim_max = MIN_STACK_LIMIT;
				if (setrlimit(RLIMIT_STACK, &rlimit) == -1) {
					log_err(errno, __func__, "setting stack limit failed");
					exit(1);
				}
			}
		} else {
			log_err(errno, __func__, "getting current stack limit failed");
			exit(1);
		}
#endif  /* not linux */
	}
#endif	/* !RLIM64_INFINITY */
}

#ifndef DEBUG
/**
 * @brief
 * 		redirect stdin, stdout and stderr to /dev/null
 *		Not done if compiled with debug
 */
void
pbs_close_stdfiles(void)
{
	static int already_done = 0;
#define NULL_DEVICE "/dev/null"

	if (!already_done) {
		(void)fclose(stdin);
		(void)fclose(stdout);
		(void)fclose(stderr);

		fopen(NULL_DEVICE, "r");

		fopen(NULL_DEVICE, "w");
		fopen(NULL_DEVICE, "w");
		already_done = 1;
	}
}

/**
 * @brief
 *		Forks a background process and continues on that, while
 * 		exiting the foreground process. It also sets the child process to
 * 		become the session leader. This function is avaible only on Non-Windows
 * 		platforms and in non-debug mode.
 *
 * @return -  pid_t	- sid of the child process (result of setsid)
 * @retval       >0	- sid of the child process.
 * @retval       -1	- Fork or setsid failed.
 */
static pid_t
go_to_background()
{
	pid_t sid = -1;
	int rc;

	lock_out(lockfds, F_UNLCK);
	rc = fork();
	if (rc == -1) /* fork failed */
		return ((pid_t) -1);
	if (rc > 0)
		exit(0); /* parent goes away, allowing booting to continue */

	lock_out(lockfds, F_WRLCK);
	if ((sid = setsid()) == -1) {
		fprintf(stderr, "pbs_comm: setsid failed");
		return ((pid_t) -1);
	}
	already_forked = 1;
	return sid;
}
#endif	/* DEBUG is defined */

/**
 * @brief
 *		main - the initialization and main loop of pbs_comm
 *
 * @param[in]	argc	- argument count.
 * @param[in]	argv	- argument values.
 *
 * @return	int
 * @retval	0	- success
 * @retval	>0	- error
 */
int
main(int argc, char **argv)
{
	char *name = NULL;
	struct tpp_config conf;
	int tpp_fd;
	char *pc;
	int numthreads;
	char lockfile[MAXPATHLEN + 1];
	char path_log[MAXPATHLEN + 1];
	char svr_home[MAXPATHLEN + 1];
	char *log_file = 0;
	char *host;
	int port;
	char *routers = NULL;
	int c, i, rc;
	extern char *optarg;
	int	are_primary;
	int	num_var_env;
	struct sigaction act;
	struct sigaction oact;

	/*the real deal or just pbs_version and exit*/
	PRINT_VERSION_AND_EXIT(argc, argv);

	/* As a security measure and to make sure all file descriptors	*/
	/* are available to us,  close all above stderr			*/

	i = sysconf(_SC_OPEN_MAX);
	while (--i > 2)
		(void)close(i); /* close any file desc left open by parent */

	/* If we are not run with real and effective uid of 0, forget it */
	if ((getuid() != 0) || (geteuid() != 0)) {
		fprintf(stderr, "%s: Must be run by root\n", argv[0]);
		return (2);
	}

	/* set standard umask */
	umask(022);

	/* load the pbs conf file */
	if (pbs_loadconf(0) == 0) {
		fprintf(stderr, "%s: Configuration error\n", argv[0]);
		return (1);
	}

	set_log_conf(pbs_conf.pbs_leaf_name, pbs_conf.pbs_mom_node_name,
			pbs_conf.locallog, pbs_conf.syslogfac,
			pbs_conf.syslogsvr, pbs_conf.pbs_log_highres_timestamp);

	umask(022);

	/* The following is code to reduce security risks                */
	/* start out with standard umask, system resource limit infinite */
	if ((num_var_env = setup_env(pbs_conf.pbs_environment)) == -1) {
		exit(1);
	}

	i = getgid();
	(void)setgroups(1, (gid_t *)&i);	/* secure suppl. groups */

	log_event_mask = &pbs_conf.pbs_comm_log_events;
	tpp_set_logmask(*log_event_mask);

	routers = pbs_conf.pbs_comm_routers;
	numthreads = pbs_conf.pbs_comm_threads;

	server_host[0] = '\0';
	if (pbs_conf.pbs_comm_name) {
		name = pbs_conf.pbs_comm_name;
		host = tpp_parse_hostname(name, &port);
		if (host)
			snprintf(server_host, sizeof(server_host), "%s", host);
		free(host);
		host = NULL;
	} else if (pbs_conf.pbs_leaf_name) {
		char *endp;

		snprintf(server_host, sizeof(server_host), "%s", pbs_conf.pbs_leaf_name);
		endp = strchr(server_host, ','); /* find the first name */
		if (endp)
			*endp = '\0';
		endp = strchr(server_host, ':'); /* cut out the port */
		if (endp)
			*endp = '\0';
		name = server_host;
	} else {
		if (gethostname(server_host, (sizeof(server_host) - 1)) == -1) {
			sprintf(log_buffer, "Could not determine my hostname, errno=%d", errno);
			fprintf(stderr, "%s\n", log_buffer);
			return (1);
		}
		if ((get_fullhostname(server_host, server_host, (sizeof(server_host) - 1)) == -1)) {
			sprintf(log_buffer, "Could not determine my hostname");
			fprintf(stderr, "%s\n", log_buffer);
			return (1);
		}
		name = server_host;
	}
	if (server_host[0] == '\0') {
		sprintf(log_buffer, "Could not determine server host");
		fprintf(stderr, "%s\n", log_buffer);
		return (1);
	}

	while ((c = getopt(argc, argv, "r:t:e:N")) != -1) {
		switch (c) {
			case 'e': *log_event_mask = strtol(optarg, NULL, 0);
				break;
			case 'r':
				routers = optarg;
				break;
			case 't':
				numthreads = atol(optarg);
				if (numthreads == -1) {
					usage(argv[0]);
					return (1);
				}
				break;
			case 'N':
				stalone = 1;
				break;
			default:
				usage(argv[0]);
				return (1);
		}
	}

	(void)strcpy(daemonname, "Comm@");
	(void)strcat(daemonname, name);
	if ((pc = strchr(daemonname, (int)'.')) != NULL)
		*pc = '\0';

	if(set_msgdaemonname(daemonname)) {
		fprintf(stderr, "Out of memory\n");
		return 1;
	}

	(void) snprintf(path_log, sizeof(path_log), "%s/%s", pbs_conf.pbs_home_path, PBS_COMM_LOGDIR);
	(void) log_open(log_file, path_log);

	/* set pbs_comm's process limits */
	set_limits(); /* set_limits can call log_record, so call only after opening log file */

	(void) snprintf(svr_home, sizeof(svr_home), "%s/%s", pbs_conf.pbs_home_path, PBS_SVR_PRIVATE);
	if (chdir(svr_home) != 0) {
		(void) sprintf(log_buffer, msg_init_chdir, svr_home);
		log_err(-1, __func__, log_buffer);
		return (1);
	}

	(void) sprintf(lockfile, "%s/%s/comm.lock", pbs_conf.pbs_home_path, PBS_SVR_PRIVATE);
	if ((are_primary = are_we_primary()) == FAILOVER_SECONDARY) {
		strcat(lockfile, ".secondary");
	} else if (are_primary == FAILOVER_CONFIG_ERROR) {
		sprintf(log_buffer, "Failover configuration error");
		log_err(-1, __func__, log_buffer);
		return (3);
	}

	if ((lockfds = open(lockfile, O_CREAT | O_WRONLY, 0600)) < 0) {
		(void) sprintf(log_buffer, "pbs_comm: unable to open lock file");
		log_err(errno, __func__, log_buffer);
		return (1);
	}

	if ((host = tpp_parse_hostname(name, &port)) == NULL) {
		sprintf(log_buffer, "Out of memory parsing leaf name");
		log_err(errno, __func__, log_buffer);
		return (1);
	}

	/* set tpp config */
	rc = set_tpp_config(&pbs_conf, &conf, host, port, routers);
	if (rc == -1) {
		(void) sprintf(log_buffer, "Error setting TPP config");
		log_err(-1, __func__, log_buffer);
		return (1);
	}
	free(host);

	i = 0;
	if (conf.routers) {
		while (conf.routers[i]) {
			sprintf(log_buffer, "Router[%d]:%s", i, conf.routers[i]);
			fprintf(stdout, "%s\n", log_buffer);
			log_event(PBSEVENT_SYSTEM | PBSEVENT_FORCE, PBS_EVENTCLASS_SERVER, LOG_INFO, msg_daemonname, log_buffer);
			i++;
		}
	}

#ifndef DEBUG
	if (stalone != 1)
		go_to_background();
#endif

	if (already_forked == 0)
		lock_out(lockfds, F_WRLCK);

	/* go_to_backgroud call creates a forked process,
	 * thus print/log pid only after go_to_background()
	 * has been called
	 */
	sprintf(log_buffer, "%s ready (pid=%d), Proxy Name:%s, Threads:%d", argv[0], getpid(), conf.node_name, numthreads);
	fprintf(stdout, "%s\n", log_buffer);
	log_event(PBSEVENT_SYSTEM | PBSEVENT_FORCE, PBS_EVENTCLASS_SERVER, LOG_INFO, msg_daemonname, log_buffer);
	log_supported_auth_methods(pbs_conf.supported_auth_methods);

#ifndef DEBUG
	pbs_close_stdfiles();
#endif

	/* comm runs 1 + tpp_conf.nuthreads threads - these might use avltree functionality */
	avl_set_maxthreads(numthreads + 1);

	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = hup_me;
	if (sigaction(SIGHUP, &act, &oact) != 0) {
		log_err(errno, __func__, "sigaction for HUP");
		return (2);
	}
	act.sa_handler = stop_me;
	if (sigaction(SIGINT, &act, &oact) != 0) {
		log_err(errno, __func__, "sigaction for INT");
		return (2);
	}
	if (sigaction(SIGTERM, &act, &oact) != 0) {
		log_err(errno, __func__, "sigactin for TERM");
		return (2);
	}
	if (sigaction(SIGQUIT, &act, &oact) != 0) {
		log_err(errno, __func__, "sigactin for QUIT");
		return (2);
	}
#ifdef SIGSHUTDN
	if (sigaction(SIGSHUTDN, &act, &oact) != 0) {
		log_err(errno, __func__, "sigactin for SHUTDN");
		return (2);
	}
#endif	/* SIGSHUTDN */

	act.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &act, &oact) != 0) {
		log_err(errno, __func__, "sigaction for PIPE");
		return (2);
	}
	if (sigaction(SIGUSR2, &act, &oact) != 0) {
		log_err(errno, __func__, "sigaction for USR2");
		return (2);
	}
#ifdef PBS_UNDOLR_ENABLED
	act.sa_handler = catch_sigusr1;
#endif
	if (sigaction(SIGUSR1, &act, &oact) != 0) {
		log_err(errno, __func__, "sigaction for USR1");
		return (2);
	}

	if (load_auths(AUTH_SERVER)) {
		log_err(-1, "pbs_comm", "Failed to load auth lib");
		return 2;
	}

	conf.node_type = TPP_ROUTER_NODE;
	conf.numthreads = numthreads;

	if ((tpp_fd = tpp_init_router(&conf)) == -1) {
		log_err(-1, __func__, "tpp init failed\n");
		return 1;
	}

	/* Protect from being killed by kernel */
	daemon_protect(0, PBS_DAEMON_PROTECT_ON);

	/* go in a while loop */
	while (get_out == 0) {

		if (hupped == 1) {
			struct pbs_config pbs_conf_bak;
			int new_logevent;

			hupped = 0; /* reset back */
			memcpy(&pbs_conf_bak, &pbs_conf, sizeof(struct pbs_config));

			if (pbs_loadconf(1) == 0) {
				log_err(-1, NULL, "Configuration error, ignoring");
				memcpy(&pbs_conf, &pbs_conf_bak, sizeof(struct pbs_config));
			} else {
				/* restore old pbs.conf */
				new_logevent = pbs_conf.pbs_comm_log_events;
				memcpy(&pbs_conf, &pbs_conf_bak, sizeof(struct pbs_config));
				pbs_conf.pbs_comm_log_events = new_logevent;
				log_err(-1, NULL, "Processed SIGHUP");

				log_event_mask = &pbs_conf.pbs_comm_log_events;
				tpp_set_logmask(*log_event_mask);
				set_log_conf(pbs_conf.pbs_leaf_name, pbs_conf.pbs_mom_node_name,
						pbs_conf.locallog, pbs_conf.syslogfac,
						pbs_conf.syslogsvr, pbs_conf.pbs_log_highres_timestamp);
			}
		}
#ifdef PBS_UNDOLR_ENABLED
		if (sigusr1_flag)
			undolr();
#endif

		sleep(3);
	}

	tpp_router_shutdown();

	log_event(PBSEVENT_SYSTEM | PBSEVENT_FORCE, PBS_EVENTCLASS_SERVER, LOG_NOTICE, msg_daemonname, "Exiting");
	log_close(1);

	lock_out(lockfds, F_UNLCK);	/* unlock  */
	(void)close(lockfds);
	(void)unlink(lockfile);
	unload_auths();

	return 0;
}
