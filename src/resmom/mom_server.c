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
 * @file	mom_server.c
 */
#include <pbs_config.h>   /* the master config generated by configure */

#ifndef WIN32
#include	<unistd.h>
#include	<netdb.h>
#include	<netinet/in.h>
#include	<sys/param.h>
#include	<sys/times.h>
#include	<sys/time.h>
#endif

#include	<assert.h>
#include	<stdio.h>
#include	<stdlib.h>
#include	<string.h>
#include	<ctype.h>
#include	<errno.h>
#include	<time.h>
#include	<limits.h>
#include	<sys/types.h>
#include	<sys/stat.h>
#include	<signal.h>

#include	"portability.h"
#include 	"list_link.h"
#include 	"pbs_ifl.h"
#include 	"server_limits.h"
#include	"pbs_error.h"
#include	"attribute.h"
#include	"log.h"
#include	"net_connect.h"
#include	"tpp.h"
#include	"dis.h"
#include 	"pbs_nodes.h"
#include	"placementsets.h"
#include	"resmon.h"
#include	"mom_server.h"
#include	"svrfunc.h"
#include	"server_limits.h"
#include	"credential.h"
#include	"ticket.h"
#include	"libpbs.h"
#include	"batch_request.h"
#include	"pbs_version.h"
#define	MOM_MACH 1	/* don't include the dependent header */
#include	"mom_func.h"
#include	"mom_hook_func.h"



/* Global Data Items */
extern	u_Long		av_phy_mem;	/* phyical memory in KB */
extern	unsigned int	default_server_port;
extern	char		mom_host[];
extern  int		mom_run_state;
extern	char		*msg_daemonname;
extern	int		num_acpus;
extern	int		num_pcpus;
extern	char		*path_jobs;
extern	int		pbs_errno;
extern	int		next_sample_time;
extern	int		min_check_poll;
extern	unsigned int	pbs_mom_port;
extern	unsigned int	pbs_rm_port;
extern	unsigned int	pbs_tm_port;
extern	time_t		time_now;
extern	int		internal_state;
extern	int		internal_state_update;
extern	int		cycle_harvester;
extern  char	        *mom_home;
extern  unsigned long   hook_action_id;
extern	pbs_list_head	svr_alljobs;
extern	pbs_list_head	svr_hook_job_actions;
extern	pbs_list_head	svr_hook_vnl_actions;
extern	pbs_list_head	svr_allhooks;
extern  int		svr_hook_resend_job_attrs;
extern  int 		mom_recvd_ip_cluster_addrs;

extern  int		server_stream;
extern  int		enable_exechost2;
extern  vnl_t		*vnlp;        	   /* vnode list */
extern  vnl_t		*vnlp_from_hook;   /* vnode list updates from hook */
extern  char		*msg_request;

extern void req_commit(struct batch_request *preq);
extern void req_quejob(struct batch_request *preq);
extern void req_jobscript(struct batch_request *preq);

extern void	mom_vnlp_report(vnl_t *vnl, char *header);
extern	char	*path_hooks;
extern	unsigned long	hooks_rescdef_checksum;
extern	int	report_hook_checksums;

/*
 * Tree search generalized from Knuth (6.2.2) Algorithm T just like
 * the AT&T man page says.
 *
 * The node_t structure is for internal use only, lint doesn't grok it.
 *
 * Written by reading the System V Interface Definition, not the code.
 *
 * Totally public domain.
 */
/*LINTLIBRARY*/

/*
 **	Modified by Tom Proett <proett@nas.nasa.gov> for PBS.
 */

typedef struct node_t {
	u_long		key;
	struct node_t	*left, *right;
} node;
node		*okclients = NULL;	/* tree of ip addrs */

/**
 * @brief
 *	find value in tree.
 *
 * @param[in]  key - value to be found in tree
 *
 * @return 	error code
 * @retval  	1     if found,
 * @retval 	0     if not
 */
int
addrfind(key)
const u_long 	key;		/* key to be located */
{
	node	**rootp = &okclients;	/* address of tree root */

#ifdef NAS_CLUSTER /* localmod 024 */
	return 1;
#endif /* localmod 024 */

	while (*rootp != NULL) {	/* Knuth's T1: */
		if (key == (*rootp)->key)	/* T2: */
			return 1;		/* we found it! */
		rootp = (key < (*rootp)->key) ?
			&(*rootp)->left :	/* T3: follow left branch */
		&(*rootp)->right;	/* T4: follow right branch */
	}
	return 0;
}

/**
 * @brief
 * 	insert value into tree
 *
 * @param[in] key - value to be inserted
 *
 * @return Void
 *
 */
void
addrinsert(const u_long key)
{
	register node	*q;
	node		**rootp = &okclients;	/* address of tree root */

	while (*rootp != NULL) {	/* Knuth's T1: */
		if (key == (*rootp)->key)	/* T2: */
			return;			/* we found it! */
		rootp = (key < (*rootp)->key) ?
			&(*rootp)->left :	/* T3: follow left branch */
		&(*rootp)->right;	/* T4: follow right branch */
	}
	q = (node *) malloc(sizeof(node));	/* T5: key not found */
	if (q != NULL) {		/* make new node */
		*rootp = q;			/* link new node to old */
		q->key = key;			/* initialize new node */
		q->left = q->right = NULL;
		sprintf(log_buffer,
			"Adding IP address %ld.%ld.%ld.%ld as authorized",
			(key & 0xff000000) >> 24,
			(key & 0x00ff0000) >> 16,
			(key & 0x0000ff00) >> 8,
			(key & 0x000000ff));
#ifdef NAS /* localmod 094 */
		log_event(PBSEVENT_DEBUG3, PBS_EVENTCLASS_SERVER,  LOG_DEBUG,
			msg_daemonname, log_buffer);
#else
		log_event(PBSEVENT_SYSTEM, PBS_EVENTCLASS_SERVER,  LOG_DEBUG,
			msg_daemonname, log_buffer);
#endif /* localmod 094 */

	}
	return;
}

/**
 * @brief
 *	free the value in tree
 *
 * @param[in] rootp - pointer to root node
 *
 * @return Void
 *
 */
void
addrfree(node **rootp)
{
	if (rootp == NULL || *rootp == NULL)
		return;
	addrfree(&(*rootp)->left);
	addrfree(&(*rootp)->right);
	free(*rootp);
	*rootp = NULL;
}

/**
 * @brief
 *	free_vnodemap - free the mominfo_array entries and mommap_array
 *
 * @return Void
 *
 */
static void
free_vnodemap(void)
{
	int i;

	if (mominfo_array) {
		for (i=0; i<mominfo_array_size; ++i) {
			if (mominfo_array[i]) {
				delete_mom_entry(mominfo_array[i]);
				mominfo_array[i] = NULL;
			}
		}
	}

	if (mommap_array) {
		for (i=0; i<mommap_array_size; ++i) {
			if (mommap_array[i]) {
				delete_momvmap_entry(mommap_array[i]);
				mommap_array[i] = NULL;
			}
		}
	}
}

/**
 * @brief
 *	reply to server
 *
 * @param[in] stream - connection stream
 * @param[in] combine_msg - combine message in the caller
 *
 * @return int
 * @retval	0: success
 * @retval	!0: error code
 *
 */
static int
registermom(int stream, int combine_msg)
{
	int  count = 0;
	int  ret;
	job *pjob;

	/* how many jobs are present */
	for (pjob = (job *)GET_NEXT(svr_alljobs);
		pjob; pjob = (job *)GET_NEXT(pjob->ji_alljobs)) {
		++count;
	}

	/* Now that all of the options data items are set, send */
	/* the option set, followed by the optional data if any */
	/* Please note,  the options MUST be sent in the order  */
	/* that they are defined, least significant bit to most */

	if (!combine_msg)
		if ((ret = is_compose(stream, IS_REGISTERMOM)) != DIS_SUCCESS)
			goto err;

	/* if there are running jobs, report them to the Server */
	/*
		* Add to the REGISTERMOM the count of jobs and the
		* following per running job:
		*   string  - job id
		*   int     - job substate
		*   long    - run version (count)
		*   int     - Node Id  (0 if Mother Superior)
		*   string  - exec_vnode string
		*   string  - pset value if set, otherwise null string
	*/

	if ((ret = diswui(stream, count)) != DIS_SUCCESS)
		goto err;
	for (pjob = (job *)GET_NEXT(svr_alljobs);
		pjob && (count > 0);
		pjob = (job *)GET_NEXT(pjob->ji_alljobs)) {

		--count;

		if ((ret = diswst(stream, pjob->ji_qs.ji_jobid)) != DIS_SUCCESS)
			goto err;
		if ((ret = diswsi(stream, pjob->ji_qs.ji_substate)) != DIS_SUCCESS)
			goto err;

		if (pjob->ji_wattr[(int)JOB_ATR_run_version].at_flags & ATR_VFLAG_SET) {
			ret = diswsl(stream, pjob->ji_wattr[(int)JOB_ATR_run_version].at_val.at_long);
		} else {
			ret = diswsl(stream, pjob->ji_wattr[(int)JOB_ATR_runcount].at_val.at_long);
		}
		if (ret != DIS_SUCCESS)
			goto err;
		/* send Node Id */
		if ((ret = diswsi(stream, pjob->ji_nodeid)) != DIS_SUCCESS)
			goto err;
		if ((ret = diswst(stream, pjob->ji_wattr[(int)JOB_ATR_exec_vnode].at_val.at_str)) != DIS_SUCCESS)
			goto err;
		if (pjob->ji_wattr[(int)JOB_ATR_pset].at_flags & ATR_VFLAG_SET)
			ret = diswst(stream, pjob->ji_wattr[(int)JOB_ATR_pset].at_val.at_str);
		else
			ret = diswst(stream, ""); /* send null string */
		if (ret != DIS_SUCCESS)
			goto err;
	}

	if (!combine_msg)
		dis_flush(stream);
	return 0;

err:
	sprintf(log_buffer, "%s for %s", dis_emsg[ret], "HELLO");
#ifdef WIN32

	if (errno != 10054)
#endif
		log_err(errno, "send_resc_used", log_buffer);
	tpp_close(stream);
	return ret;
}

/**
 * @brief
 *	batch request for log
 *
 * @param[in] request - pointer to batch_request structure
 * @param[in] stream  - connection stream
 *
 * @return Void
 *
 */
void
log_request(struct batch_request *request, int stream)
{
	sprintf(log_buffer, msg_request, request->rq_type, request->rq_user,
		request->rq_host, stream);
	log_event(PBSEVENT_DEBUG2, PBS_EVENTCLASS_REQUEST, LOG_DEBUG, "",
		log_buffer);
}

/**
 * @brief
 *  process_IS_CMD: Create batch request on received IS_CMD message
 *                   and dispatch request.
 *
 *  @param[in] - stream -  connection stream.
 *
 *  @return void
 *
 */
static void
process_IS_CMD(int stream)
{
	int rc;
	struct batch_request *request;
	struct	sockaddr_in	*addr;
	char *msgid = NULL;

	addr = tpp_getaddr(stream);
	if (addr == NULL) {
		sprintf(log_buffer, "Sender unknown");
		log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_REQUEST, LOG_DEBUG, "?", log_buffer);
		return;
	}

	/* in case of IS_CMD there is a unique id passed with each command,
	 * which we need to send back with the reply so server can
	 * match the replies to the requests
	 */
	msgid = disrst(stream, &rc);
	if (!msgid || rc) {
		close(stream);
		return;
	}

	request = alloc_br(0); /* freed when reply sent */
	if (!request) {
		close(stream);
		if (msgid)
			free(msgid);
		return;
	}

	request->rq_conn = stream;
	strcpy(request->rq_host, netaddr(addr));
	request->rq_fromsvr = 1;
	request->prot = PROT_TPP;
	request->tppcmd_msgid = msgid;

	rc = dis_request_read(stream, request);
	if (rc != 0) {
		close(stream);
		free_br(request);
		return;
	}

	log_request(request, stream);

	dispatch_request(stream, request);
}


/**
 * @brief
 *	Send one or the entire set of unacknowledged hook_job_actions
 *	to the server.   If called with a non-null pointer to an action,
 *	that one is sent;  otherwise all in the list are sent.
 *
 *	If only sending one (non-null argument), please note that that item
 *	has already been linked into the list headed by svr_hook_job_actions.
 *
 * @param[in] phjba - specific action to send or null for all
 *
 * @return none
 *
 */
void
send_hook_job_action(struct hook_job_action *phjba)
{
	struct hook_job_action *pka;
	unsigned int            count;
	int			ret;

	if (server_stream == -1) {
		/* no stream to server, ok as item already queued to resend */
		return;
	}

	if (phjba != NULL) {
		/* single new item to send */
		pka = phjba;
		count = 1;
	} else {
		/* resend queued up list of items */
		pka = GET_NEXT(svr_hook_job_actions);
		if (pka == NULL)
			return;	/* none in the list to send */
		count = 0;
		while (pka) {
			++count;
			pka = GET_NEXT(pka->hja_link);
		}
		pka = GET_NEXT(svr_hook_job_actions);
	}

	if ((ret=is_compose(server_stream, IS_HOOK_JOB_ACTION)) != DIS_SUCCESS)
		goto err;

	ret = diswui(server_stream, count);
	if (ret != DIS_SUCCESS)
		goto err;
	while (count--) {
		ret = diswst(server_stream, pka->hja_jid);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswul(server_stream, pka->hja_actid);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswsi(server_stream, pka->hja_runct);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswsi(server_stream, pka->hja_action);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswui(server_stream, pka->hja_huser);
		if (ret != DIS_SUCCESS)
			goto err;
		pka = GET_NEXT(pka->hja_link);
	}
	dis_flush(server_stream);
	return;

err:
	log_err(errno, "send_hook_job_action", (char *)dis_emsg[ret]);
	return;

}
/**
 *  @brief
 * 	Send the vnode changes in 'vnl' to the server via
 * 	hook_requests_to_server() function call, and also
 * 	requests saving 'vnlp' onto the svr_hook_vnl_action list.
 * 	This list will be tracked for an ack from the server, and if
 * 	found, then deletes 'vnl' from the svr_hook_vnl_action_list, and
 * 	frees 'vnl' itself.
 * 	If there's no ack from the server, and communication with the
 * 	server is interrupted, the 'vnl' request would be sent again.
 *
 * @note
 *	Be sure to NULL the value of 'vnl' upon return from this function,
 *	so as to not be referenced again if it later gets freed.
 *
 * @param[in]	vnl	- vnode changes to send.
 *			  This 'vnl' is saved internally inside
 *			  hook_requests_to_server(), to be freed later in
 *			  is_request() under IS_HOOK_ACTION_ACK request
 *			  on an IS_UPDATE_FROM_HOOK/IS_UPDATE_FROM_HOOK2
 *			  acknowledgement.
 * @return	int
 * 		DIS_SUCCESS	- for successful operations.
 * 		!= DIS_SUCCESS	- for failure encountered
 *
 */

int
send_hook_vnl(void *vnl)
{
	struct hook_vnl_action *pvna;
	pbs_list_head pvnalist;
	int		ret;
	vnl_t		*the_vnlp = vnl;

	if ((the_vnlp == NULL) || (the_vnlp->vnl_used == 0))
		/* nothing to send */
		return DIS_SUCCESS;

	pvna = malloc(sizeof(struct hook_vnl_action));
	if (pvna == NULL) {
		log_err(errno, __func__, "malloc");
		return DIS_NOMALLOC;
	}
	CLEAR_HEAD(pvnalist);
	CLEAR_LINK(pvna->hva_link);
	pvna->hva_euser[0] = '\0';
	pvna->hva_actid = hook_action_id++;
	pvna->hva_vnl   = the_vnlp;
	pvna->hva_update_cmd = IS_UPDATE_FROM_HOOK;
	append_link(&pvnalist, &pvna->hva_link, pvna);

	/* The argument of 1 means to save action to */
	/* svr_vnl_actions list for possible resend. */
	ret = hook_requests_to_server(&pvnalist);
	vna_list_free(pvnalist);
	return (ret);
}

/**
 * @brief
 *	Send a checksum report of the various hooks known to the current mom,
 *	if the configuration flag 'report_hook_checksums' is TRUE.
 *
 * @return	int
 * @retval	DIS_SUCCESS	- for successful operation
 * @retval	!= DIS_SUCCESS	- for failure encountered
 *
 */
static int
send_hook_checksums(void)
{
	unsigned int            count;
	hook			*phook;
	int			ret;

	if (!report_hook_checksums)
		return DIS_SUCCESS;

	if (server_stream == -1) {
		/* no stream to server...ok */
		return DIS_SUCCESS;
	}

	phook = (hook *)GET_NEXT(svr_allhooks);
	count = 0;
	while (phook) {
		phook = (hook *)GET_NEXT(phook->hi_allhooks);
		count++;
	}

	if ((ret=is_compose(server_stream, IS_HOOK_CHECKSUMS)) != DIS_SUCCESS)
		goto err;

	ret = diswui(server_stream, count);
	if (ret != DIS_SUCCESS)
		goto err;

	phook = (hook *)GET_NEXT(svr_allhooks);
	while (count--) {
		ret = diswst(server_stream, phook->hook_name);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswul(server_stream, phook->hook_control_checksum);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswul(server_stream, phook->hook_script_checksum);
		if (ret != DIS_SUCCESS)
			goto err;
		ret = diswul(server_stream, phook->hook_config_checksum);
		if (ret != DIS_SUCCESS)
			goto err;
		phook = (hook *)GET_NEXT(phook->hi_allhooks);
	}


	ret = diswul(server_stream, hooks_rescdef_checksum);
	if (ret != DIS_SUCCESS)
		goto err;

	(void)dis_flush(server_stream);

	return DIS_SUCCESS;

err:
	log_err(errno, "send_hook_checksums", (char *)dis_emsg[ret]);
	return (ret);

}

/**
 * @brief
 *	This function will process the cluster addresses from the server stream.
 *
 * @param[in]	stream - the communication stream
 * 
 * @return	int
 * @retval	0: success
 * @retval	!0: Error code
 */
static int
process_cluster_addrs(int stream)
{
	u_long	ipaddr;
	int	i;
	int 	tot = 0;
	int	ret = 0;
	u_long	ipdepth = 0;
	u_long	counter = 0;

	DBPRT(("%s: IS_CLUSTER_ADDRS\n", __func__))
	enable_exechost2 = 1;

	tot = disrui(stream, &ret);
	if (ret != DIS_SUCCESS)
		return ret;

	for (i = 0; i < tot; i++) {
		ipaddr = disrul(stream, &ret);
		if (ret != DIS_SUCCESS)
			break;
		ipdepth = disrul(stream, &ret);
		if (ret != DIS_SUCCESS)
			break;
		counter = ipaddr;
		while (counter <= ipaddr + ipdepth) {
			DBPRT(("%s:\t%ld.%ld.%ld.%ld", __func__,
				(counter & 0xff000000) >> 24,
				(counter & 0x00ff0000) >> 16,
				(counter & 0x0000ff00) >> 8,
				(counter & 0x000000ff)))
			addrinsert(counter++);
			DBPRT(("ipdepth: %lu\n", ipdepth))
		}
	}
	return 0;
}

/**
 * @brief
 *	This handles input coming from another server over a DIS on tpp stream.
 *	Read the stream to get a Inter-Server request.
 *
 * @param[in]	stream - the tpp stream
 * @param[in]	version - protocol version of the incoming connection
 *
 */
void
is_request(int stream, int version)
{
	int			command = 0;
	int			n;
	int			ret = DIS_SUCCESS;
	u_long			ipaddr;
	char			*jobid = NULL;
	struct	sockaddr_in	*addr;
	void			init_addrs();
	job			*pjob;
	FILE		 	*filen = 0;
	extern vnl_t		*vnlp;        		/* vnode list */
	extern vnl_t		*vnlp_from_hook;        /* vnode list updates from hook */
	int			hktype;
	unsigned long		hkseq;
	struct hook_job_action *phjba;
	struct hook_vnl_action *phvna;
	int			need_inv;
	mom_hook_input_t	*phook_input = NULL;
	mom_hook_output_t	*phook_output = NULL;

	DBPRT(("%s: stream %d version %d\n", __func__, stream, version))
	if (version != IS_PROTOCOL_VER) {
		sprintf(log_buffer, "protocol version %d unknown", version);
		log_err(-1, __func__, log_buffer);
		tpp_close(stream);
		return;
	}

	/* check that machine is okay to be a server */
	addr = tpp_getaddr(stream);
	if (addr == NULL) {
		sprintf(log_buffer, "Sender unknown");
		log_err(-1, __func__, log_buffer);
		tpp_close(stream);
		return;
	}
	ipaddr = ntohl(addr->sin_addr.s_addr);

	if (!addrfind(ipaddr)) {
		sprintf(log_buffer, "bad connect from %s", netaddr(addr));
		log_err(PBSE_BADHOST, __func__, log_buffer);
		tpp_close(stream);
		return;
	}

	/* Server can reach out to mom with requests even before mom sending a hello exchange.
	   This is one such occassion. So trigger hello exchange now */
	if (server_stream == -1)
		send_hellosvr(stream);

	command = disrsi(stream, &ret);
	if (ret != DIS_SUCCESS)
		goto err;

	switch (command) {

		case IS_REPLYHELLO:	/* servers return greeting to IS_HELLOSVR */
			DBPRT(("%s: IS_REPLYHELLO, state=0x%x stream=%d\n", __func__,
				internal_state, stream))
			time_delta_hellosvr(MOM_DELTA_RESET);
			need_inv = disrsi(stream, &ret);
			if (ret != DIS_SUCCESS)
				goto err;
			ret = process_cluster_addrs(stream);
			if (ret != 0 && ret != DIS_EOD)
				goto err;

			 /* return a IS_REGISTERMOM followed by an UPDATE or UPDATE2 */

			next_sample_time = min_check_poll;
			if ((ret = is_compose(stream, IS_REGISTERMOM)) != DIS_SUCCESS)
				goto err;
			if ((ret = registermom(stream, 1)) != 0)
				goto err;
			internal_state_update = UPDATE_MOM_STATE;
			if (need_inv) {
				if ((ret = state_to_server(UPDATE_VNODES, 1)) != DIS_SUCCESS)
					goto err;
				sprintf(log_buffer, "ReplyHello from server at %s", netaddr(addr));
			} else {
				if ((ret = state_to_server(UPDATE_MOM_ONLY, 1)) != DIS_SUCCESS)
					goto err;
				sprintf(log_buffer, "ReplyHello (no inventory required) from server at %s", netaddr(addr));
			}
			log_event(PBSEVENT_SYSTEM, PBS_EVENTCLASS_SERVER,  LOG_DEBUG,
					msg_daemonname, log_buffer);
			dis_flush(server_stream);

			if (send_hook_checksums() != DIS_SUCCESS)
				goto err;
			/* send any unacknowledged hook job and vnl action requests */
			send_hook_job_action(NULL);
			hook_requests_to_server(&svr_hook_vnl_actions);
			svr_hook_resend_job_attrs = 1;

			/* send any vnode changes made by */
			/* exechost_startup hook */
			mom_vnlp_report(vnlp_from_hook, "VNLP_FROM_HOOK");
			(void)send_hook_vnl(vnlp_from_hook);
			/* send_hook_vnl() saves 'vnlp_from_hook' internally, */
			/* to be freed later when server acks the request. */
			vnlp_from_hook = NULL;
			mom_recvd_ip_cluster_addrs = 1;
			break;

		case IS_CLUSTER_ADDRS:
			ret = process_cluster_addrs(stream);
			if (ret != 0 && ret != DIS_EOD)
				goto err;
			break;

		case IS_BADOBIT:
			DBPRT(("%s: IS_BADOBIT\n", __func__))
			jobid = disrst(stream, &ret);
			if (ret != DIS_SUCCESS)
				goto err;

			pjob = find_job(jobid);

			/* Allowing only to delete a job that has actually
			 * started (i.e. not in JOB_SUBSTATE_PRERUN), would
			 * avoid the race condition resulting in a hung job:
			 * server force reruns a job which is lingering in
			 * PRERUN state, and an Obit request for the previous
			 * instance of the job is received by the server and
			 * rejected, causing mom to delete the new instance of
			 * the job. If the job has passed the PRERUN stage,
			 * then it would have already synced up with the server
			 * on status, and not end up in this race condition.
		 	 */
			if (pjob && !pjob->ji_hook_running_bg_on && (pjob->ji_qs.ji_substate != JOB_SUBSTATE_PRERUN)) {
				log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_NOTICE, jobid, "Job removed, Server rejected Obit");
				mom_deljob(pjob);
			}
			free(jobid);
			jobid = NULL;
			break;

		case IS_ACKOBIT:
			DBPRT(("%s: IS_ACKOBIT\n", __func__))
			jobid = disrst(stream, &ret);
			if (ret != DIS_SUCCESS)
				goto err;

			log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_JOB, LOG_INFO,
				jobid, "Job exited, Server acknowledged Obit");
			set_job_toexited(jobid);
			free(jobid);
			jobid = NULL;
			break;

		case IS_SHUTDOWN:
			DBPRT(("%s: IS_SHUTDOWN\n", __func__))
			mom_run_state = 0;
			break;

		case IS_DISCARD_JOB:
			jobid = disrst(stream, &ret);
			if (ret != DIS_SUCCESS)
				goto err;
			DBPRT(("%s: IS_DISCARD_JOB %s\n", __func__, jobid))
			n = disrsi(stream, &ret);	/* job's run_version */
			if (ret != DIS_SUCCESS)
				n = -1;			/* default to -1 */
			pjob = find_job(jobid);
			if (pjob) {
				long runver;

				if (pjob->ji_wattr[(int)JOB_ATR_run_version].at_flags & ATR_VFLAG_SET)
					runver = pjob->ji_wattr[(int)JOB_ATR_run_version].at_val.at_long;
				else
					runver = pjob->ji_wattr[(int)JOB_ATR_runcount].at_val.at_long;
				/* a run_version of -1 means any version is to be discarded */
				if ((n == -1) || (runver == n)) {
					log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_JOB,
						LOG_NOTICE,
						pjob->ji_qs.ji_jobid,
						"Job discarded at request of Server");
						if (pjob->ji_hook_running_bg_on) {
							free(jobid);
							jobid = NULL;
							break;
						}
					(void)kill_job(pjob, SIGKILL);
					phook_input = (mom_hook_input_t *)malloc(sizeof(mom_hook_input_t));
					if (phook_input == NULL) {
						log_err(errno, __func__, MALLOC_ERR_MSG);
						goto err;
					}
					mom_hook_input_init(phook_input);
					phook_input->pjob = pjob;
					if ((phook_output = (mom_hook_output_t *)malloc(
						sizeof(mom_hook_output_t))) == NULL) {
							log_err(errno, __func__, MALLOC_ERR_MSG);
							goto err;
					}
					mom_hook_output_init(phook_output);

					if ((phook_output->reject_errcode =
						(int *)malloc(sizeof(int))) == NULL) {
							log_err(errno, __func__, MALLOC_ERR_MSG);
							free(phook_output);
							goto err;
					}
					*(phook_output->reject_errcode) = 0;

					if (mom_process_hooks(HOOK_EVENT_EXECJOB_END,
						PBS_MOM_SERVICE_NAME, mom_host,
						phook_input, phook_output, NULL, 0, 1) == HOOK_RUNNING_IN_BACKGROUND) {
							pjob->ji_hook_running_bg_on = BG_IS_DISCARD_JOB;
							if (pjob->ji_qs.ji_svrflags &
									JOB_SVFLG_HERE)	/* MS */
								(void)send_sisters(pjob,
								IM_DELETE_JOB, NULL);
							free(jobid);
							jobid = NULL;
							break;
						}
					mom_deljob(pjob);
					free(phook_output->reject_errcode);
					free(phook_output);
					free(phook_input);
				}
			}
			if ((ret=is_compose(server_stream, IS_DISCARD_DONE)) != DIS_SUCCESS) {
				free(jobid);
				jobid = NULL;
				goto err;
			}
			if ((ret=diswst(server_stream, jobid)) != DIS_SUCCESS) {
				free(jobid);
				jobid = NULL;
				goto err;
			}
			free(jobid);	/* can be freed now */
			jobid = NULL;
			if ((ret=diswsi(server_stream, n)) != DIS_SUCCESS)
				goto err;
			dis_flush(server_stream);
			break;

		case IS_CMD:
			DBPRT(("%s: IS_CMD\n", __func__))
			process_IS_CMD(stream);
			break;

		case IS_HOOK_ACTION_ACK:
			/* the Server is sending an acknowledgement that it received */
			/* and processed an IS_HOOK_JOB_ACTION request for a job.    */
			/* The Server will send one such per job		     */

			hktype = disrsi(stream, &ret);
			if (ret != DIS_SUCCESS)
				goto err;
			hkseq = disrsi(stream, &ret);
			if (ret != DIS_SUCCESS)
				goto err;

			if (hktype == IS_HOOK_JOB_ACTION) {
				for (phjba = GET_NEXT(svr_hook_job_actions);
					phjba;
					phjba = GET_NEXT(phjba->hja_link)) {
					if (hkseq == phjba->hja_actid) {
						delete_link(&phjba->hja_link);
						free(phjba);
						break;
					}
				}
			} else if ((hktype == IS_UPDATE_FROM_HOOK) ||
			           (hktype == IS_UPDATE_FROM_HOOK2)) {

				for (phvna = GET_NEXT(svr_hook_vnl_actions);
					phvna;
					phvna = GET_NEXT(phvna->hva_link)) {

					if (hkseq == phvna->hva_actid) {
						delete_link(&phvna->hva_link);
						/* save admin vnode changes */
						/* done by various hooks */
						if (phvna->hva_euser [0] == \
									'\0') {

							if ((vnlp != NULL) ||\
						    (vnl_alloc(&vnlp) \
						    		      != NULL)) {
								vnlp->vnl_modtime = time(NULL);
								vn_merge2(vnlp,
									phvna->hva_vnl,
									HOOK_VNL_PERSISTENT_ATTRIBS, NULL);
								mom_vnlp_report(\
							       vnlp, "vnlp");
							}
						}
						vnl_free(phvna->hva_vnl);
						free(phvna);
						break;
					}
				}
			}
			free(jobid);
			jobid = NULL;
			break;

		default:
			sprintf(log_buffer, "unknown command %d sent", command);
			log_err(-1, __func__, log_buffer);
			goto err;
	}

	tpp_eom(stream);
	return;

err:
	/*
	 ** We come here if we got a DIS read error or a protocol
	 ** element is missing.
	 */
	sprintf(log_buffer, "%s from %s", dis_emsg[ret], netaddr(addr));
	log_err(-1, __func__, log_buffer);
	tpp_close(stream);
	if (filen)
		fclose(filen);
	if (jobid)
		free(jobid);

	return;
}

/**
 * @brief
 *	Sends any pending requests to the server related to hooks on tpp stream
 *
 * @par
 *	May be called with:
 *	1. a new linked list in which case each vnl entry is sent to the
 *	   Server and the list entry is relinked into svr_hook_vnl_actions
 *	   where it remains until the update is acknowledged by the Server; OR
 *	2. svr_hook_vnl_actions which is done when a new tPP stream is opened
 *	   by a server on restart or reestablished communications.  In this
 *	   case only the entries in svr_hook_vnl_actions are only resent.
 * @Note
 *	Update is sent if the list of vnl changes is not empty.
 *	Upon any error, the connection to the server_stream is not closed.
 *
 * @param[in]	plist - pointer to head of list of vnl actions to send to Server
 *
 * @return	int
 * 		DIS_SUCCESS	- for successful operations.
 * 		!= DIS_SUCCESS	- for failure encountered
 *
 */
int
hook_requests_to_server(pbs_list_head *plist)
{
	int			resending = 0;
	int			ret;
	struct hook_vnl_action *nxt;
	struct hook_vnl_action *pvna;
	vnl_t		        *pvnlph;
	extern const		char *dis_emsg[];

	if (plist == NULL)
		return (0);	/* nothing to send */

	if (server_stream < 0) {
		/* log but keep going to link the changes to be sent later */
		log_err(errno, __func__, "warning: unable to send hook requests to server: No server_stream! (to be retried)");
	}

	if (plist == &svr_hook_vnl_actions) {
		/* we are resending the vnl lists on svr_hool_vnl_actions */
		/* so we don't need to update modtime or to relink        */
		resending = 1;
	}

	pvna = (struct hook_vnl_action *)GET_NEXT(*plist);
	while (pvna != NULL) {

		nxt = (struct hook_vnl_action *)GET_NEXT(pvna->hva_link);

		if ((pvnlph = pvna->hva_vnl) == NULL) {
			/* nothing to send, get rid of it */
			delete_link(&pvna->hva_link);
			free(pvna);
			pvna = nxt;
			continue;
		}

		/* We have hook changes to the vnodes to send to the Server */

		if (resending == 0) {


			/* relink them into the main list of "outstanding" */
			/* changes sent to the server */
			delete_link(&pvna->hva_link);
			append_link(&svr_hook_vnl_actions, &pvna->hva_link, pvna);
			pvna->hva_actid = ++hook_action_id;

			/*
			 * Put in a legit vnl_modtime value; otherwise, garbage
			 * value could be sent, causing pbs_server to panic with
			 * "Input value too large" upon vn_decode_DIS()
			 */
			pvnlph->vnl_modtime = time(NULL);
		}

		/* Now send each update to the Server if we can */
		if (server_stream == -1) {
			pvna = nxt;	/* next set of vnl changes */
			continue;
		}

		ret = is_compose(server_stream, pvna->hva_update_cmd);
		if (ret != DIS_SUCCESS)
			goto hook_requests_to_server_err;

		ret = diswul(server_stream, pvna->hva_actid);
		if (ret != DIS_SUCCESS)
			goto hook_requests_to_server_err;

		ret = diswst(server_stream, pvna->hva_euser);
		if (ret != DIS_SUCCESS)
			goto hook_requests_to_server_err;

		ret = vn_encode_DIS(server_stream, pvnlph);	/* vnode list */
		if (ret != DIS_SUCCESS)
			goto hook_requests_to_server_err;

		dis_flush(server_stream);

		pvna = nxt;	/* next set of vnl changes */
	}

	return 0;

hook_requests_to_server_err:
	log_err(errno, __func__, (char *)dis_emsg[ret]);
	return (ret);
}

/**
 * @brief
 * 	state_to_server() - if UPDATE_MOM_STATE is set, send state update message to
 *	the server.
 *
 * @param[in]	what_to_update - defines what to update
 * 		UPDATE_VNODES - update all the vnodes
 *		UPDATE_MOM_ONLY - update only the info about the mom
 * @param[in]	combine_msg	- combine message in the caller.
 *
 *	If we have placement set information to send, we use IS_UPDATE2;
 *	otherwise, we fall back to IS_UPDATE.
 *
 * @return int
 * @retval	0: success
 * @retval	1: failure
 *
 */
int
state_to_server(int what_to_update, int combine_msg)
{
	int			i, ret;
	extern const char *dis_emsg[];
	extern vnl_t		*vnlp;				/* vnode list */
	char			*pv;
	int			use_UPDATE2 = 0;
	int			cmd = IS_UPDATE;

	if (internal_state_update == 0)
		return 0;

	if (server_stream < 0)
		return -1;

	if (av_phy_mem == 0)
		av_phy_mem = strTouL(physmem(0), &pv, 10);

	i = internal_state & MOM_STATE_MASK;
	if (internal_state & (MOM_STATE_BUSYKB | MOM_STATE_INBYKB))
		i |= MOM_STATE_BUSY;
	if (cycle_harvester == 1)
		i |= MOM_STATE_CONF_HARVEST;

	DBPRT(("updating state 0x%x to server\n", i))

	if ((vnlp != NULL) && (what_to_update == UPDATE_VNODES)) {
		use_UPDATE2 = 1;
		cmd = IS_UPDATE2;
	}

	if (!combine_msg)
		if ((ret = is_compose(server_stream, cmd)) != DIS_SUCCESS)
			goto err;

	if ((ret = diswui(server_stream, i)) != DIS_SUCCESS)		/* node state */
		goto err;
	if ((ret = diswui(server_stream, num_pcpus)) != DIS_SUCCESS) /* phy cpus */
		goto err;
	if ((ret = diswui(server_stream, num_acpus)) != DIS_SUCCESS) /* avail cpus */
		goto err;
	if ((ret = diswull(server_stream, av_phy_mem)) != DIS_SUCCESS) /* phy mem */
		goto err;
	if ((ret = diswst(server_stream, arch(0))) != DIS_SUCCESS)	/* arch type */
		goto err;

	if (use_UPDATE2) {
#if	MOM_ALPS
		/*
		 * This is a workaround for a problem with the reporting of
		 * vnodes by multiple MoMs:  the "check_other_moms_time"
		 * variable's value being nonzero results in the vnl_modtime
		 * for additional MoMs' vnodes being set to match the modtime
		 * for the first one to report.  This in turn causes the call
		 * to update2_to_vnode() to be skipped in the case of additional
		 * MoMs because they are still reporting the old time (the one
		 * recorded when inventory_to_vnodes() created the vnodes.
		 *
		 * The fact that update2_to_vnode() is skipped means that the
		 * ATTR_NODE_TopologyInfo action function is not called and as
		 * a result, the other MoMs don't acquire socket licenses.
		 *
		 * This workaround makes sure that in response to an IS_HELLO
		 * from the server, a Cray always reports current time as the
		 * vnode mod time.
		 */
		vnlp->vnl_modtime = time(0);
#endif	/* MOM_ALPS */

		if ((ret = vn_encode_DIS(server_stream, vnlp)) != DIS_SUCCESS)	/* vnode list */
			goto err;
	}

	if ((ret = diswst(server_stream, PBS_VERSION)) != DIS_SUCCESS)	/* pbs_version */
		goto err;

	if (!combine_msg)
		dis_flush(server_stream);
	internal_state_update = 0;
	return 0;

err:
	log_err(errno, "state_to_server", (char *)dis_emsg[ret]);
	tpp_close(server_stream);
	server_stream = -1;
	return ret;
}

/**
 * @brief
 * 	Send the amount of resouces used by jobs to the server
 *	This function used to encode and send the data for IS_RESCUSED,
 *	IS_JOBOBIT, IS_RESCUSED_FROM_HOOK.
 * @param[in]	cmd	- communication command to use
 * @param[in]	count	- number of  jobs to update.
 * @param[in]	rud	- input structure containing info about the jobs,
 *			  resources used, etc...
 *
 * @note
 *	If cmd is IS_RESCUSED_FROM_HOOK and there's an error communicating
 *	to the server, the server_stream connection is not closed automatically.
 *	It's possible it could be a transient error, and this function may
 *	have been called from a child mom. Closing the server_stream would
 *	cause the server to see mom as down.
 *
 * @return Void
 *
 */

void
send_resc_used(int cmd, int count, struct resc_used_update *rud)
{
	int	ret;

	if (count == 0 || rud == NULL || server_stream < 0)
		return;
	DBPRT(("send_resc_used update to server on stream %d\n", server_stream))

	ret = is_compose(server_stream, cmd);
	if (ret != DIS_SUCCESS)
		goto err;

	ret = diswui(server_stream, count);
	if (ret != DIS_SUCCESS)
		goto err;

	while (rud) {
		ret = diswst(server_stream, rud->ru_pjobid);
		if (ret != DIS_SUCCESS)
			goto err;

		if (rud->ru_comment) {
			/* non-null comment: send "1" followed by comment */
			ret = diswsi(server_stream, 1);
			if (ret != DIS_SUCCESS)
				goto err;
			ret = diswst(server_stream, rud->ru_comment);
			if (ret != DIS_SUCCESS)
				goto err;
		} else {
			/* null comment: send "0" */
			ret = diswsi(server_stream, 0);
			if (ret != DIS_SUCCESS)
				goto err;
		}
		ret =diswsi(server_stream, rud->ru_status);
		if (ret != DIS_SUCCESS)
			goto err;

		ret = diswsi(server_stream, rud->ru_hop);
		if (ret != DIS_SUCCESS)
			goto err;

		ret = encode_DIS_svrattrl(server_stream,
			(svrattrl *)GET_NEXT(rud->ru_attr));
		if (ret != DIS_SUCCESS)
			goto err;

		rud = rud->ru_next;
	}
	dis_flush(server_stream);
	return;

err:
	sprintf(log_buffer, "%s for %d", dis_emsg[ret], cmd);
#ifdef WIN32
	if (errno != 10054)
#endif
		log_err(errno, "send_resc_used", log_buffer);

	if (cmd != IS_RESCUSED_FROM_HOOK) {
		tpp_close(server_stream);
		server_stream = -1;
	}
	return;
}

/**
 * @brief
 * 	send_wk_job_idle - send IDLE message to server for each job suspended/resumed
 *	on the workstation going busy/idle.
 *
 * @param[in] idle   suspend/reusme (1/0)
 * @param[in] jobid  job id
 *
 * @return Void
 *
 */
void
send_wk_job_idle(char *jobid, int idle)
{
	int	ret;

	if (server_stream < 0)
		return;

	ret = is_compose(server_stream, IS_IDLE);
	if (ret != DIS_SUCCESS)
		goto err;

	ret = diswui(server_stream, idle);
	if (ret != DIS_SUCCESS)
		goto err;
	ret = diswst(server_stream, jobid);
	if (ret != DIS_SUCCESS)
		goto err;
	dis_flush(server_stream);
	return;

err:
	sprintf(log_buffer, "%s for %d", dis_emsg[ret], idle);
	log_err(errno, "send_wk_job_idle", log_buffer);
	tpp_close(server_stream);
	server_stream = -1;
	return;
}

/**
 * @brief
 * 	recover_vmap - recover the vnode to host mapping data from
 *	the mom_priv/vnodemap file.   See resmom/mom_server.c
 *	is_request() function where it is written.
 *
 *	Format of the file is:
 *		integer time stamp
 *		hostname port num_of_vnodes
 *			vnode_name vhost no_task_value
 *			vnode_name vhost no_task_value
 *			...
 *		hostname ...
 *			...
 *	Note:  if "vhost" is '-', then we use the hostname of Mom, "hostname"
 *
 * @return   int
 * @retval   errno  Failure
 * @retval   0      Success
 *
 */
int
recover_vmap(void)
{
	char		  buf[PBS_MAXHOSTNAME+64];
	char	 	 *endp;
	mominfo_time_t	  maptime = {0, 0};
	int		  n;
	char		  name[PBS_MAXHOSTNAME+1];
	int		  notask;
	mominfo_t	 *pmom;
	unsigned short	  port;
	char		 *str;
	FILE		 *vmf;
	char		  vmapfile[MAXPATHLEN+1];
	char		  vhost[PBS_MAXHOSTNAME+1];
	extern	char	 *skipwhite(char *);
	extern  char	 *wtokcpy(char *, char *, int);

	sprintf(vmapfile,    "%s/%s", mom_home, VNODE_MAP);
	vmf = fopen(vmapfile, "r");
	if (vmf == NULL) {
		if (errno == ENOENT)
			return 0;
		else
			return errno;
	}

	if (fgets(buf, sizeof(buf), vmf) == NULL)
		return 0;
	str = buf;
	while (isdigit(*str))
		str++;
	if ((*str != '\n') && (*str != '.')) {
		fclose(vmf);
		return PBSE_BADTSPEC;
	}
	/* record time stamp of vmap data */
	sscanf(buf, "%lu.%d", &maptime.mit_time, &maptime.mit_gen);

	while (fgets(buf, sizeof(buf), vmf)) {
		str = skipwhite(buf);	/* pass over initial whitespace */
		if (*str == '\0')
			continue;
		str = wtokcpy(str, name, sizeof(name));
		str = skipwhite(str);
		if (*str == '\0')
			continue;
		port = (unsigned short)strtol(str, &endp, 10);
		str = skipwhite(endp);
		if (*str == '\0')
			continue;
		n = (int)strtol(str, &endp, 10);

		pmom = create_mom_entry(name, (unsigned int)port);

		while (n--) {
			if (fgets(buf, sizeof(buf), vmf) == NULL)
				break;
			str = skipwhite(buf);
			if (*str == '\0')
				break;
			str = wtokcpy(str, name, sizeof(name));
			str = skipwhite(str);
			if (*str == '\0')
				break;
			str = wtokcpy(str, vhost, sizeof(vhost));
			str = skipwhite(str);
			notask = (int)strtol(str, &endp, 10);

			if ((vhost[0] == '-') && (vhost[1] == '\0'))
				vhost[0] = '\0';	/* make null str */
			if (create_mommap_entry(name, vhost, pmom, notask) == NULL)
				break;
		}
		if (n > 0) {
			free_vnodemap();
			fclose(vmf);
			return PBSE_INTERNAL;
		}
	}
	mominfo_time = maptime;
	fclose(vmf);
	return 0;
}

/**
 * @brief
 *	Send a message on tpp stream to the Server asking that it tell the Scheduler
 *	to restart it's scheduling cycle.
 * @par
 *	If this message is lost due to a closed stream to the Server, so be it.
 *	The world will likely have likely changed by the time a new connection
 *	is established.
 *
 * @param[in] hook_user - effective user running hook,  null string if PBSADMIN
 *
 * @return int
 * @retval 0 - message queued on stream.
 * @retval non-zero - DIS error, stream may be closed.
 *
 */
int
send_sched_recycle(char *hook_user)
{
	int ret;
	ret = is_compose(server_stream, IS_HOOK_SCHEDULER_RESTART_CYCLE);
	if (ret != DIS_SUCCESS)
		goto recycle_err;
	ret = diswst(server_stream, hook_user);
	if (ret != DIS_SUCCESS)
		goto recycle_err;
	ret = dis_flush(server_stream);
	if (ret != DIS_SUCCESS)
		goto recycle_err;
	return (0);

recycle_err:
	sprintf(log_buffer, "%s error %s",
		"Failed to contact server for sched recycle",
		dis_emsg[ret]);
	log_err(-1, __func__, log_buffer);
	return (ret);
}
