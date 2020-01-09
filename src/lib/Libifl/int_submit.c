/*
 * Copyright (C) 1994-2018 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *
 * This file is part of the PBS Professional ("PBS Pro") software.
 *
 * Open Source License Information:
 *
 * PBS Pro is free software. You can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Commercial License Information:
 *
 * For a copy of the commercial license terms and conditions,
 * go to: (http://www.pbspro.com/UserArea/agreement.html)
 * or contact the Altair Legal Department.
 *
 * Altair’s dual-license business model allows companies, individuals, and
 * organizations to create proprietary derivative works of PBS Pro and
 * distribute them - whether embedded or bundled with other software -
 * under a commercial license agreement.
 *
 * Use of Altair’s trademarks, including but not limited to "PBS™",
 * "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
 * trademark licensing policies.
 *
 */
/**
 * @file	int_submit.c
 */
#include <pbs_config.h>   /* the master config generated by configure */

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#ifndef WIN32
#include <stdint.h>
#endif
#include "portability.h"
#include "libpbs.h"
#include "dis.h"
#include "rpp.h"
#include "net_connect.h"




/**
 * @brief - Start a standard inter-server message.
 *
 * @param[in] stream  - The RPP stream on which to send message
 * @param[in] command - The message type (cmd) to encode
 *
 * @return error code
 * @retval  DIS_SUCCESS - Success
 * @retval !DIS_SUCCESS - Failure
 */
int
is_compose(int stream, int command)
{
	int	ret;

	if (stream < 0)
		return DIS_EOF;
	DIS_rpp_reset();

	ret = diswsi(stream, IS_PROTOCOL);
	if (ret != DIS_SUCCESS)
		goto done;
	ret = diswsi(stream, IS_PROTOCOL_VER);
	if (ret != DIS_SUCCESS)
		goto done;
	ret = diswsi(stream, command);
	if (ret != DIS_SUCCESS)
		goto done;

	return DIS_SUCCESS;

done:
	return ret;
}

/**
 * @brief - Get a unique id each time this function is called
 *
 * @par NOTE:
 *	This id is used as a message id in every command sent out from
 * 	this daemon. This is done to match replies to asynchronous
 * 	command sends to the replies that we receive later
 *
 * @param[out] id - The return msgid created
 *
 * @return error code
 * @retval  DIS_SUCCESS  - Success
 * @retval  DIS_NOMALLOC - Failure
 */
int
get_msgid(char **id)
{
	char msgid[MAXNAMLEN];

	static time_t last_time = -1;
	static int counter = 0;
	time_t now = time(NULL);

	if (now != last_time) {
		counter = 0;
		last_time = now;
	} else {
		counter++;
	}
#ifdef WIN32
	sprintf(msgid, "%ld:%d", now, counter);
#else
	sprintf(msgid, "%ju:%d", (uintmax_t)now, counter);
#endif
	if ((*id = strdup(msgid)) == NULL)
		return DIS_NOMALLOC;

	return DIS_SUCCESS;
}

/**
 * @brief - Compose a RPP command
 *
 * @par Functionality:
 *	calls im_compose to create the message header, get_msgid to
 * 	add a msg id to the header (unless one is passed)
 *
 * @param[in] stream - rpp stream to write to
 * @param[in] command - The command to encode
 * @param[in,out] ret_msgid - The msgid, if passed to this function, is
 *                            the msgid to be used for this message.
 *                            If msgid is not passed, then create a unique
 *                            msgid and set for the message, also return it
 *                            back to caller.
 *
 * @return error code
 * @retval  DIS_SUCCESS - Success
 * @retval !DIS_SUCCESS - Failure
 */
int
is_compose_cmd(int stream, int command, char **ret_msgid)
{
	int ret;

	if ((ret = is_compose(stream, command)) != DIS_SUCCESS)
		return ret;

	if (ret_msgid == NULL || *ret_msgid == NULL || *ret_msgid[0] == '\0') /* NULL or empty id provided */
		if ((ret = get_msgid(ret_msgid)) != 0)
			return ret;

	if ((ret = diswst(stream, *ret_msgid)) != DIS_SUCCESS)
		return ret;

	return DIS_SUCCESS;
}

/**
 * @brief
 *	-PBSD_rdytocmt This function does the Ready To Commit sub-function of
 *	the Queue Job request.
 *
 * @param[in] connect - socket fd
 * @param[in] jobid - job identifier
 * @param[in] rpp - indication for rpp to use or not
 * @param[in] msgid - message id
 *
 * @return	int
 * @retval	0		success
 * @retval	!0(pbs_errno)	failure
 *
 */

int
PBSD_rdytocmt(int connect, char *jobid, int rpp, char **msgid)
{
	int     rc;
	struct batch_reply *reply;
	int     sock;

	if (!rpp) {
		sock = connection[connect].ch_socket;
		DIS_tcp_setup(sock);
	} else {
		sock = connect;
		if ((rc = is_compose_cmd(sock, IS_CMD, msgid)) != DIS_SUCCESS)
			return rc;
	}

	if ((rc=encode_wire_ReqHdr(sock, PBS_BATCH_RdytoCommit, pbs_current_user)) ||
		(rc = encode_wire_JobId(sock, jobid))  ||
		(rc = encode_wire_ReqExtend(sock, NULL))) {
		if (!rpp) {
			connection[connect].ch_errtxt = strdup(dis_emsg[rc]);
			if (connection[connect].ch_errtxt == NULL)
				return (pbs_errno = PBSE_SYSTEM);
		}
		return (pbs_errno = PBSE_PROTOCOL);
	}

	if (rpp) {
		pbs_errno = PBSE_NONE;
		if (rpp_flush(sock))
			pbs_errno = PBSE_PROTOCOL;
		return pbs_errno;
	}

	if (DIS_tcp_wflush(sock))
		return (pbs_errno = PBSE_PROTOCOL);

	/* read reply */

	reply = PBSD_rdrpy(connect);

	PBSD_FreeReply(reply);

	return connection[connect].ch_errno;
}

/**
 * @brief
 *	-PBS_commit.c This function does the Commit sub-function of
 *	the Queue Job request.
 *
 * @param[in] connect - socket fd
 * @param[in] jobid - job identifier
 * @param[in] rpp - indication for rpp to use or not
 * @param[in] msgid - message id
 *
 * @return      int
 * @retval      0               success
 * @retval      !0(pbs_errno)   failure
 *
 */

char *
PBSD_commit(int connect, char *jobid, int rpp, char **msgid)
{
	struct batch_reply *reply;
	int rc;
	int sock;
	char * return_jobid = NULL;

	if (!rpp) {
		sock = connection[connect].ch_socket;
		DIS_tcp_setup(sock);
	} else {
		sock = connect;
		if ((rc = is_compose_cmd(sock, IS_CMD, msgid)) != DIS_SUCCESS)
			return NULL;
	}

	if ((rc = encode_wire_ReqHdr(sock, PBS_BATCH_Commit, pbs_current_user)) ||
		(rc = encode_wire_JobId(sock, jobid)) ||
		(rc = encode_wire_ReqExtend(sock, NULL))) {
		if (!rpp) {
			connection[connect].ch_errtxt = strdup(dis_emsg[rc]);
			if (connection[connect].ch_errtxt == NULL) {
				pbs_errno = PBSE_SYSTEM;
				return NULL;
			}
		}
		pbs_errno = PBSE_PROTOCOL;
		return NULL;
	}

	if (rpp) {
		pbs_errno = PBSE_NONE;
		if (rpp_flush(sock))
			pbs_errno = PBSE_PROTOCOL;
		return NULL;
	}

	if (DIS_tcp_wflush(sock)) {
		pbs_errno = PBSE_PROTOCOL;
		return NULL;
	}

	reply = PBSD_rdrpy(connect);
	if (reply == NULL) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (reply->brp_choice &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Text &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Commit) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (connection[connect].ch_errno == 0) {
		return_jobid = strdup(reply->brp_un.brp_jid);
		if (return_jobid == NULL) {
			pbs_errno = PBSE_SYSTEM;
		}
	}

	PBSD_FreeReply(reply);

	return return_jobid;
}

/**
 * @brief
 *	-PBS_scbuf.c Send a chunk of a of the job script to the server.
 *	Called by pbs_submit.  The buffer length could be
 *	zero; the server should handle that case...
 *
 * @param[in] c - connection handle
 * @param[in] reqtype - request type
 * @param[in] seq - file chunk sequence number
 * @param[in] buf - file chunk
 * @param[in] len - length of chunk
 * @param[in] jobid - ob id (for types 1 and 2 only)
 * @param[in] which - standard file type (enum)
 * @param[in] rpp - indication for rpp to use or not
 * @param[in] msgid - message id
 *
 * @return      int
 * @retval      0               success
 * @retval      !0(pbs_errno)   failure
 *
 */

static int
PBSD_scbuf(int c, int reqtype, int seq, char *buf, int len, char *jobid,
		enum job_file which, int rpp, char **msgid)
{
	struct batch_reply   *reply;
	int	rc;
	int	sock;

	if (!rpp) {
		sock = get_svr_shard_connection(c, reqtype, NULL);
		if (sock == -1) {
			return (pbs_errno = PBSE_NOSERVER);
		}
		DIS_tcp_setup(sock);
	} else {
		sock = c;
		if ((rc = is_compose_cmd(sock, IS_CMD, msgid)) != DIS_SUCCESS)
			return rc;
	}

	if (jobid == NULL)
		jobid = "";	/* use null string for null pointer */

	if ((rc = encode_wire_ReqHdr(sock, reqtype, pbs_current_user)) ||
		(rc = encode_wire_JobFile(sock, seq, buf, len, jobid, which)) ||
		(rc = encode_wire_ReqExtend(sock, NULL))) {
		if (!rpp) {
			connection[c].ch_errtxt = strdup(dis_emsg[rc]);
			if (connection[c].ch_errtxt == NULL)
				return (pbs_errno = PBSE_SYSTEM);
		}
		return (pbs_errno = PBSE_PROTOCOL);
	}

	if (rpp) {
		pbs_errno = PBSE_NONE;
		if (rpp_flush(sock))
			pbs_errno = PBSE_PROTOCOL;
		return pbs_errno;
	}

	if (DIS_tcp_wflush(sock)) {
		return (pbs_errno = PBSE_PROTOCOL);
	}

	/* read reply */

	reply = PBSD_rdrpy(c);

	PBSD_FreeReply(reply);

	return connection[c].ch_errno;
}

/**
 * @brief
 *	-The Job File function used to move files related to
 *	a job between servers.
 *	-- the function PBS_scbuf is called repeatedly to
 *	transfer chunks of the script to the server.
 *
 * @param[in] c - connection handle
 * @param[in] script_file - job file
 * @param[in] rpp - indication for rpp to use or not
 * @param[in] msgid - message id
 *
 * @return	int
 * @retval	0	success
 * @retval	-1	failure
 *
 */

int
PBSD_jscript(int c, char *script_file, int rpp, char **msgid)
{
	int i;
	int fd;
	int cc;
	char s_buf[SCRIPT_CHUNK_Z];
	int rc = 0;

	if ((fd = open(script_file, O_RDONLY, 0)) < 0) {
		return (-1);
	}
	i = 0;
	cc = read(fd, s_buf, SCRIPT_CHUNK_Z);
	while ((cc > 0) &&
		((rc = PBSD_scbuf(c, PBS_BATCH_jobscript, i, s_buf, cc, NULL, JScript, rpp, msgid)) == 0)) {
		i++;
		cc = read(fd, s_buf, SCRIPT_CHUNK_Z);
	}

	close(fd);
	if (cc < 0)	/* read failed */
		return (-1);

	if (rpp)
		return (rc);

	return connection[c].ch_errno;
}

/**
 * @brief
 *	job file function for moving file between server/mom
 *
 * @param[in] c - connection handle
 * @param[in] script_file - job file
 * @param[in] rpp - indication for rpp to use or not
 * @param[in] msgid - message id
 *
 * @return      int
 * @retval      0       success
 * @retval      -1      failure
 *
 */

int
PBSD_jscript_direct(int c, char *script, int rpp, char **msgid)
{
	int rc;
	int tosend;
	int i = 0;
	char *p = script;
	int len;

	if (script == NULL) {
		pbs_errno = PBSE_INTERNAL;
		return -1;
	}

	len = strlen(script);
	do {
		tosend = (len > SCRIPT_CHUNK_Z) ? SCRIPT_CHUNK_Z : len;
		rc = PBSD_scbuf(c, PBS_BATCH_jobscript, i, p, tosend, NULL, JScript, rpp, msgid);
		i++;
		p += tosend;
		len -= tosend;
	} while ((rc == 0) && (len > 0));

	if (rpp)
		return (rc);

	return connection[c].ch_errno;
}


/**
 * @brief
 *	-PBS_jobfile.c
 *	The Job File function used to move files related to
 *	a job between servers.
 *	-- the function PBS_scbuf is called repeatedly to
 *	transfer chunks of the script to the server.
 *
 * @param[in] c - connection handle
 * @param[in] reqtype - request type
 * @param[in] path - file path
 * @param[in] jobid - job id
 * @param[in] which - standard file type (enum)
 * @param[in] rpp - indication for rpp to use or not
 * @param[in] msgid - message id
 *
 * @return      int
 * @retval      0       success
 * @retval      -1      failure
 *
 */

int
PBSD_jobfile(int c, int req_type, char *path, char *jobid,
		enum job_file which, int rpp, char **msgid)
{
	int   i;
	int   cc;
	int   fd;
	char  s_buf[SCRIPT_CHUNK_Z];
	int rc = 0;

	if ((fd = open(path, O_RDONLY, 0)) < 0) {
		return (-1);
	}
	i = 0;
	cc = read(fd, s_buf, SCRIPT_CHUNK_Z);
	set_new_shard_context(c);
	while ((cc > 0) &&
		((rc = PBSD_scbuf(c, req_type, i, s_buf, cc, jobid, which, rpp, msgid)) == 0)) {
		i++;
		cc = read(fd, s_buf, SCRIPT_CHUNK_Z);
	}

	close(fd);
	if (cc < 0)	/* read failed */
		return (-1);

	if (rpp)
		return rc;

	return connection[c].ch_errno;
}

/**
 * @brief
 *	-PBS_queuejob.c
 *	This function sends the first part of the Queue Job request
 *
 * @param[in] c - socket descriptor
 * @param[in] jobid - job identifier
 * @param[in] destin - destination name
 * @param[in] attrib - pointer to attribute list
 * @param[in] extend - extention string for req encode
 * @param[in] rpp - indication for rpp protocol
 * @param[in] msgid - message id
 *
 * @return      int
 * @retval      0               Success
 * @retval      pbs_error(!0)   error
 */

char *
PBSD_queuejob(int connect, char *jobid, char *destin, struct attropl *attrib, char *extend, int rpp, char **msgid, int *commit_done)
{
	struct batch_reply *reply;
	char  *return_jobid = NULL;
	int    rc;
	int    sock;
	void   *buf;
	ns(PBS_Header_ref_t) hdr_ref;
	ns(PBS_QueuejobReq_ref_t) quejob_ref;
	ns(PBS_Extend_ref_t) ext_ref;

	*commit_done = 0;

	if (!rpp) {
		sock = get_svr_shard_connection(connect, PBS_BATCH_QueueJob, NULL);
		if (sock == -1) {
			pbs_errno = PBSE_NOSERVER;
			return NULL;
		}
		DIS_tcp_setup(sock);
	} else {
		sock = connect;
		if ((rc = is_compose_cmd(sock, IS_CMD, msgid)) != DIS_SUCCESS) {
			pbs_errno = PBSE_PROTOCOL;
			return return_jobid;
		}
	}

	/* get buffer here */
	buf = get_encode_buffer(connect);

	/* first, set up the body of the Queue Job request */
	hdr_ref = encode_wire_ReqHdr(buf, PBS_BATCH_QueueJob, pbs_current_user);
	quejob_ref = encode_wire_QueueJob(buf, jobid, destin, attrib);
	ext_ref = encode_wire_ReqExtend(buf, extend);

	if (hdr_ef == 0 || quejob_ref == 0 || ext_ref == 0) {
		if (!rpp) {
			connection[connect].ch_errtxt = strdup("Encoding error");
			if (connection[connect].ch_errtxt == NULL) {
				pbs_errno = PBSE_SYSTEM;
			} else {
				pbs_errno = PBSE_PROTOCOL;
			}
		}
		return return_jobid;
	}

	if (rpp) {
		pbs_errno = PBSE_NONE;
		if (rpp_flush(sock))
			pbs_errno = PBSE_PROTOCOL;

		return (""); /* return something NON-NULL for rpp */
	}

	if (DIS_tcp_wflush(sock)) { // get the encoded buffer and flush to transport
		pbs_errno = PBSE_PROTOCOL;
		return return_jobid;
	}

	/* read reply from stream into presentation element */

	reply = PBSD_rdrpy(connect);
	if (reply == NULL) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (reply->brp_choice &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Text &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Queue &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Commit) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (connection[connect].ch_errno == 0) {
		return_jobid = strdup(reply->brp_un.brp_jid);
		if (return_jobid == NULL) {
			pbs_errno = PBSE_SYSTEM;
		} else if (reply->brp_choice == BATCH_REPLY_CHOICE_Commit) {
			*commit_done = 1;
		}
	}

	PBSD_FreeReply(reply);
	return return_jobid;
}
