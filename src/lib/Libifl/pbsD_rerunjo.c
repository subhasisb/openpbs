/*
 * Copyright (C) 1994-2020 Altair Engineering, Inc.
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
 * @file	pbs_rerunjob.c
 * @brief
 * This function does the RerunJob request.
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <string.h>
#include <stdio.h>
#include "libpbs.h"
#include "dis.h"
#include "pbs_ecl.h"


/**
 * @brief
 *	-send rerun batch request
 *
 * @param[in] c - connection handler
 * @param[in] jobid - job identifier
 * @param[in] extend - string to encode req
 *
 * @return      int
 * @retval      0       success
 * @retval      !0      error
 *
 */

int
__pbs_rerunjob(int c, char *jobid, char *extend)
{
	int	rc;
	struct batch_reply *reply;
	time_t  old_tcp_timeout;
	int	sock;

	if ((jobid == NULL) || (*jobid == '\0'))
		return (pbs_errno = PBSE_IVALREQ);

	/* initialize the thread context data, if not already initialized */
	if (pbs_client_thread_init_thread_context() != 0)
		return pbs_errno;

	/* lock pthread mutex here for this connection */
	/* blocking call, waits for mutex release */
	if (pbs_client_thread_lock_connection(c) != 0)
		return pbs_errno;

	set_new_shard_context(c);
	sock = get_svr_shard_connection(c, PBS_BATCH_Rerun, jobid);
	if (sock == -1) {
		return (pbs_errno = PBSE_NOSERVER);
	}

	/* setup DIS support routines for following DIS calls */

	DIS_tcp_funcs();

	if ((rc = encode_DIS_ReqHdr(sock, PBS_BATCH_Rerun, pbs_current_user)) ||
		(rc = encode_DIS_JobId(sock, jobid)) ||
		(rc = encode_DIS_ReqExtend(sock, extend))) {
		if (set_conn_errtxt(c, dis_emsg[rc]) != 0) {
			pbs_errno = PBSE_SYSTEM;
		} else {
			pbs_errno = PBSE_PROTOCOL;
		}
		(void)pbs_client_thread_unlock_connection(c);
		return pbs_errno;
	}

	/* write data */

	if (dis_flush(sock)) {
		pbs_errno = PBSE_PROTOCOL;
		(void)pbs_client_thread_unlock_connection(c);
		return pbs_errno;
	}

	/* Set timeout value to very long value as rerun request */
	/* goes from Server to Mom and may take a long time      */
	old_tcp_timeout = pbs_tcp_timeout;
	pbs_tcp_timeout = PBS_DIS_TCP_TIMEOUT_VLONG;

	/* read reply from stream into presentation element */

	reply = PBSD_rdrpy(c);

	/* reset timeout */
	pbs_tcp_timeout = old_tcp_timeout;


	PBSD_FreeReply(reply);

	rc = get_conn_errno(c);

	/* unlock the thread lock and update the thread context data */
	if (pbs_client_thread_unlock_connection(c) != 0)
		return pbs_errno;

	return rc;
}
