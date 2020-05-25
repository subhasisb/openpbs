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
 * @file	int_jcred.c
 *
 * @brief
 * send job credentials to the server.
 * @note
 * This code is not mean to be used by a persistent process.
 * If an error occurs, not all the allocated structures are freed.
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <string.h>
#include <stdio.h>
#include <assert.h>
#include "libpbs.h"
#include "dis.h"
#include "ticket.h"
#include "net_connect.h"
#include "tpp.h"

/**
 * @brief
 *	 encode a Job Credential Batch Request
 *
 * @param[in] c - socket descriptor
 * @param[in] type - credential type
 * @param[in] buf - credentials
 * @param[in] len - credential length
 * @param[in] prot - PROT_TCP or PROT_TPP
 * @param[in] msgid - msg id
 *
 * @return	int
 * @retval	0		success
 * @retval	!0(pbse error)	error
 *
 */
int
PBSD_jcred(int c, int type, char *buf, int len, int prot, char **msgid)
{
	int			rc;
	struct batch_reply	*reply = NULL;
	int	sock;
	int index;

	if (prot == PROT_TCP) {
		sock = get_svr_shard_connection(c, SHARD_UNKNOWN, NULL, &index);
		if (sock == -1) {
			return (pbs_errno = PBSE_NOCONNECTION);
		}
		DIS_tcp_funcs();
	} else {
		sock = c;
		if ((rc = is_compose_cmd(sock, IS_CMD, msgid)) != DIS_SUCCESS)
			return rc;
	}

	if ((rc =encode_DIS_ReqHdr(sock, PBS_BATCH_JobCred, pbs_current_user)) ||
		(rc = encode_DIS_JobCred(sock, type, buf, len)) ||
		(rc = encode_DIS_ReqExtend(sock, NULL))) {
		if (prot == PROT_TCP) {
			if (set_conn_errtxt(c, dis_emsg[rc]) != 0)
				return (pbs_errno = PBSE_SYSTEM);
		}
		return (pbs_errno = PBSE_PROTOCOL);
	}

	pbs_errno = PBSE_NONE;
	if (dis_flush(sock)) {
		return (pbs_errno = PBSE_PROTOCOL);
	}

	if (prot == PROT_TPP) {
		return pbs_errno;
	}

	reply = PBSD_rdrpy(c);

	PBSD_FreeReply(reply);

	return get_conn_errno(c);
}
