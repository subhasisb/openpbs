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
 * @file	enc_CpyFil.c
 * @brief
 * encode_wire_CopyFiles() - encode a Copy Files Dependency Batch Request
 *
 *	This request is used by the server ONLY; its input is a server
 *	batch request structure.
 *
 * @par	Data items are:
 * 			string		job id
 *			string		job owner		(may be null)
 *			string		execution user name
 *			string		execution group name	(may be null)
 *			unsigned int	direction & job_dir_enable flag
 *			unsigned int	count of file pairs in set
 *			set of		file pairs:
 *				unsigned int	flag
 *				string		local path name
 *				string		remote path name (may be null)
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <sys/types.h>
#include "libpbs.h"
#include "list_link.h"
#include "server_limits.h"
#include "attribute.h"
#include "credential.h"
#include "batch_request.h"
#include "dis.h"

/**
 * @brief
 *	-encode a Copy Files Dependency Batch Request
 *
 * @param[in] sock - socket descriptor
 * @param[in] preq - pointer to batch_request 
 *
 * @return	int
 * @retval	0	success
 * @retval	!0	error
 *
 */
int
encode_wire_CopyFiles(int sock, struct batch_request *preq)
{
	int   pair_ct = 0;
	char *nullstr = "";
	struct rqfpair *ppair;
	int   rc;

	ppair = (struct rqfpair *)GET_NEXT(preq->rq_ind.rq_cpyfile.rq_pair);
	while (ppair) {
		++pair_ct;
		ppair = (struct rqfpair *)GET_NEXT(ppair->fp_link);
	}

	if ((rc = diswst(sock, preq->rq_ind.rq_cpyfile.rq_jobid) != 0) ||
		(rc = diswst(sock, preq->rq_ind.rq_cpyfile.rq_owner) != 0) ||
		(rc = diswst(sock, preq->rq_ind.rq_cpyfile.rq_user) != 0) ||
		(rc = diswst(sock, preq->rq_ind.rq_cpyfile.rq_group) != 0) ||
		(rc = diswui(sock, preq->rq_ind.rq_cpyfile.rq_dir) != 0))
			return rc;

	if ((rc = diswui(sock, pair_ct) != 0))
		return rc;
	ppair = (struct rqfpair *)GET_NEXT(preq->rq_ind.rq_cpyfile.rq_pair);
	while (ppair) {
		if (ppair->fp_rmt == NULL)
			ppair->fp_rmt = nullstr;
		if ((rc = diswui(sock, ppair->fp_flag) != 0) ||
			(rc = diswst(sock, ppair->fp_local) != 0) ||
			(rc = diswst(sock, ppair->fp_rmt) != 0))
				return  rc;
		ppair = (struct rqfpair *)GET_NEXT(ppair->fp_link);
	}

	return 0;
}

/**
 * @brief
 * 	-encode_wire_CopyFiles_Cred() - encode a Copy Files with Credential Dependency
 *	Batch Request
 *
 * @par Note:
 *	This request is used by the server ONLY; its input is a server
 *	batch request structure.
 *
 * @param[in] sock - socket descriptor
 * @param[in] preq - pointer to batch request
 *
 * @par	Data items are:\n
 *		string		job id\n
 *		string		job owner(may be null)\n
 *		string		execution user name\n
 *		string		execution group name(may be null)\n
 *		unsigned int	direction & job_dir_enable flag\n
 *		unsigned int	count of file pairs in set\n
 *	set of	file pairs:\n
 *		unsigned int	flag\n
 *		string		local path name\n
 *		string		remote path name (may be null)\n
 *		unsigned int	credential type\n
 *		unsigned int	credential length (bytes)\n
 *		byte string	credential\n
 *
 * @return	int
 * @retval	0	success
 * @retval	!0	error
 *
 */
int
encode_wire_CopyFiles_Cred(int sock, struct batch_request *preq)
{
	int			pair_ct = 0;
	char			*nullstr = "";
	struct rqfpair		*ppair;
	int			rc;
	size_t			clen;
	struct	rq_cpyfile	*rcpyf;

	clen = (size_t)preq->rq_ind.rq_cpyfile_cred.rq_credlen;
	rcpyf = &preq->rq_ind.rq_cpyfile_cred.rq_copyfile;
	ppair = (struct rqfpair *)GET_NEXT(rcpyf->rq_pair);

	while (ppair) {
		++pair_ct;
		ppair = (struct rqfpair *)GET_NEXT(ppair->fp_link);
	}

	if ((rc = diswst(sock, rcpyf->rq_jobid) != 0) ||
		(rc = diswst(sock, rcpyf->rq_owner) != 0) ||
		(rc = diswst(sock, rcpyf->rq_user) != 0) ||
		(rc = diswst(sock, rcpyf->rq_group) != 0) ||
		(rc = diswui(sock, rcpyf->rq_dir) != 0))
			return rc;

	if ((rc = diswui(sock, pair_ct) != 0))
		return rc;
	ppair = (struct rqfpair *)GET_NEXT(rcpyf->rq_pair);
	while (ppair) {
		if (ppair->fp_rmt == NULL)
			ppair->fp_rmt = nullstr;
		if ((rc = diswui(sock, ppair->fp_flag) != 0) ||
			(rc = diswst(sock, ppair->fp_local) != 0) ||
			(rc = diswst(sock, ppair->fp_rmt) != 0))
				return  rc;
		ppair = (struct rqfpair *)GET_NEXT(ppair->fp_link);
	}

	rc = diswui(sock, preq->rq_ind.rq_cpyfile_cred.rq_credtype);
	if (rc != 0) return rc;
	rc = diswcs(sock, preq->rq_ind.rq_cpyfile_cred.rq_pcred, clen);
	if (rc != 0) return rc;

	return 0;
}
