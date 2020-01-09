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
 * @file	dec_QueueJob.c
 * @brief
 * 	decode_wire_QueueJob() - decode a Queue Job Batch Request
 *
 * @par Data items are:
 * 			string	job id
 *			string	destination
 *			list of attributes (attropl)
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
 * @brief -
 *	decode a Queue Job Batch Request
 *
 * @par	Functionality:
 *		string  job id\n
 *		string  destination\n
 *		list of attributes (attropl)
 *
 * @param[in] sock - socket descriptor
 * @param[out] preq - pointer to batch_request structure
 *
 * @return      int
 * @retval      DIS_SUCCESS(0)  success
 * @retval      error code      error
 *
 */

int
decode_wire_QueueJob(void *buf, struct batch_request *preq)
{
	int rc;

	CLEAR_HEAD(preq->rq_ind.rq_queuejob.rq_attr);

	PBS_QueuejobReq_table_t rq = buf; /* typecast preq to quejob req table type */

	flatbuffers_string_t jobid = PBS_QueuejobReq_jobId(rq);
	strncpy(preq->rq_ind.rq_queuejob.rq_jid, PBS_MAXSVRJOBID, jobid);

	rc = disrfst(sock, PBS_MAXSVRJOBID+1, preq->rq_ind.rq_queuejob.rq_jid);
	if (rc) return rc;

	rc = disrfst(sock, PBS_MAXSVRJOBID+1, preq->rq_ind.rq_queuejob.rq_destin);
	if (rc) return rc;

	return (decode_wire_svrattrl(sock, &preq->rq_ind.rq_queuejob.rq_attr));
}


/**
 * @brief
 *	-encode a Queue Job Batch Request
 *
 * @par	Functionality:
 *		This request is used for the first step in submitting a job, sending
 *      	the job attributes.
 *
 * @param[in] sock - socket descriptor
 * @param[in] jobid - job id
 * @param[in] destin - destination queue name
 * @param[in] aoplp - pointer to attropl structure(list)
 * @return      int
 * @retval      DIS_SUCCESS(0)  success
 * @retval      error code      error
 *
 */
flatbuffers_ref_t
encode_wire_QueueJob(void *buf, char *jobid, char *destin, struct attropl *aoplp)
{
	flatcc_builder_t *B = (flattcc_builder_t *) buf;
	ns(Attribute_vec_ref_t) attrs;

	if (jobid == NULL)
		jobid = "";
	if (destin == NULL)
		destin = "";

	flatbuffers_string_ref_t jobid = flatbuffers_string_create_str(B, jobid);
	flatbuffers_string_ref_t destin = flatbuffers_string_create_str(B, destin);

	attrs =  encode_wire_attropl(buf, aoplp);

	return (ns(PBS_QueuejobReq_create(B, jobid, destin, attrs, extend)); 
}
