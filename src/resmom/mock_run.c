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


#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include "attribute.h"
#include "job.h"
#include "mock_run.h"
#include "mom_func.h"
#include "pbs_error.h"
#include "resource.h"


/**
 * @brief	work task handler for end of a job in mock run mode
 *
 * @param[in]	ptask - pointer to the work task
 *
 * @return void
 */
void
mock_run_end_job_task(struct work_task *ptask)
{
	job *pjob;

	if (ptask == NULL)
		return;

	pjob = ptask->wt_parm1;

	pjob->ji_qs.ji_substate = JOB_SUBSTATE_EXITING;
	pjob->ji_qs.ji_state = JOB_STATE_EXITING;
	pjob->ji_qs.ji_un.ji_momt.ji_exitstat = JOB_EXEC_OK;

	scan_for_exiting();
}


/**
 * @brief
 * 	Update the resources used.<attributes> of a job when in mock run mode
 *
 * @param[in]	pjob - job in question.
 *
 *
 * @return int
 * @retval PBSE_NONE	for success.
 */
int
mock_run_mom_set_use(job *pjob)
{
	int i;
	resource *pres;
	resource *pres_req;
	attribute *at;
	attribute *at_req;
	resource_def *rdefp;
	long val_req = 0;
	static resource_def	**rd = NULL;
	static resource_def *vmemd = NULL;
	int memval = 0;
	unsigned int mem_atsv_shift = 10;
	unsigned int mem_atsv_units = ATR_SV_BYTESZ;

	assert(pjob != NULL);
	at = &pjob->ji_wattr[(int)JOB_ATR_resc_used];
	assert(at->at_type == ATR_TYPE_RESC);
	at->at_flags |= (ATR_VFLAG_MODIFY|ATR_VFLAG_SET);

	if (rd == NULL) {
		rd = malloc(5 * sizeof(resource_def *));
		if (rd  == NULL)
			return PBSE_SYSTEM;

		rd[0] = find_resc_def(svr_resc_def, "ncpus", svr_resc_size);
		rd[1] = find_resc_def(svr_resc_def, "mem", svr_resc_size);
		rd[2] = find_resc_def(svr_resc_def, "cput", svr_resc_size);
		rd[3] = find_resc_def(svr_resc_def, "cpupercent", svr_resc_size);
		rd[4] = NULL;
	}
	if (vmemd == NULL) {
		vmemd = find_resc_def(svr_resc_def, "vmem", svr_resc_size);
		assert(vmemd != NULL);
	}

	for (i = 0; rd[i] != NULL; i++) {
		rdefp = rd[i];
		pres = find_resc_entry(at, rdefp);
		if (pres == NULL) {
			pres = add_resource_entry(at, rdefp);
			pres->rs_value.at_flags |= ATR_VFLAG_SET;
			pres->rs_value.at_type = rd[i]->rs_type;

			/*
			 * get pointer to list of resources *requested* for the job
			 * so the res used can be set to res requested
			 */
			at_req = &pjob->ji_wattr[(int)JOB_ATR_resource];
			assert(at->at_type == ATR_TYPE_RESC);

			pres_req = find_resc_entry(at_req, rdefp);
			if (pres_req != NULL &&
				(val_req = pres_req->rs_value.at_val.at_long) != 0)
				pres->rs_value.at_val.at_long = val_req;
			else
				pres->rs_value.at_val.at_long = 0;

			if (rd[i]->rs_type == ATR_TYPE_SIZE) {
				if (pres_req != NULL) {
					memval = val_req;
					mem_atsv_shift = pres_req->rs_value.at_val.at_size.atsv_shift;
					mem_atsv_units = pres_req->rs_value.at_val.at_size.atsv_units;
					pres->rs_value.at_val.at_size.atsv_shift = mem_atsv_shift;
					pres->rs_value.at_val.at_size.atsv_units = mem_atsv_units;
				} else {
					pres->rs_value.at_val.at_size.atsv_shift = 10; /* KB */
					pres->rs_value.at_val.at_size.atsv_units = ATR_SV_BYTESZ;
				}
			}
		}
	}

	/* Set vmem equal to the value of mem */
	pres = find_resc_entry(at, vmemd);
	if (pres == NULL) {
		pres = add_resource_entry(at, vmemd);
		pres->rs_value.at_flags |= ATR_VFLAG_SET;
		pres->rs_value.at_type = ATR_TYPE_LONG;
		pres->rs_value.at_val.at_long = memval;
		pres->rs_value.at_val.at_size.atsv_shift = mem_atsv_shift;
		pres->rs_value.at_val.at_size.atsv_units = mem_atsv_units;
	}

	pjob->ji_sampletim = time(NULL);

	/* update walltime usage */
	update_walltime(pjob);

	return (PBSE_NONE);
}
