/*
 * Copyright (C) 1994-2021 Altair Engineering, Inc.
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
 *
 * @brief
 * 		Functions relating to the Select Job Batch Request and the Select-Status
 * 		(SelStat) Batch Request.
 *
 */

#include <pbs_config.h>   /* the master config generated by configure */

#define STAT_CNTL 1

#include <sys/types.h>
#include <stdlib.h>
#include "libpbs.h"
#include <string.h>
#include "server_limits.h"
#include "list_link.h"
#include "attribute.h"
#include "resource.h"
#include "server.h"
#include "credential.h"
#include "batch_request.h"
#include "job.h"
#include "reservation.h"
#include "queue.h"
#include "pbs_error.h"
#include "log.h"
#include "pbs_nodes.h"
#include "svrfunc.h"
#include "pbs_sched.h"

/* Private Data */

/* Global Data Items  */

extern int	 resc_access_perm;
extern pbs_list_head svr_alljobs;
extern time_t	 time_now;
extern char	 statechars[];
extern long svr_history_enable;
extern int scheduler_jobs_stat;

/* Private Functions  */

static int
build_selist(svrattrl *, int perm, struct  select_list **,
	pbs_queue **, int *bad, char **pstate);
static void free_sellist(struct select_list *pslist);
static int  sel_attr(attribute *, struct select_list *);
static int  select_job(job *, struct select_list *, int, int);

/**
 * @brief
 * 		order_chkpnt - provide order value for various checkpoint attribute values
 *		n > s > c=minutes > c
 *
 * @param[in]	attr	-	attribute structure
 *
 * @return	order value
 * @retval	0	: no match
 * @retval	!0	: value according to the checkpoints
 */

static int
order_chkpnt(attribute *attr)
{
	if (((is_attr_set(attr)) == 0) ||
		(attr->at_val.at_str == 0))
		return 0;

	switch (*attr->at_val.at_str) {
		case 'n':	return 5;
		case 's':	return 4;
		case 'c':	if (*(attr->at_val.at_str+1) != '\0')
				return 3;
			else
				return 2;
		case 'u':	return 1;
		default:	return 0;
	}
}

/**
 * @brief
 * 		comp_chkpnt - compare two checkpoint attributes for selection
 *
 * @param[in]	attr	-	attribute structure to compare
 * @param[in]	with	-	attribute structure to compare with
 *
 * @return	int
 * @retval	0	: same
 * @retval	1	: attr > with
 * @retval	-1	: attr < with
 */

int
comp_chkpnt(attribute *attr, attribute *with)
{
	int a;
	int w;

	a = order_chkpnt(attr);
	w = order_chkpnt(with);

	if (a == w)
		return 0;
	else if (a > w)
		return 1;
	else
		return -1;
}

/**
 * @brief
 * 		comp_state - compare the state of a job attribute (state) with that in
 *		a select list (multiple state letters)
 *
 * @param[in]	state	-	state of a job attribute
 * @param[in]	selstate	-	select list (multiple state letters)
 *
 * @return	int
 * @retval	0	: match found
 * @retval	1	: no match
 * @retval	-1	: either state or selstate fields are empty
 */
static int
comp_state(attribute *state, attribute *selstate)
{
	char *ps;

	if (!state || !selstate || !selstate->at_val.at_str)
		return (-1);

	for (ps = selstate->at_val.at_str; *ps; ++ps) {
		if (*ps == state->at_val.at_char)
			return (0);
	}
	return (1);
}

static attribute_def state_sel = {
	ATTR_state,
	decode_str,
	encode_str,
	set_str,
	comp_state,
	free_str,
	NULL_FUNC,
	READ_ONLY,
	ATR_TYPE_STR,
	PARENT_TYPE_JOB
};

/**
 * @brief
 * 		chk_job_statenum - check the state of a job (actual numeric state) with
 * 		a list of state letters
 *
 * @param[in]	state_ltr	-	state of a job as a letter
 * @param[in]	statelist	-	list of state letters
 *
 * @return	int
 * @retval	0	: no match
 * @retval	1	: match found
 */
static int
chk_job_statenum(char state_ltr, char *statelist)
{
	log_errf(-1, __func__, "statlist=%s, state_ltr=%c", statelist, state_ltr);
	if (statelist == NULL)
		return 1;

	if (strchr(statelist, (int) state_ltr))
		return 1;
	return 0;
}

/**
 * @brief
 * 		add_select_entry - add one jobid entry to the select return
 * 
 * @param[in] preq - statjob, SelectJob Request or Select-status Job Request
 * @param[in] jid	-	jobid entry
 * @param[in,out] pselx	-	select return
 *
 * @return	int
 * @retval	-1	: error and not added
 * @retval	 0	: success
 */
static int
add_select_entry(struct batch_request *preq, char *jid, struct brp_select ***pselx)
{
	struct brp_select *pselect;
	struct batch_reply *preply = &preq->rq_reply;

	if (jid == NULL)
		return -1;

	pselect = (struct brp_select *)malloc(sizeof(struct brp_select));
	if (pselect == NULL)
		return -1;

	pselect->brp_next = NULL;
	(void)strcpy(pselect->brp_jobid, jid);
	**pselx = pselect;
	*pselx = &pselect->brp_next;

	preply->brp_count++;

	return 0;
}

/**
 * @brief
 * 	Add qualifiying subjobs (or all subjobs) to the stat reply
 *  Used for statjob, selectjob and selstat, basically all stats
 * 
 * @param[in] preq - statjob, SelectJob Request or Select-status Job Request
 * @param[in] pjob  pointer to job
 * @param[in] dosub  treat as a normal job or array job
 * @param[in] statelist  If statelist is NULL, then no need to check anything,
 * 						just add the subjobs to the return list.
 * @param[in,out] pselx	 select return
 * @param[in] dosubjobs - select subjobs as well? 
 * @param[in] from_tm - in case of diffstat, the timestamp to stat updates from
 *
 * @return	int
 * @retval	-1	: error and not added
 * @retval	 0	: success
 */
int
add_subjobs(struct batch_request *preq, job *pjob, char *statelist, struct brp_select ***pselx, int dosubjobs, struct timeval from_tm)
{
	int i;
	int rc = 0;
	job *parent;
	job *prev;
	int bad;
	deleted_obj_t *dj, *dj_prev;
	struct batch_reply *preply = &preq->rq_reply;
	svrattrl *plist = NULL;

	if (preq->rq_type == PBS_BATCH_SelStat) {
		plist = (svrattrl *) GET_NEXT(preq->rq_ind.rq_select.rq_rtnattr);
	}

	if (IS_FULLSTAT(from_tm)) {
		for (i = pjob->ji_ajinfo->tkm_start; i <= pjob->ji_ajinfo->tkm_end; i += pjob->ji_ajinfo->tkm_step) {
			char sjst = JOB_STATE_LTR_QUEUED;

			if ((preq->rq_type != PBS_BATCH_SelectJobs) && (range_contains(pjob->ji_ajinfo->trm_quelist, i)))
				continue; /* don't return queued subjobs for stats (except select) IFL will expand it */
			
			/*
			* If statelist is NULL, then no need to check anything,
			* just add the subjobs to the return list.
			*/
			job *sj = get_subjob_and_state(pjob, i, &sjst, NULL);
			if (sjst == JOB_STATE_LTR_UNKNOWN)
				continue;

			if (statelist == NULL || chk_job_statenum(sjst, statelist)) {
				if (preq->rq_type == PBS_BATCH_SelectJobs) {
					if (add_select_entry(preq, sj ? sj->ji_qs.ji_jobid : create_subjob_id(pjob->ji_qs.ji_jobid, i), pselx) != 0)
						return -1;
				} else {
					rc = status_subjob(pjob, preq, plist, i, &preply->brp_un.brp_status, &bad, 0, from_tm);
					if (rc && rc != PBSE_PERM)
						return -1;
				}
			}
		}
	} else {
		log_eventf(PBSEVENT_DEBUG3, PBS_EVENTCLASS_JOB, LOG_DEBUG, pjob->ji_qs.ji_jobid, "diffstat array subjobs");
		parent = pjob; /* save parent job pointer */
		pjob = (job *) GET_PRIOR(parent->ji_ajinfo->subjobs_timed);
		/* traverse backwards to find the oldest job matching (>=) the provided from time stamp */
		while (pjob && (rc == PBSE_NONE)) {
			log_eventf(PBSEVENT_DEBUG3, PBS_EVENTCLASS_JOB, LOG_DEBUG, pjob->ji_qs.ji_jobid,
				"diffstat considering subjob subjob_tm={%d,%d}, from_tm={%d,%d}", 
				pjob->update_tm.tv_sec, pjob->update_tm.tv_usec, from_tm.tv_sec, from_tm.tv_usec);

			if (!(TS_NEWER(pjob->update_tm, from_tm)))
				break;

			prev = (job *)GET_PRIOR(pjob->ji_timed_link); /* save prev ptr as pjob can change in status_job */
			rc = status_job(pjob, preq, plist, &preply->brp_un.brp_status, &bad, dosubjobs, from_tm);
			if (rc && rc != PBSE_PERM)
				break;

			pjob = prev;
		}

		/* adding entries for deleted subjobs - not as deleted ids, since subjobs are listed till job array goes away!!! */
		dj = (deleted_obj_t *) GET_PRIOR(parent->ji_ajinfo->subjobs_deleted);
		while (dj) {
			log_eventf(PBSEVENT_DEBUG3, PBS_EVENTCLASS_JOB, LOG_DEBUG, dj->obj_id,
				"diffstat considering deleted subjob subjob_tm={%d,%d}, from_tm={%d,%d}", 
				dj->tm_deleted.tv_sec, dj->tm_deleted.tv_usec, from_tm.tv_sec, from_tm.tv_usec);

			if (!(TS_NEWER(dj->tm_deleted, from_tm)))
				break;

			dj_prev = (deleted_obj_t *) GET_PRIOR(dj->deleted_obj_link);

			rc = status_subjob(parent, preq, plist, strtoul(dj->obj_id, NULL, 10), &preply->brp_un.brp_status, &bad, 0, from_tm);
			if (rc && rc != PBSE_PERM)
				break;
			dj = dj_prev;
		}
	}
	return 0;
}

/**
 * @brief
 * 		add_select_array_entries - add one jobid entry to the select return
 *		for each subjob whose state matches
 *
 * @param[in] preq - statjob, SelectJob Request or Select-status Job Request
 * @param[in] pjob - pointer to job
 * @param[in] dosub -	treat as a normal job or array job
 * @param[in] statelist -	If statelist is NULL, then no need to check anything,
 * 								just add the subjobs to the return list.
 * @param[in,out] pselx	- select return
 * @param[in] psel - pointer to select list
 *
 * @return	int
 * @retval	-1	: error and not added
 * @retval	 0	: success
 */
static int
add_select_array_entries(struct batch_request *preq, 
	job *pjob, int dosub, char *statelist,
	struct brp_select ***pselx,
	struct select_list *psel,
	struct timeval from_tm)
{
	int rc = 0;

	if (pjob->ji_qs.ji_svrflags & JOB_SVFLG_SubJob)
		return 0;
	else if ((dosub == 0) || (pjob->ji_qs.ji_svrflags & JOB_SVFLG_ArrayJob) == 0) {
		/* is or treat as a normal job */
		rc = add_select_entry(preq, pjob->ji_qs.ji_jobid, pselx);
	} else {
		/* Array Job */
		rc = add_subjobs(preq, pjob, statelist, pselx, dosub, from_tm);
	}

	return rc;
}

/**
 * @brief
 * 	Service both the Select Job Request and the (special for the scheduler)
 * 	Select-status Job Request
 *
 *	This request selects jobs based on a supplied criteria and returns
 *	Select   - a list of the job identifiers which meet the criteria
 *	Sel_stat - a list of the status of the jobs that meet the criteria
 *	             and only the list of specified attributes if specified
 *
 * @param[in,out] preq - Select Job Request or Select-status Job Request
 * @param[in] pjob - the job to add to the reply
 * @param[in] selistp - ptr to the select list
 * @param[in] statelist - list of states to include
 * @param[in/out] pselx - select return
 * @param[in] dosubjobs - select subjobs as well? 
 * @param[in] dohistjob - select history jobs as well?
 * @param[in] from_tm - in case of diffstat, the timestamp to stat updates from
 *
 * @return void
 *
 */
int
add_selstat_reply(struct batch_request *preq, job *pjob, struct select_list *selistp, 
		char *statelist, struct brp_select ***pselx, int dosubjobs, int dohistjobs, struct timeval from_tm)
{
	struct batch_reply *preply = &preq->rq_reply;
	svrattrl *plist;
	int rc;
	int bad = 0;

	if (server.sv_attr[SVR_ATR_query_others].at_val.at_long || svr_authorize_jobreq(preq, pjob) == 0) {
		/*
		* either job owner or has special permission to see job
		* and
		* look at the job and see if the required attributes match
		* If "T" was specified, dosubjobs is set, and if the job is
		* an Array Job, then the State is Not checked. The State
		* must be checked against the state of each Subjob
		*/

		if (select_job(pjob, selistp, dosubjobs, dohistjobs)) {

			/* job is selected, include in reply */
			if (preq->rq_type == PBS_BATCH_SelectJobs) {

				/* Select Jobs Reply */

				add_select_array_entries(preq, pjob, dosubjobs, statelist, pselx, selistp, from_tm);

			} else if ((pjob->ji_qs.ji_svrflags & JOB_SVFLG_SubJob) == 0 || dosubjobs == 2) {

				/* Select-Status Reply */

				plist = (svrattrl *) GET_NEXT(preq->rq_ind.rq_select.rq_rtnattr);
				if (dosubjobs == 1 && pjob->ji_ajinfo) {
					return (add_subjobs(preq, pjob, statelist, pselx, dosubjobs, from_tm));
				} else {
					rc = status_job(pjob, preq, plist, &preply->brp_un.brp_status, &bad, 0, from_tm);
					if (rc && rc != PBSE_PERM)
						return -1;
				}
			}
		}
	}
	return 0;
}

/**
 * @brief
 * 	Service both the Select Job Request and the (special for the scheduler)
 * 	Select-status Job Request
 *
 *	This request selects jobs based on a supplied criteria and returns
 *	Select   - a list of the job identifiers which meet the criteria
 *	Sel_stat - a list of the status of the jobs that meet the criteria
 *	             and only the list of specified attributes if specified
 *
 * @param[in,out] preq - Select Job Request or Select-status Job Request
 *
 * @return void
 *
 */
void
req_selectjobs(struct batch_request *preq)
{
	int bad = 0;
	job *pjob;
	svrattrl *plist;
	pbs_queue *pque;
	struct batch_reply *preply;
	struct brp_select **pselx;
	int dosubjobs = 0;
	int dohistjobs = 0;
	char *pstate = NULL;
	int rc;
	struct select_list *selistp;
	pbs_sched *psched;
	struct timeval from_tm = {0, 0};

	if (preq->rq_extend != NULL) {
		/*
		 * if the letter T (or t) is in the extend string, select subjobs
		 *
		 * if the letter S is in the extend string, select real jobs,
		 * regular and running subjobs as it is requested by the Scheduler.
		 */
		if (strchr(preq->rq_extend, 'T') || strchr(preq->rq_extend, 't'))
			dosubjobs = 1;
		else if (strchr(preq->rq_extend, 'S'))
			dosubjobs = 2;
		/*
		 * If the letter x is in the extend string, Check if the server is
		 * configured for job history info. If it is not SET or set to FALSE
		 * then return with PBSE_JOBHISTNOTSET error. Otherwise select history
		 * jobs also.
		 */
		if (strchr(preq->rq_extend, 'x')) {
			if (svr_history_enable == 0) {
				req_reject(PBSE_JOBHISTNOTSET, 0, preq);
				return;
			}
			dohistjobs = 1;
		}
		from_tm = parse_ts_from_extend(preq->rq_extend);
	}

	/*
	 * The first selstat() call from the scheduler indicates that a cycle
	 * is in progress and has reached the point of querying for jobs.
	 *
	 * TODO: This approach must be revisited if the scheduler changes its
	 * approach to query for jobs, e.g., by issuing a single pbs_statjob()
	 * instead of a per-queue selstat()
	 */
	psched = find_sched_from_sock(preq->rq_conn, CONN_SCHED_PRIMARY);
	if (psched != NULL && psched == dflt_scheduler && !scheduler_jobs_stat)
		scheduler_jobs_stat = 1;

	plist = (svrattrl *) GET_NEXT(preq->rq_ind.rq_select.rq_selattr);
	rc = build_selist(plist, preq->rq_perm, &selistp, &pque, &bad, &pstate);
	if (rc != 0) {
		reply_badattr(rc, bad, plist, preq);
		free_sellist(selistp);
		return;
	}

	/* setup the appropriate return */
	preply = &preq->rq_reply;
	if (preq->rq_type == PBS_BATCH_SelectJobs) {
		preply->brp_choice = BATCH_REPLY_CHOICE_Select;
		preply->brp_un.brp_select = NULL;
	} else {
		preply->brp_choice = BATCH_REPLY_CHOICE_Status;
		CLEAR_HEAD(preply->brp_un.brp_status);
	}
	pselx = &preply->brp_un.brp_select;
	preply->brp_count = 0;

	rc = PBSE_NONE;
	preply->latestObj.tv_sec = 0;
	preply->latestObj.tv_usec = 0;
	if (!IS_FULLSTAT(from_tm)) { /* if not a full stat = diff stat */
		if ((last_job_purge_ts.tv_sec != 0) && TS_NEWER(last_job_purge_ts, from_tm)) {
			/* oops too old timestamp, reject */
			req_reject(PBSE_STALE_DIFFQUERY, 0, preq);
			return;
		}
		preply->brp_auxcode = 1; /* diffstat reply */
		pjob = (job *) GET_PRIOR(svr_alljobs_timed);
		if (pjob)
			preply->latestObj = pjob->update_tm;

		/* traverse backwards to find the oldest job matching (>=) the provided from time stamp */
		for (; pjob && (rc == PBSE_NONE); pjob = (job *) GET_PRIOR(pjob->ji_timed_link)) {
			if (!(TS_NEWER(pjob->update_tm, from_tm)))
				break;
			if (pque && pjob->ji_qhdr != pque)
				continue;
			rc = add_selstat_reply(preq, pjob, selistp, pstate, &pselx, dosubjobs, dohistjobs, from_tm);  /* stat backwards, IFL will reverse */
			if( rc == -2)
				return; /* critical error in reply_send, so simply bail out */
			else if (rc == -1)
				goto out;
		}

		/* now stat deleted jobs */
		stat_deleted_ids(&svr_alljobs_deleted, from_tm, &preply->brp_un.brp_status, &last_job_purge_ts, &preply->brp_count, &preply->latestObj);
	} else { /* full stat */
		/* now start checking for jobs that match the selection criteria */
		pjob = (pque) ? (job *) GET_NEXT(pque->qu_jobs) : (job *) GET_NEXT(svr_alljobs);	

		while (pjob) {
			rc = add_selstat_reply(preq, pjob, selistp, pstate, &pselx, dosubjobs, dohistjobs, from_tm);
			if( rc == -2)
				return; /* critical error in reply_send, so simply bail out */
			else if (rc == -1)
				goto out;

			pjob = (pque) ? (job *) GET_NEXT(pjob->ji_jobque) : (job *) GET_NEXT(pjob->ji_alljobs);
		}
	}
out:
	free(pstate);
	free_sellist(selistp);
	if (rc)
		req_reject(rc, 0, preq);
	else
		reply_send(preq);
}

/**
 * @brief
 * 		select_job - determine if a single job matches the selection criteria
 *
 * @param[in]	pjob	-	pointer to job
 * @param[in]	psel	-	selection list
 * @param[in]	dosubjobs	-	Does it needs to check the subjob.
 * @param[in]	dohistjobs	-	If not being asked for history jobs specifically,
 * 								then just skip them otherwise include them.
 *
 * @return	int
 * @retval	0	: no match
 * @retval	1	: matches
 */

static int
select_job(job *pjob, struct select_list *psel, int dosubjobs, int dohistjobs)
{

	/*
	 * If not being asked for history jobs specifically, then just skip
	 * them otherwise include them. i.e. if the batch request has the special
	 * extended flag 'x'.
	 */
	if ((!dohistjobs) && ((check_job_state(pjob, JOB_STATE_LTR_FINISHED)) ||
		(check_job_state(pjob, JOB_STATE_LTR_MOVED)))) {
		return 0;
	}

	if ((dosubjobs == 2) && (pjob->ji_qs.ji_svrflags & JOB_SVFLG_SubJob) &&
		(!check_job_state(pjob, JOB_STATE_LTR_EXITING)) &&
		(!check_job_state(pjob, JOB_STATE_LTR_RUNNING))) /* select only exiting or running subjobs */
		return 0;

	if ((pjob->ji_qs.ji_svrflags & JOB_SVFLG_ArrayJob) == 0)
		dosubjobs = 0;  /* not an Array Job,  ok to check state */
	else if ((dosubjobs != 2) &&
		(pjob->ji_qs.ji_svrflags & JOB_SVFLG_SubJob))
		return 0;	/* don't bother to look at sub job */

	for (; psel; psel = psel->sl_next) {

		if (psel->sl_atindx == (int)JOB_ATR_userlst) {
			if (!acl_check(&psel->sl_attr, get_jattr_str(pjob, JOB_ATR_job_owner), ACL_User))
				return (0);

		} else if (!dosubjobs || (psel->sl_atindx != JOB_ATR_state)) {
			if (!sel_attr(get_jattr(pjob, psel->sl_atindx), psel)) {
				/* Make sure we haven't incorrectly dismissed a suspended job */
				if (psel->sl_atindx == JOB_ATR_state && get_attr_str(&psel->sl_attr)[0] == 'S') {
					if (check_job_state(pjob, JOB_STATE_LTR_RUNNING) &&
							(check_job_substate(pjob, JOB_SUBSTATE_SCHSUSP) ||
									check_job_substate(pjob, JOB_SUBSTATE_SUSPEND)))
						continue;
				}
				return 0;
			} else if (psel->sl_atindx == JOB_ATR_state && get_attr_str(&psel->sl_attr)[0] == 'R') {
				/* Make sure we don't incorrectly select suspended jobs */
				if (check_job_substate(pjob, JOB_SUBSTATE_SCHSUSP) || check_job_substate(pjob, JOB_SUBSTATE_SUSPEND))
					return 0;
			}
		}
	}

	return 1;
}

/**
 * @brief
 * 		sel_attr - determine if attribute is according to the selection operator
 *
 * @param[in]	jobat	-	job attribute
 * @param[in]	pselst	-	selection operator
 *
 * @return	int
 * @retval	0	: attribute does not meets criteria
 * @retval	1	: attribute meets criteria
 *
 */

static int
sel_attr(attribute *jobat, struct select_list *pselst)
{
	int	   rc;
	resource  *rescjb;
	resource  *rescsl;

	if (pselst->sl_attr.at_type == ATR_TYPE_RESC) {

		/* Only one resource per selection entry, 		*/
		/* find matching resource in job attribute if one	*/

		rescsl = (resource *)GET_NEXT(pselst->sl_attr.at_val.at_list);
		rescjb = find_resc_entry(jobat, rescsl->rs_defin);

		if (rescjb && (is_attr_set(&rescjb->rs_value)))
			/* found match, compare them */
			rc = pselst->sl_def->at_comp(&rescjb->rs_value, &rescsl->rs_value);
		else		/* not one in job,  force to .lt. */
			rc = -1;

	} else {
		/* "normal" attribute */

		rc = pselst->sl_def->at_comp(jobat, &pselst->sl_attr);
	}

	if (rc < 0) {
		if ((pselst->sl_op == NE) ||
			(pselst->sl_op == LT) ||
			(pselst->sl_op == LE))
			return (1);

	} else if (rc > 0) {
		if ((pselst->sl_op == NE) ||
			(pselst->sl_op == GT) ||
			(pselst->sl_op == GE))
			return (1);

	} else {	/* rc == 0 */
		if ((pselst->sl_op == EQ) ||
			(pselst->sl_op == GE) ||
			(pselst->sl_op == LE))
			return (1);
	}
	return (0);
}

/**
 * @brief
 * 		Free a select_list list created by build_selist()
 * @par
 *		For each entry in the select_list free the enclosed attribute entry
 *		using the index into the job_attr_def array in sl_atindx.  For an
 *		attribute of type resource, this is the index of the resource type
 *		attribute (typically Resource_List).  Where as sl_def is specific to
 *		the resource in the list headed by that attribute.  There is only one
 *		resource per select_list entry.
 *
 * @param[in]	pslist	-	pointer to first entry in the select list.
 *
 * @return	none
 */

static void
free_sellist(struct select_list *pslist)
{
	struct select_list *next;

	while (pslist) {
		next = pslist->sl_next;
		if (pslist->sl_atindx == JOB_ATR_state)
			state_sel.at_free(&pslist->sl_attr);
		else
			free_attr(job_attr_def, &pslist->sl_attr, pslist->sl_atindx);
		(void)free(pslist);			  /* free the entry */
		pslist = next;
	}
}


/**
 * @brief
 * 		build_selentry - build a single entry for a select list
 *
 * @param[in]	pslist	-	svrattrl structure from which we decode the select list
 * @param[in]	pdef	-	attribute_def structure.
 * @param[in]	perm	-	permission
 * @param[out]	rtnentry	-	pointer to the single entry for the select list
 *
 * @return	int
 * @retval	0	: success
 * @retval	!0	: error code
 *
 */

static int
build_selentry(svrattrl *plist, attribute_def *pdef, int perm, struct select_list **rtnentry)
{
	struct select_list *entry;
	resource_def *prd;
	int old_perms = resc_access_perm;
	int		    rc;

	/* create a select list entry for this attribute */

	entry = (struct select_list *)
		malloc(sizeof(struct select_list));
	if (entry == NULL)
		return (PBSE_SYSTEM);

	entry->sl_next = NULL;

	clear_attr(&entry->sl_attr, pdef);

	if (!(pdef->at_flags & ATR_DFLAG_RDACC & perm)) {
		(void)free(entry);
		return (PBSE_PERM);    /* no read permission */
	}
	if ((pdef->at_flags & ATR_DFLAG_SELEQ) && (plist->al_op != EQ) &&
		(plist->al_op != NE)) {
		/* can only select eq/ne on this attribute */
		(void)free(entry);
		return (PBSE_IVALREQ);
	}

	/*
	 * If a resource is marked flag=r in resourcedef
	 * we need to force the decode function to
	 * decode it to allow us to select upon it.
	 */
	if (plist->al_resc != NULL) {
		prd =find_resc_def(svr_resc_def, plist->al_resc);
		if (prd != NULL && (prd->rs_flags&NO_USER_SET) == NO_USER_SET) {
			resc_access_perm = ATR_DFLAG_ACCESS;
		}
	}

	/* decode the attribute into the entry */

	rc = set_attr_generic(&entry->sl_attr, pdef, plist->al_value, plist->al_resc, INTERNAL);


	resc_access_perm = old_perms;
	if (rc) {
		if (rc == PBSE_UNKRESC) {
			/* The resource was unknown, free the allocated attribute */
			pdef->at_free(&entry->sl_attr);
		}
		(void)free(entry);
		return (rc);
	}
	if (!is_attr_set(&entry->sl_attr)) {
		(void)free(entry);
		return (PBSE_BADATVAL);
	}

	/*
	 * save the pointer to the attribute definition,
	 * if a resource, use the resource specific one
	 */

	if (entry->sl_attr.at_type == ATR_TYPE_RESC) {
		entry->sl_def = (attribute_def *) find_resc_def(svr_resc_def, plist->al_resc);
		if (!entry->sl_def) {
			(void)free(entry);
			return (PBSE_UNKRESC);
		}
	} else
		entry->sl_def = pdef;

	/* save the selection operator to pass along */

	entry->sl_op = plist->al_op;

	*rtnentry = entry;
	return (0);
}

/**
 * @brief
 * 		build_selist - build the list of select_list structures based on
 *		the svrattrl structures in the request.
 * @par
 *		Function returns non-zero on an error, also returns into last
 *		four entries of the parameter list.
 *
 * @param[in]	plist	svrattrl structure from which we decode the select list
 * @param[in]	perm	permission
 * @param[out]	pselist	RETURN : select list
 * @param[out]	pque	RETURN : queue ptr if limit to que
 * @param[out]	bad	-	RETURN - index of bad attr
 * @param[out]	pstate	RETURN - pointer to required state
 *
 * @return	int
 * @retval	0	: success
 * @retval	!0	: error code
 */

static int
build_selist(svrattrl *plist, int perm, struct select_list **pselist, pbs_queue **pque, int *bad, char **pstate)
{
	struct select_list *entry;
	int i;
	char *pc;
	attribute_def *pdef;
	struct select_list *prior = NULL;
	int rc;
	char *st = NULL;

	/* set permission for decode_resc() */

	resc_access_perm = perm;

	*pque = NULL;
	*bad = 0;
	*pselist = NULL;
	while (plist) {
		(*bad)++;	/* list counter incase one is bad */

		/* go for all job unless a "destination" other than */
		/* "@server" is specified			    */

		if (!strcasecmp(plist->al_name, ATTR_q)) {
			if (plist->al_valln) {
				if (((pc = strchr(plist->al_value, (int)'@')) == 0) ||
					(pc != plist->al_value)) {

					/* does specified destination exist? */

					*pque = find_queuebyname(plist->al_value);
#ifdef NAS /* localmod 075 */
					if (*pque == NULL)
						*pque = find_resvqueuebyname(plist->al_value);
#endif /* localmod 075 */
					if (*pque == NULL)
						return (PBSE_UNKQUE);
				}
			}
		} else {
			i = find_attr(job_attr_idx, job_attr_def, plist->al_name);
			if (i < 0)
				return (PBSE_NOATTR);   /* no such attribute */

			if (i == JOB_ATR_state) {
				pdef = &state_sel;
				pbs_strcat(&st, NULL, plist->al_value);
			} else {
				pdef = job_attr_def + i;
			}

			/* create a select list entry for this attribute */

			rc = build_selentry(plist, pdef, perm, &entry);
			if (rc)
				return rc;
			entry->sl_atindx = i;

			/* add the entry to the select list */

			if (prior)
				prior->sl_next = entry;    /* link into list */
			else
				*pselist = entry;    /* return start of list */
			prior = entry;
		}
		plist = (svrattrl *)GET_NEXT(plist->al_link);
	}

	*pstate = st;
	
	return (0);
}
