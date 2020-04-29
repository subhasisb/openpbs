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
 * @file   

 * 		This file contains the functions to record a job
 *		data struture to database and to recover it from database.
 *
 *		The data is recorded in the database
 *
 */


#include <pbs_config.h>   /* the master config generated by configure */

#include <stdio.h>
#include <sys/types.h>

#ifndef WIN32
#include <sys/param.h>
#include <execinfo.h>
#endif

#include "pbs_ifl.h"
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <unistd.h>
#include "server_limits.h"
#include "list_link.h"
#include "attribute.h"

#ifdef WIN32
#include <sys/stat.h>
#include <io.h>
#include <windows.h>
#include "win.h"
#endif

#include "job.h"
#include "reservation.h"
#include "queue.h"
#include "log.h"
#include "pbs_nodes.h"
#include "svrfunc.h"
#include <memory.h>
#include "libutil.h"
#include "pbs_db.h"
#include "server.h"


#define MAX_SAVE_TRIES 3

#ifndef PBS_MOM
extern pbs_db_conn_t	*svr_db_conn;
#ifndef WIN32
#define BACKTRACE_BUF_SIZE 50
void print_backtrace(char *);
#endif
#endif

/* global data items */
extern time_t time_now;

#ifndef PBS_MOM

/**
 * @brief
 *		convert job structure to DB format
 *
 * @see
 * 		job_save_db
 *
 * @param[in]	pjob - Address of the job in the server
 * @param[out]	dbjob - Address of the database job object
 *
 * @retval	-1  Failure
 * @retval	>=0 What to save: 0=nothing, OBJ_SAVE_NEW or OBJ_SAVE_QS
 */
static int
job_2_db(job *pjob, pbs_db_job_info_t *dbjob)
{
	int savetype = 0;
	int save_all_attrs = 0;

	strcpy(dbjob->ji_jobid, pjob->ji_qs.ji_jobid);
	strcpy(dbjob->ji_savetm, pjob->ji_savetm);

	if (pjob->ji_qs.ji_state == JOB_STATE_FINISHED)
		save_all_attrs = 1;

	if ((encode_attr_db(job_attr_def, pjob->ji_wattr, JOB_ATR_LAST, &dbjob->cache_attr_list, &dbjob->db_attr_list, save_all_attrs)) != 0)
		return -1;

	if (pjob->ji_savetm[0] == '\0') /* object was never saved/loaded before */
		savetype |= (OBJ_SAVE_NEW | OBJ_SAVE_QS);

	if (obj_qs_modified(&pjob->ji_qs, sizeof(pjob->ji_qs), pjob->qs_hash) == 1) {
		savetype |= OBJ_SAVE_QS;

		dbjob->ji_state     = pjob->ji_qs.ji_state;
		dbjob->ji_substate  = pjob->ji_qs.ji_substate;
		dbjob->ji_svrflags  = pjob->ji_qs.ji_svrflags;
		dbjob->ji_numattr   = pjob->ji_qs.ji_numattr;
		dbjob->ji_ordering  = pjob->ji_qs.ji_ordering;
		dbjob->ji_priority  = pjob->ji_qs.ji_priority;
		dbjob->ji_stime     = pjob->ji_qs.ji_stime;
		dbjob->ji_endtBdry  = pjob->ji_qs.ji_endtBdry;
		strcpy(dbjob->ji_queue, pjob->ji_qs.ji_queue);
		strcpy(dbjob->ji_destin, pjob->ji_qs.ji_destin);
		dbjob->ji_un_type   = pjob->ji_qs.ji_un_type;
		if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_NEW) {
			dbjob->ji_fromsock  = pjob->ji_qs.ji_un.ji_newt.ji_fromsock;
			dbjob->ji_fromaddr  = pjob->ji_qs.ji_un.ji_newt.ji_fromaddr;
		} else if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_EXEC) {
			dbjob->ji_momaddr   = pjob->ji_qs.ji_un.ji_exect.ji_momaddr;
			dbjob->ji_momport   = pjob->ji_qs.ji_un.ji_exect.ji_momport;
			dbjob->ji_exitstat  = pjob->ji_qs.ji_un.ji_exect.ji_exitstat;
		} else if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_ROUTE) {
			dbjob->ji_quetime   = pjob->ji_qs.ji_un.ji_routet.ji_quetime;
			dbjob->ji_rteretry  = pjob->ji_qs.ji_un.ji_routet.ji_rteretry;
		} else if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_MOM) {
			dbjob->ji_exitstat  = pjob->ji_qs.ji_un.ji_momt.ji_exitstat;
		}
		/* extended portion */
		strcpy(dbjob->ji_4jid, pjob->ji_extended.ji_ext.ji_4jid);
		strcpy(dbjob->ji_4ash, pjob->ji_extended.ji_ext.ji_4ash);
		dbjob->ji_credtype  = pjob->ji_extended.ji_ext.ji_credtype;
		dbjob->ji_qrank = pjob->ji_wattr[(int)JOB_ATR_qrank].at_val.at_long;
	}

	return savetype;
}

/**
 * @brief
 *		convert from database to job structure
 *
 * @see
 * 		job_recov_db
 *
 * @param[out]	pjob - Address of the job in the server
 * @param[in]	dbjob - Address of the database job object
 *
 * @retval   !=0  Failure
 * @retval   0    Success
 */
static int
db_2_job(job *pjob,  pbs_db_job_info_t *dbjob)
{
	/* Variables assigned constant values are not stored in the DB */
	pjob->ji_qs.ji_jsversion = JSVERSION;
	strcpy(pjob->ji_qs.ji_jobid, dbjob->ji_jobid);
	pjob->ji_qs.ji_state = dbjob->ji_state;
	pjob->ji_qs.ji_substate = dbjob->ji_substate;
	pjob->ji_qs.ji_svrflags = dbjob->ji_svrflags;
	pjob->ji_qs.ji_numattr = dbjob->ji_numattr ;
	pjob->ji_qs.ji_ordering = dbjob->ji_ordering;
	pjob->ji_qs.ji_priority = dbjob->ji_priority;
	pjob->ji_qs.ji_stime = dbjob->ji_stime;
	pjob->ji_qs.ji_endtBdry = dbjob->ji_endtBdry;
	strcpy(pjob->ji_qs.ji_queue, dbjob->ji_queue);
	strcpy(pjob->ji_qs.ji_destin, dbjob->ji_destin);
	pjob->ji_qs.ji_fileprefix[0] = 0;
	pjob->ji_qs.ji_un_type = dbjob->ji_un_type;
	if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_NEW) {
		pjob->ji_qs.ji_un.ji_newt.ji_fromsock = dbjob->ji_fromsock;
		pjob->ji_qs.ji_un.ji_newt.ji_fromaddr = dbjob->ji_fromaddr;
		pjob->ji_qs.ji_un.ji_newt.ji_scriptsz = 0;
	} else if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_EXEC) {
		pjob->ji_qs.ji_un.ji_exect.ji_momaddr = dbjob->ji_momaddr;
		pjob->ji_qs.ji_un.ji_exect.ji_momport = dbjob->ji_momport;
		pjob->ji_qs.ji_un.ji_exect.ji_exitstat = dbjob->ji_exitstat;
	} else if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_ROUTE) {
		pjob->ji_qs.ji_un.ji_routet.ji_quetime = dbjob->ji_quetime;
		pjob->ji_qs.ji_un.ji_routet.ji_rteretry = dbjob->ji_rteretry;
	} else if (pjob->ji_qs.ji_un_type == JOB_UNION_TYPE_MOM) {
		pjob->ji_qs.ji_un.ji_momt.ji_svraddr = 0;
		pjob->ji_qs.ji_un.ji_momt.ji_exitstat = dbjob->ji_exitstat;
		pjob->ji_qs.ji_un.ji_momt.ji_exuid = 0;
		pjob->ji_qs.ji_un.ji_momt.ji_exgid = 0;
	}

	/* extended portion */
#if defined(__sgi)
	pjob->ji_extended.ji_ext.ji_jid = 0;
	pjob->ji_extended.ji_ext.ji_ash = 0;
#else
	strcpy(pjob->ji_extended.ji_ext.ji_4jid, dbjob->ji_4jid);
	strcpy(pjob->ji_extended.ji_ext.ji_4ash, dbjob->ji_4ash);
#endif
	pjob->ji_extended.ji_ext.ji_credtype = dbjob->ji_credtype;

	if ((decode_attr_db(pjob, &dbjob->cache_attr_list, &dbjob->db_attr_list, job_attr_def, pjob->ji_wattr, (int)JOB_ATR_LAST, (int) JOB_ATR_UNKN)) != 0)
		return -1;

	obj_qs_modified(&pjob->ji_qs, sizeof(pjob->ji_qs), pjob->qs_hash);

	strcpy(pjob->ji_savetm, dbjob->ji_savetm);

	return 0;
}

/**
 * @brief
 *		Save job to database
 *
 * @param[in]	pjob - The job to save
 * @param[in]   updatetype:
 *				- bitwise operator to speficy what part to save. OBJ_SAVE_QS, OBJ_SAVE_ATTRS
 * @return      Error code
 * @retval	 0 - Success
 * @retval	-1 - Failure
 * @retval	 1 - Jobid clash, retry with new jobid
 *
 */
int
job_save_db(job *pjob)
{
	pbs_db_job_info_t dbjob= {{0}};
	pbs_db_obj_info_t obj;
	pbs_db_conn_t *conn = svr_db_conn;
	int savetype;
	int rc = -1;

	if ((savetype = job_2_db(pjob, &dbjob)) == -1)
		goto done;

	obj.pbs_db_obj_type = PBS_DB_JOB;
	obj.pbs_db_un.pbs_db_job = &dbjob;

	if ((rc = pbs_db_save_obj(conn, &obj, savetype)) == 0) {
		strcpy(pjob->ji_savetm, dbjob.ji_savetm); /* update savetm when we save a job, so that we do not save multiple times */

		/* don't save mtime, set it from ji_savetm - TODO */
		pjob->ji_wattr[JOB_ATR_mtime].at_val.at_long = time_now;
		pjob->ji_wattr[JOB_ATR_mtime].at_flags |= ATR_VFLAG_MODCACHE;
	}

done:
	free_db_attr_list(&dbjob.db_attr_list);
	free_db_attr_list(&dbjob.cache_attr_list);

	if (rc != 0) {
		if (conn->conn_db_err) {
			if ((savetype & OBJ_SAVE_NEW) && strstr(conn->conn_db_err, "duplicate key"))
				rc = 1;
		} else {
			sprintf(log_buffer, "Failed to save job %s ", pjob->ji_qs.ji_jobid);
			if (conn->conn_db_err != NULL)
				strncat(log_buffer, conn->conn_db_err, LOG_BUF_SIZE - strlen(log_buffer) - 1);
			log_err(-1, __func__, log_buffer);
		}
		
		if (rc == -1)
			panic_stop_db(log_buffer);
	}

	return (rc);
}

/**
 * @brief
 *	Utility function called to allocate and decode job structure
 *
 * @param[in]	pjob  - pointer to job structure in heap, if exists, else NULL
 * @param[in]	dbjob - Pointer to the database structure of a job
 *
 * @retval	 NULL - Failure
 * @retval	!NULL - Success, pointer to job structure recovered
 *
 */
job *
job_recov_db_spl(job *pjob, pbs_db_job_info_t *dbjob)
{
	job *pj = NULL;

	if (!pjob) {
		pj = job_alloc();
		pjob = pj;
	}
	
	if (pjob) {
		if (db_2_job(pjob, dbjob) == 0)
			return (pjob);
	}

	/* error case */
	if (pj)
		job_free(pj); /* free if we allocated here */

	snprintf(log_buffer, LOG_BUF_SIZE, "Failed to decode job %s", dbjob->ji_jobid);
	log_err(-1, __func__, log_buffer);

	return (NULL);
}

/**
 * @brief
 *	Recover job from database
 *
 * @param[in]	jid - Job id of job to recover
 * @param[in]	pjob - job pointer, if any, to be updated
 *
 * @return      The recovered job
 * @retval	 NULL - Failure
 * @retval	!NULL - Success, pointer to job structure recovered
 *
 */
job *
job_recov_db(char *jid, job *pjob)
{
	pbs_db_job_info_t dbjob = {{0}};
	pbs_db_obj_info_t obj;
	int rc = -1;
	pbs_db_conn_t *conn = svr_db_conn;

	if (pjob) {
		CHECK_ALREADY_LOADED(pjob);
		strcpy(dbjob.ji_savetm, pjob->ji_savetm);
	} else
		dbjob.ji_savetm[0] = '\0';
	
	strcpy(dbjob.ji_jobid, jid);
	obj.pbs_db_obj_type = PBS_DB_JOB;
	obj.pbs_db_un.pbs_db_job = &dbjob;
	rc = pbs_db_load_obj(conn, &obj);
	if (rc == -2)
		return pjob; /* no change in job, return the same job */

	if (rc == 0) {
		pjob = job_recov_db_spl(pjob, &dbjob);
	}
		
	free_db_attr_list(&dbjob.db_attr_list);
	free_db_attr_list(&dbjob.cache_attr_list);

	return (pjob);
}

/**
 * @brief
 *		convert resv structure to DB format
 *
 * @see
 * 		resv_save_db
 *
 * @param[in]	presv - Address of the resv in the server
 * @param[out]  dbresv - Address of the database resv object
 *
 * @retval   -1  Failure
 * @retval   >=0 What to save: 0=nothing, OBJ_SAVE_NEW or OBJ_SAVE_QS
 */
static int
resv_2_db(resc_resv *presv,  pbs_db_resv_info_t *dbresv)
{
	int savetype = 0;

	strcpy(dbresv->ri_resvid, presv->ri_qs.ri_resvID);
	strcpy(dbresv->ri_savetm, presv->ri_savetm);

	if ((encode_attr_db(resv_attr_def, presv->ri_wattr, (int)RESV_ATR_LAST, &(dbresv->cache_attr_list), &(dbresv->db_attr_list), 0)) != 0)
		return -1;

	if (presv->ri_savetm[0] == '\0') /* object was never saved or loaded before */
		savetype |= (OBJ_SAVE_NEW | OBJ_SAVE_QS);

	if (obj_qs_modified(&presv->ri_qs, sizeof(presv->ri_qs), presv->qs_hash) == 1) {
		savetype |= OBJ_SAVE_QS;

		strcpy(dbresv->ri_queue, presv->ri_qs.ri_queue);
		dbresv->ri_duration = presv->ri_qs.ri_duration;
		dbresv->ri_etime = presv->ri_qs.ri_etime;
		dbresv->ri_un_type = presv->ri_qs.ri_un_type;
		if (dbresv->ri_un_type == RESV_UNION_TYPE_NEW) {
			dbresv->ri_fromaddr = presv->ri_qs.ri_un.ri_newt.ri_fromaddr;
			dbresv->ri_fromsock = presv->ri_qs.ri_un.ri_newt.ri_fromsock;
		}
		dbresv->ri_numattr = presv->ri_qs.ri_numattr;
		dbresv->ri_resvTag = presv->ri_qs.ri_resvTag;
		dbresv->ri_state = presv->ri_qs.ri_state;
		dbresv->ri_stime = presv->ri_qs.ri_stime;
		dbresv->ri_substate = presv->ri_qs.ri_substate;
		dbresv->ri_svrflags = presv->ri_qs.ri_svrflags;
		dbresv->ri_tactive = presv->ri_qs.ri_tactive;
		dbresv->ri_type = presv->ri_qs.ri_type;
	}
	
	return savetype;
}

/**
 * @brief
 *		convert from database to resv structure
 *
 * @param[out]	presv - Address of the resv in the server
 * @param[in]	dbresv - Address of the database resv object
 *
 * @retval   !=0  Failure
 * @retval   0    Success
 */
static int
db_2_resv(resc_resv *presv, pbs_db_resv_info_t *pdresv)
{
	strcpy(presv->ri_qs.ri_resvID, pdresv->ri_resvid);
	strcpy(presv->ri_qs.ri_queue, pdresv->ri_queue);
	presv->ri_qs.ri_duration = pdresv->ri_duration;
	presv->ri_qs.ri_etime = pdresv->ri_etime;
	presv->ri_qs.ri_un_type = pdresv->ri_un_type;
	if (pdresv->ri_un_type == RESV_UNION_TYPE_NEW) {
		presv->ri_qs.ri_un.ri_newt.ri_fromaddr = pdresv->ri_fromaddr;
		presv->ri_qs.ri_un.ri_newt.ri_fromsock = pdresv->ri_fromsock;
	}
	presv->ri_qs.ri_numattr = pdresv->ri_numattr;
	presv->ri_qs.ri_resvTag = pdresv->ri_resvTag;
	presv->ri_qs.ri_state = pdresv->ri_state;
	presv->ri_qs.ri_stime = pdresv->ri_stime;
	presv->ri_qs.ri_substate = pdresv->ri_substate;
	presv->ri_qs.ri_svrflags = pdresv->ri_svrflags;
	presv->ri_qs.ri_tactive = pdresv->ri_tactive;
	presv->ri_qs.ri_type = pdresv->ri_type;
	strcpy(presv->ri_savetm, pdresv->ri_savetm);

	if ((decode_attr_db(presv, &pdresv->cache_attr_list, &pdresv->db_attr_list, resv_attr_def, presv->ri_wattr, (int) RESV_ATR_LAST, (int) RESV_ATR_UNKN)) != 0)
		return -1;

	obj_qs_modified(&presv->ri_qs, sizeof(presv->ri_qs), presv->qs_hash);

	strcpy(presv->ri_savetm, pdresv->ri_savetm);

	return 0;

}

/**
 * @brief
 *	Save resv to database
 *
 * @param[in]	presv - The resv to save
 * @param[in]   updatetype:
 *		SAVERESV_QUICK - Quick update without attributes
 *		SAVERESV_FULL  - Full update with attributes
 *		SAVERESV_NEW   - New resv, insert into database
 *
 * @return      Error code
 * @retval	 0 - Success
 * @retval	-1 - Failure
 * @retval	 1 - resvid clash, retry with new resvid
 *
 */
int
resv_save_db(resc_resv *presv)
{
	pbs_db_resv_info_t dbresv = {{0}};
	pbs_db_obj_info_t obj;
	pbs_db_conn_t *conn = svr_db_conn;
	int savetype;
	int rc = -1;

	if ((savetype = resv_2_db(presv, &dbresv)) == -1)
		goto done;	

	obj.pbs_db_obj_type = PBS_DB_RESV;
	obj.pbs_db_un.pbs_db_resv = &dbresv;
	
	if (pbs_db_save_obj(conn, &obj, savetype) == 0) {
		strcpy(presv->ri_savetm, dbresv.ri_savetm); /* update savetm when we save a job, so that we do not save multiple times */

		/* don't save mtime, set it from ji_savetm - TODO */
		presv->ri_wattr[RESV_ATR_mtime].at_val.at_long = time_now;
		presv->ri_wattr[RESV_ATR_mtime].at_val.at_long |= ATR_VFLAG_MODCACHE|ATR_VFLAG_MODIFY;
		
		rc = 0;
	}

done:
	free_db_attr_list(&dbresv.db_attr_list);
	free_db_attr_list(&dbresv.cache_attr_list);

	if (rc != 0) {
		sprintf(log_buffer, "Failed to save resv %s ", presv->ri_qs.ri_resvID);
		if (conn->conn_db_err != NULL)
			strncat(log_buffer, conn->conn_db_err, LOG_BUF_SIZE - strlen(log_buffer) - 1);
		log_err(-1, __func__, log_buffer);

		if(conn->conn_db_err) {
			if (savetype == OBJ_SAVE_NEW && strstr(conn->conn_db_err, "duplicate key value"))
				rc = 1;
		}
		
		if (rc == -1)
			panic_stop_db(log_buffer);
	}

	return (rc);
}

/**
 * @brief
 *	Utility function called to allocate and decode resv structure
 *
 * @param[in]	presv  - pointer to resv structure in heap, if exists, else NULL
 * @param[in]	dbresv - Pointer to the database structure of a resv
 *
 * @retval	 NULL - Failure
 * @retval	!NULL - Success, pointer to job structure recovered
 *
 */
resc_resv *
resv_recov_db_spl(resc_resv *presv, pbs_db_resv_info_t *dbresv)
{
	resc_resv *pr = NULL;

	if (!presv) {
		pr = resc_resv_alloc();
		presv = pr;
	}
	
	if (presv) {
		if (db_2_resv(presv, dbresv) == 0)
			return (presv);
	}

	/* error case */
	if (pr)
		resv_free(pr); /* free if we allocated here */

	snprintf(log_buffer, LOG_BUF_SIZE, "Failed to decode job %s", dbresv->ri_resvid);
	log_err(-1, __func__, log_buffer);

	return (NULL);
}

/**
 * @brief
 *	Recover resv from database
 *
 * @param[in]	resvid - Resv id to recover
 * @param[in]	presv - Resv pointer, if any, to be updated
 *
 * @return      The recovered reservation
 * @retval	 NULL - Failure
 * @retval	!NULL - Success, pointer to resv structure recovered
 *
 */
resc_resv *
resv_recov_db(char *resvid, resc_resv *presv)
{
	pbs_db_resv_info_t dbresv = {{0}};
	pbs_db_obj_info_t obj;
	pbs_db_conn_t *conn = svr_db_conn;
	int rc = -1;

	if (presv) {
		CHECK_ALREADY_LOADED(presv);
		strcpy(dbresv.ri_savetm, presv->ri_savetm);
	} else
		dbresv.ri_savetm[0] = '\0';

	strcpy(dbresv.ri_resvid, resvid);
	obj.pbs_db_obj_type = PBS_DB_RESV;
	obj.pbs_db_un.pbs_db_resv = &dbresv;
	rc = pbs_db_load_obj(conn, &obj);
	if (rc == -2)
		return presv; /* no change in resv */

	if (rc == 0) {
		presv = resv_recov_db_spl(presv, &dbresv);
	}

	free_db_attr_list(&dbresv.db_attr_list);
	free_db_attr_list(&dbresv.cache_attr_list);

	return presv;
}


/**
 * @brief
 *	Refresh/retrieve job from database and add it into AVL tree if not present
 *
 *	@param[in]	dbjob - The pointer to the wrapper job object of type pbs_db_job_info_t
 *  @param[in]  refreshed - To count the no. of jobs refreshed
 *
 * @return	The recovered job
 * @retval	NULL - Failure
 * @retval	!NULL - Success, pointer to job structure recovered
 *
 */
job *
refresh_job(pbs_db_job_info_t *dbjob, int *refreshed) 
{
	job *pj = NULL;
	*refreshed = 0;

	if ((pj = find_job_avl(dbjob->ji_jobid)) == NULL) {
		if ((pj = job_recov_db_spl(pj, dbjob)) == NULL) /* if job is not in AVL tree, load the job from database */
			goto err;

		svr_enquejob(pj); /* add job into AVL tree */
		account_entity_limit_usages(pj, NULL, NULL, INCR, ETLIM_ACC_ALL);

		*refreshed = 1;
		
	} else if (strcmp(dbjob->ji_savetm, pj->ji_savetm) != 0) { /* if the job had really changed in the DB */
		if (db_2_job(pj, dbjob) != 0)
			goto err;

		*refreshed = 1;
	}

	return pj;

err:
	snprintf(log_buffer, LOG_BUF_SIZE, "Failed to refresh job %s", dbjob->ji_jobid);
	log_err(-1, __func__, log_buffer);
	return NULL;
}


/**
 * @brief
 *	Refresh/retrieve reservation from database and add it into list if not present
 *
 *	@param[in]	dbresv - The pointer to the wrapper resv object of type pbs_db_resv_info_t
 *  @param[in]  refreshed - To count the no. of reservation refreshed
 *
 * @return	The recovered reservation
 * @retval	NULL - Failure
 * @retval	!NULL - Success, pointer to reservation structure recovered
 *
 */
resc_resv *
refresh_resv(pbs_db_resv_info_t *dbresv, int *refreshed) 
{
	extern pbs_list_head	svr_allresvs; 
	resc_resv *presv = NULL;
	char *at;

	*refreshed = 0;
	
	if ((at = strchr(dbresv->ri_resvid, (int)'@')) != 0)
		*at = '\0';	/* strip of @server_name */

	presv = (resc_resv *)GET_NEXT(svr_allresvs);
	while (presv != NULL) {
		if (!strcmp(dbresv->ri_resvid, presv->ri_qs.ri_resvID))
			break;
		presv = (resc_resv *)GET_NEXT(presv->ri_allresvs);
	}
	if (at)
		*at = '@';	/* restore @server_name */

	if (presv == NULL) {
		/* if resv is not in list, load the resv from database */
		if ((presv = resv_recov_db_spl(presv, dbresv)) == NULL)
			goto err;

		/* add resv to server list */
		append_link(&svr_allresvs, &presv->ri_allresvs, presv);
		
		*refreshed = 1;

	} else if (strcmp(dbresv->ri_savetm, presv->ri_savetm) != 0) { /* if the job had really changed in the DB */
		if (db_2_resv(presv, dbresv) != 0)
			goto err;
		
		*refreshed = 1;
	}

	return presv;

err:
	snprintf(log_buffer, LOG_BUF_SIZE, "Failed to refresh resv attribute %s", dbresv->ri_resvid);
	log_err(-1, __func__, log_buffer);
	return NULL;
}

#endif /* ifndef PBS_MOM */
