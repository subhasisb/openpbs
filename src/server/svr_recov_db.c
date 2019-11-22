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
 * @file    svr_recov_db.c
 *
 * @brief
 * 		svr_recov_db.c - contains functions to save server state and recover
 *
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/time.h>
#include "pbs_ifl.h"
#include "server_limits.h"
#include "list_link.h"
#include "attribute.h"
#include "job.h"
#include "reservation.h"
#include "queue.h"
#include "server.h"
#include "pbs_nodes.h"
#include "svrfunc.h"
#include "log.h"
#include "pbs_db.h"
#include "pbs_sched.h"
#include "pbs_share.h"

/* Global Data Items: */

extern struct server server;
extern pbs_list_head svr_queues;
extern attribute_def svr_attr_def[];
extern char	*path_priv;
extern time_t	time_now;
extern char	*msg_svdbopen;
extern char	*msg_svdbnosv;
extern char	*path_svrlive;

#ifndef PBS_MOM
extern char *pbs_server_name;
extern pbs_db_conn_t	*svr_db_conn;
extern void sched_free(pbs_sched *psched);
#endif

extern pbs_sched *sched_alloc(char *sched_name);

/**
 * @brief
 *		Update the $PBS_HOME/server_priv/svrlive file timestamp
 *
 * @return	Error code
 * @retval	0	: Success
 * @retval	-1	: Failed to update timestamp
 *
 */
int
update_svrlive()
{
	static int fdlive = -1;
	if (fdlive == -1) {
		/* first time open the file */
		fdlive = open(path_svrlive, O_WRONLY | O_CREAT, 0600);
		if (fdlive < 0)
			return -1;
	}
	(void)utimes(path_svrlive, NULL);
	return 0;
}

/**
 * @brief
 *	convert server structure to DB format
 *
 * @param[in]	ps	-	Address of the server in pbs server
 * @param[out]	pdbsvr	-	Address of the database server object
 * @param[in]   updatetype -    quick or full update
 *
 * @retval   -1  Failure
 * @retval   >=0    Success
 *
 */
static int
svr_2_db(struct server *ps, pbs_db_svr_info_t *pdbsvr)
{
	int savetype = 0;

	strcpy(pdbsvr->sv_savetm, ps->sv_savetm);
	pdbsvr->sv_jobidnumber = ps->sv_qs.sv_lastid;

	if ((encode_attr_db(svr_attr_def, ps->sv_attr, (int)SRV_ATR_LAST, &pdbsvr->cache_attr_list, &pdbsvr->db_attr_list, 1)) != 0) /* encode all attributes */
		return -1;
	
	if (ps->sv_savetm[0] == '\0') /* object was never saved or loaded before */
		savetype |= (OBJ_SAVE_NEW | OBJ_SAVE_QS);

	return savetype;
}

/**
 * @brief
 *	convert from DB to server structure 
 *
 * @param[out]	ps	-	Address of the server in pbs server
 * @param[in]	pdbsvr	-	Address of the database server object
 *
 * @return   !=0   - Failure
 * @return   0     - Success
 */
int
db_2_svr(struct server *ps, pbs_db_svr_info_t *pdbsvr)
{
	if ((decode_attr_db(ps, &pdbsvr->cache_attr_list, &pdbsvr->db_attr_list, svr_attr_def, ps->sv_attr, (int) SRV_ATR_LAST, 0)) != 0)
		return -1;

	strcpy(ps->sv_savetm, pdbsvr->sv_savetm);
	ps->sv_qs.sv_jobidnumber = pdbsvr->sv_jobidnumber;

	return 0;
}

/**
 * @brief
 *	convert sched structure to DB format
 *
 * @param[in]	ps - Address of the scheduler in pbs server
 * @param[out] pdbsched  - Address of the database scheduler object
 * @param[in] updatetype - quick or full update
 *
 * @retval   -1  Failure
 * @retval   >=0    Success
 */
static int
sched_2_db(struct pbs_sched *ps, pbs_db_sched_info_t *pdbsched)
{
	int savetype = 0;

	strcpy(pdbsched->sched_name, ps->sc_name);
	strcpy(pdbsched->sched_savetm, ps->sc_savetm);

	if ((encode_attr_db(sched_attr_def, ps->sch_attr, (int)SCHED_ATR_LAST, &pdbsched->cache_attr_list, &pdbsched->db_attr_list, 0)) != 0) 
		return -1;

	if (ps->sc_savetm[0] == '\0') /* was never loaded or saved before */
		savetype |= OBJ_SAVE_NEW;

	return savetype;
}

/**
 * @brief
 *	convert from DB to sched structure 
 *
 * @param[out] ps - Address of the scheduler in pbs server
 * @param[in]  pdbsched  - Address of the database scheduler object
 *
 */
static int
db_2_sched(struct pbs_sched *ps, pbs_db_sched_info_t *pdbsched)
{
	strcpy(ps->sc_name, pdbsched->sched_name);

	if ((decode_attr_db(ps, &pdbsched->cache_attr_list, &pdbsched->db_attr_list, sched_attr_def, ps->sch_attr, (int) SCHED_ATR_LAST, 0)) != 0)
		return -1;

	strcpy(ps->sc_savetm, pdbsched->sched_savetm);

	return 0;
}

/**
 * @brief
 *		Recover server information and attributes from server database
 *
 * @return	Error code
 * @retval	0	: On successful recovery and creation of server structure
 * @retval	-1	: On failure
 *
 */
int
svr_recov_db(void)
{
	pbs_db_conn_t *conn = (pbs_db_conn_t *) svr_db_conn;
	pbs_db_svr_info_t dbsvr = {{0}};
	pbs_db_obj_info_t obj;
	int rc = -1;

	obj.pbs_db_obj_type = PBS_DB_SVR;
	obj.pbs_db_un.pbs_db_svr = &dbsvr;
	
	strcpy(dbsvr.sv_savetm, server.sv_savetm);

	if (pbs_db_load_obj(conn, &obj) == 0) {
		if (db_2_svr(&server, &dbsvr) == 0)
			rc = 0;
	}

	free_db_attr_list(&dbsvr.db_attr_list);
	free_db_attr_list(&dbsvr.cache_attr_list);

	return rc;
}

/**
 * @brief
 *		Save the state of the server, server quick save sub structure and
 *		optionally the attributes.
 *
 * @param[in]	ps   -	Pointer to struct server
 * @param[in]	mode -  type of save, either SVR_SAVE_QUICK or SVR_SAVE_FULL
 *
 * @return	Error code
 * @retval	 0	: Successful save of data.
 * @retval	-1	: Failure
 *
 */

int
svr_save_db(struct server *ps)
{
	pbs_db_conn_t *conn = (pbs_db_conn_t *) svr_db_conn;
	pbs_db_svr_info_t dbsvr = {{0}};
	pbs_db_obj_info_t obj;
	int savetype;
	int rc = -1;

	/* as part of the server save, update svrlive file now,
	 * used in failover
	 */
	if (update_svrlive() != 0)
		goto done;

	if ((savetype = svr_2_db(ps, &dbsvr)) == -1)
		goto done;

	obj.pbs_db_obj_type = PBS_DB_SVR;
	obj.pbs_db_un.pbs_db_svr = &dbsvr;

	if (pbs_db_save_obj(conn, &obj, savetype) == 0) {
		strcpy(ps->sv_savetm, dbsvr.sv_savetm);
		rc = 0;
	}

done:
	free_db_attr_list(&dbsvr.db_attr_list);
	free_db_attr_list(&dbsvr.cache_attr_list);

	if (rc != 0) {
		strcpy(log_buffer, msg_svdbnosv);
		if (conn->conn_db_err != NULL)
			strncat(log_buffer, conn->conn_db_err, LOG_BUF_SIZE - strlen(log_buffer) - 1);
		log_err(-1, __func__, log_buffer);

		panic_stop_db(log_buffer);
	}

	return (rc);
}

/**
 * @brief Recover Schedulers
 *
 * @see	pbsd_init.c
 *
 *
 * @return	Error code
 * @retval	 0 :	On successful recovery and creation of server structure
 * @retval	-1 :	On failure to open or read file.
 * @retval	-2 :	No schedulers found.
 * */

pbs_sched *
sched_recov_db(char *sname, pbs_sched *ps)
{
	pbs_db_sched_info_t	dbsched = {{0}};
	pbs_db_obj_info_t	obj;
	pbs_db_conn_t		*conn = (pbs_db_conn_t *) svr_db_conn;
	int rc = -1;

	if (ps)
		strcpy(dbsched.sched_savetm, ps->sc_savetm);
	else {
		dbsched.sched_savetm[0] = '\0';
		if ((ps = sched_alloc(sname)) == NULL) {
			log_err(-1, __func__, "sched_alloc failed");
			return NULL;
		}
	}

	obj.pbs_db_obj_type = PBS_DB_SCHED;
	obj.pbs_db_un.pbs_db_sched = &dbsched;

	/* load sched */
	snprintf(dbsched.sched_name, sizeof(dbsched.sched_name), "%s", sname);

	rc = pbs_db_load_obj(conn, &obj);
	if (rc == -2)
		return ps; /* no change in sched */

	if (rc == 0) {
		rc = db_2_sched(ps, &dbsched);
		
		free_db_attr_list(&dbsched.db_attr_list);
		free_db_attr_list(&dbsched.cache_attr_list);

		if (rc == 0)
			return (ps);
	}

	/* error */
	if (ps)
		sched_free(ps);
	
	return NULL;
}


/**
 * @brief
 *		Save the state of the scheduler structure which consists only of attributes
 *
 * @param[in]	ps   -	Pointer to struct sched
 * @param[in]	mode -  type of save, only SVR_SAVE_FULL
 *
 * @return	Error code
 * @retval	 0 :	Successful save of data.
 * @retval	-1 :	Failure
 *
 */

int
sched_save_db(pbs_sched *ps)
{
	pbs_db_conn_t *conn = (pbs_db_conn_t *) svr_db_conn;
	pbs_db_sched_info_t dbsched = {{0}};
	pbs_db_obj_info_t obj;
	int savetype;
	int rc = -1;

	if ((savetype = sched_2_db(ps, &dbsched)) == -1)
		goto done;

	obj.pbs_db_obj_type = PBS_DB_SCHED;
	obj.pbs_db_un.pbs_db_sched = &dbsched;

	if (pbs_db_save_obj(conn, &obj, savetype) == 0) {
		strcpy(ps->sc_savetm, dbsched.sched_savetm);
		rc = 0;
	}

done:
	free_db_attr_list(&dbsched.db_attr_list);
	free_db_attr_list(&dbsched.cache_attr_list);

	if (rc != 0) {
		sprintf(log_buffer, "Failed to save sched %s ", ps->sc_name);
		if (conn->conn_db_err != NULL)
			strncat(log_buffer, conn->conn_db_err, LOG_BUF_SIZE - strlen(log_buffer) - 1);
		log_err(-1, __func__, log_buffer);

		panic_stop_db(log_buffer);
	}

	return rc;
}
