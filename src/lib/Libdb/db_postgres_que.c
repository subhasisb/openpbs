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
 * @file    db_postgres_que.c
 *
 * @brief
 *      Implementation of the queue data access functions for postgres
 */

#include <pbs_config.h>   /* the master config generated by configure */
#include "pbs_db.h"
#include "db_postgres.h"

/**
 * @brief
 *	Prepare all the queue related sqls. Typically called after connect
 *	and before any other sql exeuction
 *
 * @param[in]	conn - Database connection handle
 *
 * @return      Error code
 * @retval	-1 - Failure
 * @retval	 0 - Success
 *
 */
int
pg_db_prepare_que_sqls(pbs_db_conn_t *conn)
{
	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "insert into pbs.queue("
		"qu_name, "
		"qu_type, "
		"qu_creattm, "
		"qu_savetm, "
		"attributes "
		") "
		"values "
		"($1, $2,  localtimestamp, localtimestamp, hstore($3::text[])) "
		"returning to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm");
	if (pg_prepare_stmt(conn, STMT_INSERT_QUE, conn->conn_sql, 3) != 0)
		return -1;

	/* rewrite all attributes for FULL update */
	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "update pbs.queue set "
			"qu_type = $2, "
			"qu_savetm = localtimestamp, "
			"attributes = attributes || hstore($3::text[]) "
			"where qu_name = $1 "
			"returning to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm");
	if (pg_prepare_stmt(conn, STMT_UPDATE_QUE, conn->conn_sql, 3) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "update pbs.queue set "
			"qu_type = $2, "
			"qu_savetm = localtimestamp "
			"where qu_name = $1 "
			"returning to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm");
	if (pg_prepare_stmt(conn, STMT_UPDATE_QUE_QUICK, conn->conn_sql, 2) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "update pbs.queue set "
			"qu_savetm = localtimestamp, "
			"attributes = attributes || hstore($2::text[]) "
			"where qu_name = $1 "
			"returning to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm");
	if (pg_prepare_stmt(conn, STMT_UPDATE_QUE_ATTRSONLY, conn->conn_sql, 2) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "update pbs.queue set "
		"qu_savetm = localtimestamp,"
		"attributes = attributes - $2::text[] "
		"where qu_name = $1 "
		"returning to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm");
	if (pg_prepare_stmt(conn, STMT_REMOVE_QUEATTRS, conn->conn_sql, 2) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "select qu_name, "
			"qu_type, "
			"to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm, "
			"hstore_to_array(attributes) as attributes "
			"from pbs.queue "
			"where qu_name = $1");
	if (pg_prepare_stmt(conn, STMT_SELECT_QUE, conn->conn_sql, 1) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "select qu_name, "
			"qu_type, "
			"to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm, "
			"hstore_to_array(attributes) as attributes "
			"from pbs.queue "
			"where qu_savetm > to_timestamp($1, 'YYYY-MM-DD HH24:MI:SS:US') "
			"order by qu_savetm ");
	if (pg_prepare_stmt(conn, STMT_FIND_QUES_FROM_TIME_ORDBY_SAVETM, conn->conn_sql, 1) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "select "
			"qu_name, "
			"qu_type, "
			"to_char(qu_savetm, 'YYYY-MM-DD HH24:MI:SS.US') as qu_savetm, "
			"hstore_to_array(attributes) as attributes "
			"from pbs.queue order by qu_creattm");
	if (pg_prepare_stmt(conn, STMT_FIND_QUES_ORDBY_CREATTM, conn->conn_sql, 0) != 0)
		return -1;

	snprintf(conn->conn_sql, MAX_SQL_LENGTH, "delete from pbs.queue where qu_name = $1");
	if (pg_prepare_stmt(conn, STMT_DELETE_QUE, conn->conn_sql, 1) != 0)
		return -1;

	return 0;
}

/**
 * @brief
 *	Load queue data from the row into the queue object
 *
 * @param[in]	res - Resultset from a earlier query
 * @param[in]	pq  - Queue object to load data into
 * @param[in]	row - The current row to load within the resultset
 *
 * @return      Error code
 * @retval	-1 - On Error
 * @retval	 0 - On Success
 * @retval	>1 - Number of attributes
 */
static int
load_que(PGresult *res, pbs_db_que_info_t *pq, int row)
{
	char *raw_array;
	static int qu_name_fnum, qu_type_fnum, qu_savetm_fnum, attributes_fnum;
	static int fnums_inited = 0;

	if (fnums_inited == 0) {
		qu_name_fnum = PQfnumber(res, "qu_name");
		qu_type_fnum = PQfnumber(res, "qu_type");
		qu_savetm_fnum = PQfnumber(res, "qu_savetm");
		attributes_fnum = PQfnumber(res, "attributes");
		fnums_inited = 1;
	}

	GET_PARAM_STR(res, row, pq->qu_name, qu_name_fnum);
	GET_PARAM_INTEGER(res, row, pq->qu_type, qu_type_fnum);
	GET_PARAM_STR(res, row, pq->qu_savetm, qu_savetm_fnum);
	GET_PARAM_BIN(res, row, raw_array, attributes_fnum);

	/* convert attributes from postgres raw array format */
	return (dbarray_2_attrlist(raw_array, &pq->db_attr_list));
}

/**
 * @brief
 *	Insert queue data into the database
 *
 * @param[in]	conn - Connection handle
 * @param[in]	obj  - Information of queue to be inserted
 *
 * @return      Error code
 * @retval	-1 - Failure
 * @retval	 0 - Success
 *
 */
int
pg_db_save_que(pbs_db_conn_t *conn, pbs_db_obj_info_t *obj, int savetype)
{
	pbs_db_que_info_t *pq = obj->pbs_db_un.pbs_db_que;
	char *stmt = NULL;
	int params;
	char *raw_array = NULL;
	static int qu_savetm_fnum;
	static int fnums_inited = 0;

	SET_PARAM_STR(conn, pq->qu_name, 0);

	if (savetype & OBJ_SAVE_QS) {
		SET_PARAM_INTEGER(conn, pq->qu_type, 1);
		params = 2;
		stmt = STMT_UPDATE_QUE_QUICK;
	} 
	
	/* are there attributes to save to memory or local cache? */
	if (pq->cache_attr_list.attr_count > 0) {
		dist_cache_save_attrs(pq->qu_name, &pq->cache_attr_list);
	}

	if ((pq->db_attr_list.attr_count > 0) || (savetype & OBJ_SAVE_NEW)) {
		int len = 0;
		/* convert attributes to postgres raw array format */
		if ((len = attrlist_2_dbarray(&raw_array, &pq->db_attr_list)) <= 0)
			return -1;

		if (savetype & OBJ_SAVE_QS) {
			SET_PARAM_BIN(conn, raw_array, len, 2);
			params = 3;
			stmt = STMT_UPDATE_QUE;
		} else {
			SET_PARAM_BIN(conn, raw_array, len, 1);
			params = 2;
			stmt = STMT_UPDATE_QUE_ATTRSONLY;
		}
	}

	if (savetype & OBJ_SAVE_NEW)
		stmt = STMT_INSERT_QUE;

	if (stmt != NULL) {
		if (pg_db_cmd(conn, stmt, params) != 0) {
			free(raw_array);
			return -1;
		}
		if (fnums_inited == 0) {
			qu_savetm_fnum = PQfnumber(conn->conn_resultset, "qu_savetm");
			fnums_inited = 1;
		}
		GET_PARAM_STR(conn->conn_resultset, 0, pq->qu_savetm, qu_savetm_fnum);
		PQclear(conn->conn_resultset);
		free(raw_array);
	}

	return 0;
}

/**
 * @brief
 *	Load queue data from the database
 *
 * @param[in]	conn - Connection handle
 * @param[in]	obj  - Load queue information into this object
 *
 * @return      Error code
 * @retval	-1 - Failure
 * @retval	 0 - Success
 * @retval	 1 -  Success but no rows loaded
 *
 */
int
pg_db_load_que(pbs_db_conn_t *conn, pbs_db_obj_info_t *obj)
{
	PGresult *res;
	int rc;
	pbs_db_que_info_t *pq = obj->pbs_db_un.pbs_db_que;

	SET_PARAM_STR(conn, pq->qu_name, 0);

	if ((rc = pg_db_query(conn, STMT_SELECT_QUE, 1, &res)) != 0)
		return rc;

	rc = load_que(res, pq, 0);

	PQclear(res);

	if (rc == 0) {
		/* in case of multi-server, also read NOSAVM attributes from distributed cache */
		/* call in this functions since all call paths lead to this before decode */
		//if (use_dist_cache) {
		//	dist_cache_recov_attrs(pq->qu_name, &pq->qu_savetm, &pq->cache_attr_list);
		//}
	}

	return rc;
}

/**
 * @brief
 *	Find queues
 *
 * @param[in]	conn - Connection handle
 * @param[in]	st   - The cursor state variable updated by this query
 * @param[in]	obj  - Information of queue to be found
 * @param[in]	opts - Any other options (like flags, timestamp)
 *
 * @return      Error code
 * @retval	-1 - Failure
 * @retval	 0 - Success
 * @retval	 1 - Success, but no rows found
 *
 */
int
pg_db_find_que(pbs_db_conn_t *conn, void *st, pbs_db_obj_info_t *obj, pbs_db_query_options_t *opts)
{
	PGresult *res;
	int rc;
	int params;
	pg_query_state_t *state = (pg_query_state_t *) st;

	if (!state)
		return -1;

	if (opts != NULL && opts->timestamp) {
		SET_PARAM_STR(conn, opts->timestamp, 0);
		params = 1;
		strcpy(conn->conn_sql, STMT_FIND_QUES_FROM_TIME_ORDBY_SAVETM);
	} else {
		strcpy(conn->conn_sql, STMT_FIND_QUES_ORDBY_CREATTM);
		params = 0;
	}
	if ((rc = pg_db_query(conn, conn->conn_sql, params, &res)) != 0)
		return rc;

	state->row = 0;
	state->res = res;
	state->count = PQntuples(res);

	return 0;
}

/**
 * @brief
 *	Get the next queue from the cursor
 *
 * @param[in]	conn - Connection handle
 * @param[in]	st   - The cursor state
 * @param[in]	obj  - queue information is loaded into this object
 *
 * @return      Error code
 *		(Even though this returns only 0 now, keeping it as int
 *			to support future change to return a failure)
 * @retval	 0 - Success
 *
 */
int
pg_db_next_que(pbs_db_conn_t* conn, void *st, pbs_db_obj_info_t* obj)
{
	pg_query_state_t *state = (pg_query_state_t *) st;
	obj->pbs_db_un.pbs_db_que->qu_savetm[0] = '\0';

	return (load_que(state->res, obj->pbs_db_un.pbs_db_que, state->row));
}

/**
 * @brief
 *	Delete the queue from the database
 *
 * @param[in]	conn - Connection handle
 * @param[in]	obj  - queue information
 *
 * @return      Error code
 * @retval	-1 - Failure
 * @retval	 0 - Success
 * @retval	 1 - Success but no rows deleted
 *
 */
int
pg_db_delete_que(pbs_db_conn_t *conn, pbs_db_obj_info_t *obj)
{
	pbs_db_que_info_t *pq = obj->pbs_db_un.pbs_db_que;
	SET_PARAM_STR(conn, pq->qu_name, 0);
	return (pg_db_cmd(conn, STMT_DELETE_QUE, 1));
}


/**
 * @brief
 *	Deletes attributes of a queue
 *
 * @param[in]	conn - Connection handle
 * @param[in]	obj  - queue information
 * @param[in]	obj_id  - queue id
 * @param[in]	attr_list - List of attributes
 *
 * @return      Error code
 * @retval	 0 - Success
 * @retval	-1 - On Failure
 *
 */
int
pg_db_del_attr_que(pbs_db_conn_t *conn, void *obj_id, char *sv_time, pbs_db_attr_list_t *attr_list)
{
	char *raw_array = NULL;
	int len = 0;
	static int qu_savetm_fnum;
	static int fnums_inited = 0;

	if ((len = attrlist_2_dbarray_ex(&raw_array, attr_list, 1)) <= 0)
		return -1;

	SET_PARAM_STR(conn, obj_id, 0);
	SET_PARAM_BIN(conn, raw_array, len, 1);

	if (pg_db_cmd(conn, STMT_REMOVE_QUEATTRS, 2) != 0) {
		free(raw_array);
		return -1;
	}
	if (fnums_inited == 0) {
		qu_savetm_fnum = PQfnumber(conn->conn_resultset, "qu_savetm");
		fnums_inited = 1;
	}
	GET_PARAM_STR(conn->conn_resultset, 0, sv_time, qu_savetm_fnum);
	PQclear(conn->conn_resultset);
	free(raw_array);

	return 0;
}

