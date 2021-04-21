/*
 * Copyright (C) 1994-2020 Altair Engineering, Inc.
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

#include <pbs_config.h>
#include "pbs_nodes.h"
#include "list_link.h"

extern	pbs_list_head	svr_allnodes_timed;
void update_node_timedlist(pbsnode *);

/**
 * @brief	Get attribute of node based on given attr index
 *
 * @param[in] pnode    - pointer to node struct
 * @param[in] attr_idx - attribute index
 *
 * @return attribute *
 * @retval NULL  - failure
 * @retval !NULL - pointer to attribute struct
 */
attribute *
get_nattr(const struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL)
		return _get_attr_by_idx((attribute *)pnode->nd_attr, attr_idx);
	return NULL;
}

/**
 * @brief	Getter function for node attribute of type string
 *
 * @param[in]	pnode - pointer to the node
 * @param[in]	attr_idx - index of the attribute to return
 *
 * @return	char *
 * @retval	string value of the attribute
 * @retval	NULL if pnode is NULL
 */
char *
get_nattr_str(const struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL)
		return get_attr_str(get_nattr(pnode, attr_idx));

	return NULL;
}

/**
 * @brief	Getter function for node attribute of type string of array
 *
 * @param[in]	pnode - pointer to the node
 * @param[in]	attr_idx - index of the attribute to return
 *
 * @return	struct array_strings *
 * @retval	value of the attribute
 * @retval	NULL if pnode is NULL
 */
struct array_strings *
get_nattr_arst(const struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL)
		return get_attr_arst(get_nattr(pnode, attr_idx));

	return NULL;
}

/**
 * @brief	Getter for node attribute's list value
 *
 * @param[in]	pnode - pointer to the node
 * @param[in]	attr_idx - index of the attribute to return
 *
 * @return	pbs_list_head
 * @retval	value of attribute
 */
pbs_list_head
get_nattr_list(const struct pbsnode *pnode, int attr_idx)
{
	return get_attr_list(get_nattr(pnode, attr_idx));
}

/**
 * @brief	Getter function for node attribute of type long
 *
 * @param[in]	pnode - pointer to the node
 * @param[in]	attr_idx - index of the attribute to return
 *
 * @return	long
 * @retval	long value of the attribute
 * @retval	-1 if pnode is NULL
 */
long
get_nattr_long(const struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL)
		return get_attr_l(get_nattr(pnode, attr_idx));

	return -1;
}

/**
 * @brief	Getter function for node attribute of type char
 *
 * @param[in]	pnode - pointer to the node
 * @param[in]	attr_idx - index of the attribute to return
 *
 * @return	char
 * @retval	char value of the attribute
 * @retval	-1 if pnode is NULL
 */
char
get_nattr_c(const struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL)
		return get_attr_c(get_nattr(pnode, attr_idx));

	return -1;
}

/**
 * @brief	Generic node attribute setter (call if you want at_set() action functions to be called)
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - new val to set
 * @param[in]	rscn - new resource val to set, if applicable
 * @param[in]	op - batch_op operation, SET, INCR, DECR etc.
 *
 * @return	int
 * @retval	0 for success
 * @retval	!0 for failure
 */
int
set_nattr_generic(struct pbsnode *pnode, int attr_idx, char *val, char *rscn, enum batch_op op)
{
	if (pnode == NULL || val == NULL)
		return 1;

	update_node_timedlist(pnode);
	return set_attr_generic(get_nattr(pnode, attr_idx), &node_attr_def[attr_idx], val, rscn, op);
}

/**
 * @brief	"fast" node attribute setter for string values
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - new val to set
 * @param[in]	rscn - new resource val to set, if applicable
 *
 * @return	int
 * @retval	0 for success
 * @retval	!0 for failure
 */
int
set_nattr_str_slim(struct pbsnode *pnode, int attr_idx, char *val, char *rscn)
{
	if (pnode == NULL || val == NULL)
		return 1;

	update_node_timedlist(pnode);
	return set_attr_generic(get_nattr(pnode, attr_idx), &node_attr_def[attr_idx], val, rscn, INTERNAL);
}

/**
 * @brief	"fast" node attribute setter for long values
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - new val to set
 * @param[in]	op - batch_op operation, SET, INCR, DECR etc.
 *
 * @return	int
 * @retval	0 for success
 * @retval	1 for failure
 */
int
set_nattr_l_slim(struct pbsnode *pnode, int attr_idx, long val, enum batch_op op)
{
	if (pnode == NULL)
		return 1;

	update_node_timedlist(pnode);
	set_attr_l(get_nattr(pnode, attr_idx), val, op);

	return 0;
}

/**
 * @brief	"fast" node attribute setter for boolean values
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - new val to set
 * @param[in]	op - batch_op operation, SET, INCR, DECR etc.
 *
 * @return	int
 * @retval	0 for success
 * @retval	1 for failure
 */
int
set_nattr_b_slim(struct pbsnode *pnode, int attr_idx, long val, enum batch_op op)
{
	if (pnode == NULL)
		return 1;

	update_node_timedlist(pnode);
	set_attr_b(get_nattr(pnode, attr_idx), val, op);

	return 0;
}

/**
 * @brief	"fast" node attribute setter for char values
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - new val to set
 * @param[in]	op - batch_op operation, SET, INCR, DECR etc.
 *
 * @return	int
 * @retval	0 for success
 * @retval	1 for failure
 */
int
set_nattr_c_slim(struct pbsnode *pnode, int attr_idx, char val, enum batch_op op)
{
	if (pnode == NULL)
		return 1;

	update_node_timedlist(pnode);
	set_attr_c(get_nattr(pnode, attr_idx), val, op);

	return 0;
}

/**
 * @brief	"fast" node attribute setter for short values
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - new val to set
 * @param[in]	op - batch_op operation, SET, INCR, DECR etc.
 *
 * @return	int
 * @retval	0 for success
 * @retval	1 for failure
 */
int
set_nattr_short_slim(struct pbsnode *pnode, int attr_idx, short val, enum batch_op op)
{
	if (pnode == NULL)
		return 1;

	update_node_timedlist(pnode);
	set_attr_short(get_nattr(pnode, attr_idx), val, op);

	return 0;
}


/**
 * @brief	Check if a node attribute is set
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to check
 *
 * @return	int
 * @retval	1 if it is set
 * @retval	0 otherwise
 */
int
is_nattr_set(const struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL)
		return is_attr_set(get_nattr(pnode, attr_idx));

	return 0;
}

/**
 * @brief	Free a node attribute
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to free
 *
 * @return	void
 */
void
free_nattr(struct pbsnode *pnode, int attr_idx)
{
	attribute *pattr = get_nattr(pnode, attr_idx);
	update_node_timedlist(pnode);
	if (pnode != NULL)
		free_attr(node_attr_def, pattr, attr_idx);
#ifndef PBS_MOM
	post_attr_set_unset(pattr);
#endif
}

/**
 * @brief	clear a node attribute
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to clear
 *
 * @return	void
 */
void
clear_nattr(struct pbsnode *pnode, int attr_idx)
{
	attribute *pattr = get_nattr(pnode, attr_idx);
	update_node_timedlist(pnode);
	clear_attr(pattr, &node_attr_def[attr_idx]);
#ifndef PBS_MOM
	post_attr_set_unset(pattr);
#endif
}

/**
 * @brief	Mark a node attribute as "set"
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 *
 * @return	void
 */
void
mark_nattr_set(struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL) {
		mark_attr_set(get_nattr(pnode, attr_idx));
#ifndef PBS_MOM
		update_node_timedlist(pnode);
#endif
	}
}

/**
 * @brief	Mark a node attribute as "not set"
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 *
 * @return	void
 */
void
mark_nattr_not_set(struct pbsnode *pnode, int attr_idx)
{
	if (pnode != NULL) {
		mark_attr_not_set(get_nattr(pnode, attr_idx));
#ifndef PBS_MOM
		update_node_timedlist(pnode);
#endif
	}
}

/**
 * @brief	Special setter func to set node's job info value
 *
 * @param[in]	pnode - pointer to node
 * @param[in]	attr_idx - attribute index to set
 * @param[in]	val - pointer to node as value to set
 *
 * @return	void
 */
void
set_nattr_jinfo(struct pbsnode *pnode, int attr_idx, struct pbsnode *val)
{
	attribute *attr = get_nattr(pnode, attr_idx);
	attr->at_val.at_jinfo = val;
	attr->at_flags = ATR_SET_MOD_MCACHE;
	update_node_timedlist(pnode);
}

/**
 * @brief
 * 		set this node at the right place in the jobs list sorted on update time
 *
 * @param[in]	pnode - node pointer
 *
 * @return	void
 *
 * @par MT-Safe: Yes
 * @par Side Effects: None
 *
 */
void
update_node_timedlist(pbsnode *pnode)
{
#ifndef PBS_MOM
	/* the time updation functionlity is required only for server daemon */
	pbsnode *platest;

	if (!pnode) 
		return; /* null, newobj, or dup'd node for hook processing, ignore */
	 
	gettimeofday(&pnode->update_tm, NULL); /* update the current timestamp */ 

	platest = (pbsnode *) GET_PRIOR(svr_allnodes_timed);
	if (platest == pnode)
		return; /* all set */

	if (platest == NULL)
		insert_link(&svr_allnodes_timed, &pnode->nd_allnodes_timed, pnode, LINK_INSET_AFTER); /* link first in server's list */
	else {
		delete_link(&pnode->nd_allnodes_timed);
		insert_link(&platest->nd_allnodes_timed, &pnode->nd_allnodes_timed, pnode, LINK_INSET_AFTER); /* link after 'latest' node in server's list */
	}
#endif
}