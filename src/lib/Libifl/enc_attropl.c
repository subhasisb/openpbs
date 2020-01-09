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
 * @file	enc_attropl.c
 * @brief
 * encode_wire_attropl() - encode a list of PBS API "attropl" structures
 *
 *	The first item encoded is a unsigned integer, a count of the
 *	number of attropl entries in the linked list.  This is encoded
 *	even when there are no attropl entries in the list.
 *
 * @par Each individual entry is then encoded as:
 *		u int	size of the three strings (name, resource, value)
 *			including the terminating nulls
 *		string	attribute name
 *		u int	1 or 0 if resource name does or does not follow
 *		string	resource name (if one)
 *		string  value of attribute/resource
 *		u int	"op" of attrlop
 * @note
 *	the encoding of a attropl is the same as the encoding of
 *	the pbs_ifl.h structures "attrl" and the server svrattrl.  Any
 *	one of the three forms can be decoded into any of the three with the
 *	possible loss of the "flags" field (which is the "op" of the attrlop).
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include "pbs_ifl.h"
#include "dis.h"

/**
 * @brief
 *	- encode a list of PBS API "attropl" structures
 *
 * @par	Note:
 *	The first item encoded is a unsigned integer, a count of the
 *      number of attropl entries in the linked list.  This is encoded
 *      even when there are no attropl entries in the list.
 *
 * @par	 Each individual entry is then encoded as:\n
 *			u int   size of the three strings (name, resource, value)
 *                      	including the terminating nulls\n
 *			string  attribute name\n
 *			u int   1 or 0 if resource name does or does not follow\n
 *			string  resource name (if one)\n
 *			string  value of attribute/resource\n
 *			u int   "op" of attrlop\n
 *
 * @par	Note:
 *	the encoding of a attropl is the same as the encoding of
 *      the pbs_ifl.h structures "attrl" and the server svrattrl.  Any
 *      one of the three forms can be decoded into any of the three with the
 *      possible loss of the "flags" field (which is the "op" of the attrlop).
 *
 * @param[in] sock - socket id
 * @param[in] pattropl - pointer to attropl structure
 *
 * @return      int
 * @retval      DIS_SUCCESS(0)  success
 * @retval      error code      error
 *
 */

flatbuffers_ref_t
encode_wire_attropl(void *buf, struct attropl *pattropl)
{
	struct attropl *ps;
	flatcc_builder_t *B = (flattcc_builder_t *) buf;
	flatbuffers_string_ref_t name, resc, value, op;

	ns(Attribute_vec_start(B));

	for (ps = pattropl; ps; ps = ps->next) {
		name = flatbuffers_string_create_str(B, ps->name);
		if (ps->resource)
			resc = flatbuffers_string_create_str(B, ps->resource);
		value = flatbuffers_string_create_str(B, ps->value);
		op = flatbuffers_string_create_str(B, ps->op);
		
		ns(Attribute_ref_t) attr = ns(Attribute_create(B, name, resc, value, op));
		ns(Attribute_vec_push(B, attr));
	}
	
	return ((ns(Attribute_vec_end(B)));
}
