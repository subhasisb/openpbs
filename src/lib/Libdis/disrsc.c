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

/**
 * @file	disrsc.c
 *
 * @par Synopsis:
 *	signed char disrsc(int stream, int *retval)
 *
 *	Gets a Data-is-Strings signed integer from <stream>, converts it into a
 *	signed char, and returns it.  The signed integer in <stream> consists
 *	of a counted string of digits, starting with a zero or a minus sign,
 *	which represents the number.  If the number doesn't lie between -9 and
 *	9, inclusive, it is preceeded by at least one count.
 *
 *	This format for character strings representing signed integers can best
 *	be understood through the decoding algorithm:
 *
 *	1. Initialize the digit count to 1.
 *
 *	2. Read the next digit; if it is a sign, go to step (4).
 *
 *	3. Decode a new count from the digit decoded in step (2) and the next
 *	   count - 1 digits; repeat step (2).
 *
 *	4. Decode the next count digits as the magnitude of the signed integer.
 *
 *	*<retval> gets DIS_SUCCESS if everything works well.  It gets an error
 *	code otherwise.  In case of an error, the <stream> character pointer is
 *	reset, making it possible to retry with some other conversion strategy.
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <assert.h>
#include <stddef.h>

#include "dis.h"
#include "dis_.h"
#undef disrsc

/**
 * @brief
 *      -Gets a Data-is-Strings signed integer from <stream>, converts it into a
 *      signed char, and returns it.  The signed integer in <stream> consists
 *      of a counted string of digits, starting with a zero or a minus sign,
 *      which represents the number.  If the number doesn't lie between -9 and
 *      9, inclusive, it is preceeded by at least one count.
 *
 * @param[in] stream - socket descriptor
 * @param[out] retval - dis status val
 *
 * @return      signed char
 * @retval      signed char val		success
 * @retval	0			error
 *
 */
signed char
disrsc(int stream, int *retval)
{
	int		locret;
	int		negate;
	signed char	value;
	unsigned int	uvalue;

	assert(retval != NULL);

	value = 0;
	switch (locret = disrsi_(stream, &negate, &uvalue, 1, 0)) {
		case DIS_SUCCESS:
			if (negate ? -uvalue >= SCHAR_MIN : uvalue <= SCHAR_MAX) {
				value = negate ? -uvalue : uvalue;
				break;
			} else
				locret = DIS_OVERFLOW;
		case DIS_OVERFLOW:
			value = negate ? SCHAR_MIN : SCHAR_MAX;
	}
	*retval = locret;
	return (value);
}
