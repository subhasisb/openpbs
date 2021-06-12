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
 * @file	thread_utils.c
 * @brief
 * thread_utils.c - contains utility functions for multi-threading using pthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include "libutil.h"

/**
 * @brief	initialize a mutex attr object
 *
 * @param[out]	attr - the attr object to initialize
 *
 * @return int
 * @retval 0 for Success
 * @retval -1 for Error
 */
int
init_mutex_attr_recursive(void *attr)
{
	if (pthread_mutexattr_init(attr) != 0) {
		return -1;
	}

	if (pthread_mutexattr_settype(attr,
#if defined (linux)
			PTHREAD_MUTEX_RECURSIVE_NP
#else
			PTHREAD_MUTEX_RECURSIVE
#endif
	)) {
		return -1;
	}

	return 0;
}

static pthread_key_t app_key_tls;
static pthread_once_t app_once_ctrl = PTHREAD_ONCE_INIT; /* once ctrl to initialize tls key */

static void
app_init_tls_key_once(void)
{
	if (pthread_key_create(&app_key_tls, NULL) != 0) {
		fprintf(stderr, "Failed to initialize TLS key\n");
	}
}

int
init_tls_key()
{
	if (pthread_once(&app_once_ctrl, app_init_tls_key_once) != 0)
		return -1;
	return 0;
}

/**
 * @brief
 *	Get the data from the thread TLS
 *
 * @return	Pointer of the tpp_thread_data structure from threads TLS
 * @retval	NULL - Pthread functions failed
 * @retval	!NULl - Data from TLS
 *
 * @par Side Effects:
 *	None
 *
 * @par MT-safe: Yes
 *
 */
tls_t *
get_tls()
{
	tls_t *ptr;
	if ((ptr = pthread_getspecific(app_key_tls)) == NULL) {
		ptr = calloc(1, sizeof(tls_t));
		if (!ptr)
			return NULL;

		ptr->staticbuf = NULL;
		ptr->staticbufsize = 0;
		ptr->thread_index = -1;

		if (pthread_setspecific(app_key_tls, ptr) != 0) {
			free(ptr);
			return NULL;
		}
	}
	return (tls_t *) ptr; /* thread data already initialized */
}