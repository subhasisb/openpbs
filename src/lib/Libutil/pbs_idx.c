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

#include "pbs_idx.h"
#include "avltree.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* iteration context structure, opaque to application */
typedef struct _iter_ctx {
	void *idx; /* pointer to idx */
	AVL_IX_REC *pkey; /* pointer to key used while iteration */
} iter_ctx;

/**
 * @brief
 *	Create an empty index
 *
 * @param[in] - flags  - index flags like duplicates allowed, or case insensitive compare
 * @param[in] - keylen - length of key in index (can be 0 for default size)
 *
 * @return void *
 * @retval !NULL - success
 * @retval NULL  - failure
 *
 */

typedef struct {
	void *idx;
	pthread_mutex_t mutex;
} pbs_index_t;

void *
pbs_idx_create(int flags, int keylen)
{
	void *idx = NULL;
	pbs_index_t *pbs_idx = malloc(sizeof(pbs_index_t));

	idx = malloc(sizeof(AVL_IX_DESC));
	if (idx == NULL)
		return NULL;

	if (avl_create_index(idx, flags, keylen)) {
		free(idx);
		return NULL;
	}

	pbs_idx->idx = idx;
	pthread_mutex_init(&pbs_idx->mutex, NULL);

	return (void *) pbs_idx;
}

/**
 * @brief
 *	destroy index
 *
 * @param[in] - idx - pointer to index
 *
 * @return void
 *
 */
void
pbs_idx_destroy(void *idx)
{
	pbs_index_t *pbs_idx = idx;
	if (idx != NULL) {
		avl_destroy_index(pbs_idx->idx);
		free(pbs_idx->idx);
		free(pbs_idx);
		idx = NULL;
	}
}

/**
 * @brief
 *	add entry in index
 *
 * @param[in] - idx  - pointer to index
 * @param[in] - key  - key of entry
 * @param[in] - data - data of entry
 *
 * @return int
 * @retval PBS_IDX_RET_OK   - success
 * @retval PBS_IDX_RET_FAIL - failure
 *
 */
int
pbs_idx_insert(void *idx, void *key, void *data)
{
	AVL_IX_REC *pkey;
	pbs_index_t *pbs_idx = idx;

	if (idx == NULL || key == NULL)
		return PBS_IDX_RET_FAIL;

	pthread_mutex_lock(&pbs_idx->mutex);

	pkey = avlkey_create(pbs_idx->idx, key);
	if (pkey == NULL) {
		pthread_mutex_unlock(&pbs_idx->mutex);
		return PBS_IDX_RET_FAIL;
	}

	pkey->recptr = data;
	if (avl_add_key(pkey, pbs_idx->idx) != AVL_IX_OK) {
		pthread_mutex_unlock(&pbs_idx->mutex);
		free(pkey);
		return PBS_IDX_RET_FAIL;
	}
	free(pkey);
	pthread_mutex_unlock(&pbs_idx->mutex);
	return PBS_IDX_RET_OK;
}

/**
 * @brief
 *	delete entry from index
 *
 * @param[in] - idx - pointer to index
 * @param[in] - key - key of entry
 *
 * @return int
 * @retval PBS_IDX_RET_OK   - success
 * @retval PBS_IDX_RET_FAIL - failure
 *
 */
int
pbs_idx_delete(void *idx, void *key)
{
	AVL_IX_REC *pkey;
	pbs_index_t *pbs_idx = idx;


	if (idx == NULL || key == NULL)
		return PBS_IDX_RET_FAIL;

	pthread_mutex_lock(&pbs_idx->mutex);

	pkey = avlkey_create(idx, key);
	if (pkey == NULL) {
		pthread_mutex_unlock(&pbs_idx->mutex);
		return PBS_IDX_RET_FAIL;
	}

	pkey->recptr = NULL;
	avl_delete_key(pkey, pbs_idx->idx);
	free(pkey);

	pthread_mutex_unlock(&pbs_idx->mutex);
	return PBS_IDX_RET_OK;
}

/**
 * @brief
 *	delete exact entry from index using given context
 *
 * @param[in] - ctx - pointer to context used while
 *                    deleting exact entry in index
 *
 * @return int
 * @retval PBS_IDX_RET_OK   - success
 * @retval PBS_IDX_RET_FAIL - failure
 *
 */
int
pbs_idx_delete_byctx(void *ctx)
{
	iter_ctx *pctx = (iter_ctx *) ctx;
	pbs_index_t *pbs_idx = pctx->idx;

	if (pctx == NULL || pctx->idx == NULL || pctx->pkey == NULL)
		return PBS_IDX_RET_FAIL;

	pthread_mutex_lock(&pbs_idx->mutex);

	avl_delete_key(pctx->pkey, pctx->idx);
	
	pthread_mutex_unlock(&pbs_idx->mutex);
	return PBS_IDX_RET_OK;
}

/**
 * @brief
 *	find or iterate entry in index
 *
 * @param[in]     - idx  - pointer to index
 * @param[in/out] - key  - key of the entry
 *                         if *key is NULL then this routine will
 *                         return the first entry in index
 * @param[in/out] - data - data of the entry
 * @param[in/out] - ctx  - context to be set for iteration
 *                         can be NULL, if caller doesn't want
 *                         iteration context
 *                         if *ctx is not NULL, then this routine
 *                         will return next entry in index
 *
 * @return int
 * @retval PBS_IDX_RET_OK   - success
 * @retval PBS_IDX_RET_FAIL - failure
 *
 * @note
 * 	ctx should be free'd after use, using pbs_idx_free_ctx()
 *
 */
int
pbs_idx_find(void *pbs_idx, void **key, void **data, void **ctx)
{
	iter_ctx *pctx;
	AVL_IX_REC *pkey;
	int rc = AVL_IX_FAIL;
	void *idx = ((pbs_index_t *) pbs_idx)->idx;

	if (idx == NULL || data == NULL)
		return PBS_IDX_RET_FAIL;

	pthread_mutex_lock(&((pbs_index_t *) pbs_idx)->mutex);

	if (ctx != NULL && *ctx != NULL) {
		pctx = (iter_ctx *) *ctx;

		*data = NULL;
		if (key)
			*key = NULL;

		if (pctx->idx != idx || pctx->pkey == NULL) {
			pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
			return PBS_IDX_RET_FAIL;
		}

		if (avl_next_key(pctx->pkey, pctx->idx) != AVL_IX_OK) {
			pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
			return PBS_IDX_RET_FAIL;
		}

		*data = pctx->pkey->recptr;
		if (key)
			*key = &pctx->pkey->key;

		pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
		return PBS_IDX_RET_OK;
	} else {
		*data = NULL;
		pkey = avlkey_create(idx, key ? *key : NULL);
		if (pkey == NULL) {
			pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
			return PBS_IDX_RET_FAIL;
		}

		if (key != NULL && *key != NULL) {
			rc = avl_find_key(pkey, idx);
		} else {
			avl_first_key(idx);
			rc = avl_next_key(pkey, idx);
		}

		if (rc == AVL_IX_OK) {
			*data = pkey->recptr;
			if (key != NULL && *key == NULL)
				*key = &pkey->key;
			if (ctx != NULL) {
				pctx = (iter_ctx *) malloc(sizeof(iter_ctx));
				if (pctx == NULL) {
					free(pkey);
					pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
					return PBS_IDX_RET_FAIL;
				}
				pctx->idx = idx;
				pctx->pkey = pkey;
				*ctx = (void *) pctx;

				pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
				return PBS_IDX_RET_OK;
			}
		}
		free(pkey);
	}

	pthread_mutex_unlock(&((pbs_index_t *) pbs_idx)->mutex);
	return rc == AVL_IX_OK ? PBS_IDX_RET_OK : PBS_IDX_RET_FAIL;
}


/**
 * @brief
 *	find or iterate entry in index
 *
 * @param[in] - ctx  - context to be set for iteration
 *                         can be NULL, if caller doesn't want
 *                         iteration context
 *                         if *ctx is not NULL, then this routine
 *                         will return next entry in index
 * @param[in/out] - data - data of the entr
 *
 * @return int
 * @retval PBS_IDX_RET_OK   - success
 * @retval PBS_IDX_RET_FAIL - failure
 *
 * @note
 * 	ctx should be free'd after use, using pbs_idx_free_ctx()
 *
 */
int 
pbs_idx_update(void *ctx, void *data)
{
	iter_ctx *pctx;

	if (ctx != NULL) {
		pctx = (iter_ctx *) ctx;
		pctx->pkey->recptr = data;
	}
	return 0;
}

/**
 * @brief
 *	free given iteration context
 *
 * @param[in] - ctx - pointer to context for iteration
 *
 * @return void
 *
 */
void
pbs_idx_free_ctx(void *ctx)
{
	if (ctx != NULL) {
		iter_ctx *pctx = (iter_ctx *) ctx;
		free(pctx->pkey);
		free(ctx);
		ctx = NULL;
	}
}
