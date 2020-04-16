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

#ifndef __LIB_SHARD_H
#define __LIB_SHARD_H
#ifdef  __cplusplus
extern "C" {
#endif


#ifdef WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

typedef struct server_instance {
    char *hostname;
    int port;
} server_instance_t;

/**
 * @brief
 * This API  initializes the information needed in the shard library. 
 * It should be called once at the startup of the client so that 
 * the shard library would work on these parameters. 
 *
 * @param[in]	max_allowed_servers - Max number of PBS servers as specified in pbs.conf file by the admin.
 * @param[in]	server_instances - Array of server_instance object, each object contains the server name, port
 * @param[in]	num_instances - Numbers of server instances currently configured in the complex.
 *
 * @return	int
 * @retval  0	- success
 * @retval -1	- error
 */

extern DLLEXPORT int pbs_shard_init(int max_allowed_servers, server_instance_t **server_instances, int num_instances);

/**
 * @brief
 * This API uses the internal sharding logic to identify the right server instance.
 * If the chosen server is not active, the logic will return the next active server instance
 * by referring to the array of configured server instances and an array of inactive server instances.
 * However, the caller should maintain the same series of server instances received from the last call.
 * This interface is used in a client-side application. 
 *
 * @param[in]	obj_id - The identifier used to find the respective server.
 * @param[in]	obj_type - This would specify the object type. 
 * @param[in]	inactive_servers - The array of failed servers list, 
 * 		the caller would update the failed server before the next call. 
 *
 * @return      int
 * @retval !=-1 - success, returns one of the index value of server_instance array
 * @retval -1	- error
 */

extern DLLEXPORT int pbs_shard_get_server_byindex(int obj_type, char *obj_id, int *inactive_servers);

/**
 * @brief
 * This API generates the next sequence ID.  
 * It accepts the current seq id and maximum seq id to compute the next one in series. 
 * This function would apply the "max_seq_id" limitations and make sure 
 * the generated ID is less than the maximum allowed sequence ID. 
 *
 * @param[in]	curr_seq_id - The current seq ID in the PBS server.
 * @param[in]	max_seq_id  - The limitation in the PBS SERVER on an object ID, 
 * 			      which needs to be considered while generating the new object ID. 
 * @param[in]	svr_index   - The caller server index.
 *
 * @return long long
 * @retval !=-1 - success, the next obj_id to use is returned.
 * @retval -1 -  error
 */

extern DLLEXPORT long long pbs_shard_get_next_seqid(long long curr_seq_id, long long max_seq_id, int svr_index);

#ifdef  __cplusplus
}
#endif
#endif /* __LIB_SHARD_H */
