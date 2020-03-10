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
 * @file	shard_functions.c
 *
 * @brief	Miscellaneous utility routines used by the shard library
 *
 *
 */
#include <pbs_config.h>
#include <ctype.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <math.h>
#include "shard_internal.h"

int pbs_shard_init(int max_allowed_servers, struct server_instance **server_instances, int num_instances);

int pbs_shard_get_server_byindex(char *obj_id, enum obj_type_t obj_type, int *inactive_servers);

long long pbs_shard_get_next_seqid(long long curr_seq_id_seq_id, long long max_seq_id);

long long pbs_shard_get_last_seqid(long long highest_seqid);

int pbs_shard_get_index(struct server_instance *instance, int from_server);

int compute_srv_ind(char *obj_id);

int max_num_of_servers = 0;
struct server_instance **configured_servers = NULL;
int configured_num_servers = 0;
static int svr_index = -1;

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
int 
pbs_shard_init(int max_allowed_servers, struct server_instance **server_instances, int num_instances) {
        
        if (server_instances == NULL)
                return -1;
        max_num_of_servers = max_allowed_servers;
        configured_servers = server_instances;
        configured_num_servers = num_instances;
        return 0;
}

/**
 * @brief
 * This API computes the server index based on internal hasing. 
 *
 * @param[in]	id - object identifier.
 *
 */
int compute_srv_ind(char *id) {
        int nshardid = 0;
        nshardid = strtoull(id, NULL, 10);
        return (nshardid % max_num_of_servers);
}

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
 *                                 the caller would update the failed server before the next call. 
 *
 * @return      int
 * @retval !=-1 - success, returns one of the index value of server_instance array
 * @retval -1	- error
 */
int
pbs_shard_get_server_byindex(char *obj_id, enum obj_type_t obj_type, int *inactive_servers)
{
        int srv_ind = 0;
        int loop_i = 0;
        static int seeded = 0;
        struct timeval tv;
        unsigned long time_in_micros;
        int counter = 0;

        if (!max_num_of_servers || !configured_servers)
                return -1;

        if (obj_id) {
                srv_ind = compute_srv_ind(obj_id);
        } else {
                if (!seeded) {
                        gettimeofday(&tv,NULL);
                        time_in_micros = 1000000 * tv.tv_sec + tv.tv_usec;
                        srand(time_in_micros); /* seed the random generator */
                        seeded = 1;
                }
                srv_ind = rand() % configured_num_servers;
        }

 check_again:
        if (counter >= configured_num_servers)
                return -1;
        loop_i = 0;    
        for(; loop_i < configured_num_servers && inactive_servers[loop_i] != -1; loop_i++) {
                if (srv_ind == inactive_servers[loop_i]) {
                        srv_ind = (srv_ind + 1) % configured_num_servers;
                        counter++;                                 
                        goto check_again;
                }
        }     
        return srv_ind;
}

/**
 * @brief
 * a) This API initializes the sharding library’s internal variable(svr_index),
 * which is used in pbs_shard_get_next_seqid() and pbs_shard_get_last_seqid() 
 * b) This API is used to check the multiserver configuration while starting the server 
 * and c) this API also returns the server index based on the given server_instance object.     
 *
 * @param[in]	instance - object of server_instance, having hostname and port number.
 * @param[in]	from_server - Flag to refer the caller.
 *
 * @return      int
 * @retval !=-1 - success, caller's server index. 
 * @retval -1	- error
 */
int 
pbs_shard_get_index(struct server_instance *instance, int from_server)
{
	int i;
        if (from_server) {
                if (svr_index == -1) {
                        if (configured_num_servers > 1) {
                                for(i = 0; i < configured_num_servers; i++) {
                                        if (instance->port == configured_servers[i]->port) {
                                                if (strcmp(instance->hostname, configured_servers[i]->hostname) == 0) {
                                                        svr_index = i;
                                                        break;
                                                }
                                        }
                                }
                        } else 
                                svr_index = 0;
                }
                return svr_index;
        } else {
                if (configured_num_servers > 1) {
                        for(i = 0; i < configured_num_servers; i++) {
                                if (instance->port == configured_servers[i]->port) {
                                        if (strcmp(instance->hostname, configured_servers[i]->hostname) == 0) 
                                                return i;
                                }
                        }
                } else 
                        return 0;

                return -1;
        }
}

/**
 * @brief
 * This API generates the next sequence ID.  
 * It accepts the current seq id and maximum seq id to compute the next one in series. 
 * This function would apply the "max_seq_id" limitations and make sure 
 * the generated ID is less than the maximum allowed sequence ID. 
 *
 * @param[in]	curr_seq_id - The current seq ID in the PBS server.
 * @param[in]	max_seq_id - The limitation in the PBS SERVER on an object ID, 
 *                           which needs to be considered while generating the new object ID. 
 *
 * @return long long
 * @retval !=-1 - success, the next obj_id to use is returned.
 * @retval -1 -  error
 */
long long 
pbs_shard_get_next_seqid(long long curr_seq_id, long long max_seq_id)
{

        if (svr_index == -1) {
                if (configured_num_servers > 1)  
                        return -1;
                else
                        svr_index = 0;
        }

        if (curr_seq_id == -1) {
                return svr_index;
        }

        curr_seq_id += max_num_of_servers;
        /* If server job limit is over, reset back to zero */
        if (curr_seq_id > max_seq_id) {
                curr_seq_id -= max_seq_id + 1;
        }
        return curr_seq_id;

}


/**
 * @brief
 * If the server gets restarted, it will be needed to know the last generated seq id. 
 * This last seq id is used by sharding logic to compute the next seq id. 
 * This API accepts the maximum generated seq ID.
 *
 * @param[in]	highest_seqid - The maximum generated seq ID. 
 *
 * @return long long
 * @retval !=-1 - success, the next obj_id to use is returned.
 * @retval -1 -  error
 */
long long 
pbs_shard_get_last_seqid(long long highest_seqid)
{

        if (svr_index == -1) {
                if (configured_num_servers > 1)  
                        return -1;
                else
                        svr_index = 0;
        }

        if (highest_seqid == -1)
                return 0;

        return (ceil(highest_seqid / max_num_of_servers)*max_num_of_servers + svr_index);

}

