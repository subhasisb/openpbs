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
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <math.h>
#include "libshard.h"

int pbs_shard_init(int max_allowed_servers, struct server_instance **server_instances, int num_instances);

int pbs_shard_get_server_byindex(char *obj_id, enum obj_type_t obj_type, int *inactive_servers);

long long pbs_shard_get_next_seqid(long long curr_seq_id_seq_id, long long max_seq_id);

long long pbs_shard_get_last_seqid(long long njobid);

int get_my_index(struct server_instance myinstance);

int get_svr_index(struct server_instance remote_instance);

int max_allowed_servers_lib = 0;
struct server_instance **server_instances_lib = NULL;
int num_instances_lib = 0;
static int my_index = -1;

int 
pbs_shard_init(int max_allowed_servers, struct server_instance **server_instances, int num_instances) {
        max_allowed_servers_lib = max_allowed_servers;
        server_instances_lib = server_instances;
        num_instances_lib = num_instances;
        return 0;
}

int
pbs_shard_get_server_byindex(char *obj_id, enum obj_type_t obj_type, int *inactive_servers)
{
        int srv_ind = 0;
        int nshardid = 0;
        int loop_i = 0;
        static int seeded = 0;
        struct timeval tv;
        unsigned long time_in_micros;
        int counter = 0;

        if (!max_allowed_servers_lib || !server_instances_lib)
                return -1;

        if (obj_id) {
                nshardid = strtoull(obj_id, NULL, 10);
                srv_ind = nshardid % max_allowed_servers_lib;
        } else {
                if (!seeded) {
                        gettimeofday(&tv,NULL);
                        time_in_micros = 1000000 * tv.tv_sec + tv.tv_usec;
                        srand(time_in_micros); /* seed the random generator */
                        seeded = 1;
                }
                srv_ind = rand() % num_instances_lib;
        }

 check_again:
        if (counter >= num_instances_lib)
                return -1;
        loop_i = 0;
        if (inactive_servers[0] != -1) {     
                for(; loop_i < num_instances_lib; loop_i++) {
                        if (srv_ind == inactive_servers[loop_i]) {
                                srv_ind = (srv_ind + 1) % num_instances_lib;
                                counter++;                                 
                                goto check_again;
                        }
                }
        }        
        return srv_ind;
}

int 
get_my_index(struct server_instance myinstance)
{
	int i;

	if (my_index == -1) {
		if (num_instances_lib > 1) {
			/* find my index */
			for(i = 0; i < num_instances_lib; i++) {
				if (myinstance.port == server_instances_lib[i]->port)
					my_index = i;
			}
		} else 
			my_index = 0;
	}

	return my_index;
}

long long 
pbs_shard_get_next_seqid(long long curr_seq_id, long long max_seq_id)
{

        if (my_index == -1) {
                if (num_instances_lib > 1)  
                        return -1;
                else
                        my_index = 0;
        }

        if (curr_seq_id == -1) {
                return my_index;
        }

        curr_seq_id += max_allowed_servers_lib;
        /* If server job limit is over, reset back to zero */
        if (curr_seq_id > max_seq_id) {
                curr_seq_id -= max_seq_id + 1;
        }
        return curr_seq_id;

}


long long 
pbs_shard_get_last_seqid(long long njobid)
{

        if (my_index == -1) {
                if (num_instances_lib > 1)  
                        return -1;
                else
                        my_index = 0;
        }

        if (njobid == -1)
                return 0;

        return (ceil(njobid / max_allowed_servers_lib)*max_allowed_servers_lib + my_index);

}

int 
get_svr_index(struct server_instance remote_instance)
{
	int i;

	if (num_instances_lib > 1) {
		/* find my index */
		for(i = 0; i < num_instances_lib; i++) {
			if (remote_instance.port == server_instances_lib[i]->port)
				return i;
		}
	} else 
		return 0;

	return -1;
}
