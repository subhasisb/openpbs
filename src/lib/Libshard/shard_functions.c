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


int shard_get_server_instance(int pbs_max_servers, char *shard_hint, int *pbs_server_instances, int *inactive_servers);


int
shard_get_server_instance(int pbs_max_servers, char *shard_hint, int *pbs_server_instances, int *inactive_servers)
{
        int srv_ind = 0;
        int nshardid = 0;
        int count = 0;
        int loop_i = 0;
        static int seeded = 0;
        struct timeval tv;
        unsigned long time_in_micros;
        int *tmp = NULL;

        if (!pbs_max_servers || !pbs_server_instances || pbs_server_instances[0] == -1)
                return -1;

        tmp = pbs_server_instances;
        for(; *tmp != -1; tmp++)
                count++;

        if (shard_hint) {
                nshardid = strtoull(shard_hint, NULL, 10);
                srv_ind = nshardid % pbs_max_servers;
        } else {
                if (!seeded) {
                        gettimeofday(&tv,NULL);
                        time_in_micros = 1000000 * tv.tv_sec + tv.tv_usec;
                        srand(time_in_micros); /* seed the random generator */
                        seeded = 1;
                }
                srv_ind = pbs_server_instances[rand() % count];
        }

 check_again:       
        if (inactive_servers[loop_i] != -1) {
                for(; inactive_servers[loop_i] != -1; loop_i++) {
                        if (srv_ind == inactive_servers[loop_i]) {
                                srv_ind = pbs_server_instances[(srv_ind + 1) % count]; 
                                goto check_again;
                        }
                }
                if (loop_i == count)
                        return -1;
        }
        return srv_ind;
}

