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
 *
 * @brief
 *		all the functions related to multi-server.
 *
 */

#include	"pbs_nodes.h"
#include	"pbs_error.h"


extern char server_host[];
extern uint	pbs_server_port_dis;
extern mominfo_t* create_svrmom_struct(char *phost, int port);



mominfo_t *
get_peersvr(struct sockaddr_in *addr)
{
	mominfo_t *pmom;

	if ((pmom = tfind2(ntohl(addr->sin_addr.s_addr), ntohs(addr->sin_port),
			   &ipaddrs)) != NULL) {
		if (pmom->mi_rmport == pmom->mi_port)
			return pmom;
	}

	return NULL;
}

/**
 * @brief
 * 	Return multiserver mode
 *
 * @return int
 */
int
msvr_mode(void)
{
	return (get_num_servers() > 1);
}

struct peersvr_list {
	struct peersvr_list *next;
	mominfo_t  *pmom;
};
typedef struct peersvr_list peersvr_list_t;

static struct peersvr_list *peersvrl;

mominfo_t *
create_svr_entry(char *hostname, unsigned int port)
{
	mominfo_t *pmom = NULL;

	pmom = (mominfo_t *) malloc(sizeof(mominfo_t));
	if (!pmom)
		goto err;

	strncpy(pmom->mi_host, hostname, PBS_MAXHOSTNAME);
	pmom->mi_host[PBS_MAXHOSTNAME] = '\0';
	pmom->mi_port = port;
	pmom->mi_rmport = port;
	pmom->mi_modtime = (time_t) 0;
	pmom->mi_data = NULL;
	pmom->mi_action = NULL;
	pmom->mi_num_action = 0;

	if (peersvrl) {
		peersvr_list_t *tmp = calloc(1, sizeof(peersvr_list_t));
		if (!tmp)
			goto err;
		tmp->pmom = pmom;
		peersvr_list_t *itr;
		for (itr = peersvrl; itr->next; itr = itr->next)
			;
		itr->next = tmp;
	} else {
		peersvrl = calloc(1, sizeof(peersvr_list_t));
		if (!peersvrl)
			goto err;
		peersvrl->pmom = pmom;
	}

	return pmom;

err:
	log_errf(PBSE_SYSTEM, __func__, "malloc/calloc failed");
	return pmom;
}

int
init_msi()
{
	int i;

	peersvrl = NULL;

	for (i = 0; i < get_num_servers(); i++) {

		if (!strcmp(pbs_conf.psi[i].name, pbs_conf.pbs_server_name) &&
		    (pbs_conf.psi[i].port == pbs_server_port_dis))
			continue;

		if (create_svrmom_struct(pbs_conf.psi[i].name, pbs_conf.psi[i].port) == NULL) {
			log_errf(-1, __func__, "Failed initialization for %s", pbs_conf.psi[i].name);
			return -1;
		}
	}

	return 0;
}
