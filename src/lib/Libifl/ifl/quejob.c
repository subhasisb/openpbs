#include <stdio.h>
#include <stdlib.h>
#include "pbs_ifl_builder.h"
#include "pbs_ifl_reader.h"

#define PBS_BATCH_QueueJob 99

// Convenient namespace macro to manage long namespace prefix.
#undef ns
#define ns(x) FLATBUFFERS_WRAP_NAMESPACE(PBS_ifl, x) // Specified in the schema.

enum batch_op {	SET, UNSET, INCR, DECR,
	EQ, NE, GE, GT, LE, LT, DFLT
};

struct attrl {
	struct attrl *next;
	char	     *name;
	char	     *resource;
	char	     *value;
	enum batch_op 	 op;	/* not used */
};

struct attropl {
	struct attropl	*next;
	char		*name;
	char		*resource;
	char		*value;
	enum batch_op 	 op;
};

flatbuffers_ref_t
encode_wire_ReqHdr(void *buf, int reqt, char *user)
{
    flatcc_builder_t *B = (flatcc_builder_t *) buf;
    flatbuffers_string_ref_t usr;
    uint16_t reqn = 99;
	ns(Header_ref_t) hdr;

    usr = flatbuffers_string_create_str(B, user);
	ns(Header_start(B));
	ns(Header_batchId_add(B, reqn));
	ns(Header_protType_add(B, ns(ProtType_Batch)));
	ns(Header_user_add(B, usr));
	hdr = ns(Header_end(B));

	return hdr;
}

flatbuffers_ref_t
encode_wire_ReqExtend(void *buf, char *extend)
{
    flatcc_builder_t *B = (flatcc_builder_t *) buf;
    flatbuffers_string_ref_t extstr = 0;
	ns(Extend_ref_t) ext = 0;

	if (extend && extend[0] != '\0') {
		extstr = flatbuffers_string_create_str(B, extend);
		ext = ns(Extend_create(B, extstr));
	}
	return ext;
}

flatbuffers_ref_t
encode_wire_attropl(void *buf, struct attropl *pattropl)
{
	struct attropl *ps;
	flatcc_builder_t *B = (flatcc_builder_t *) buf;
	flatbuffers_string_ref_t name, resc, value;
    ns(batch_op_enum_t) bop;

	ns(Attribute_vec_start(B));

	for (ps = pattropl; ps; ps = ps->next) {
		ns(Attribute_start(B));
		name = flatbuffers_string_create_str(B, ps->name);
		ns(Attribute_name_add(B, name));
		if (ps->resource) {
			resc = flatbuffers_string_create_str(B, ps->resource);
			ns(Attribute_resc_add(B, resc));
		}
		value = flatbuffers_string_create_str(B, ps->value);
		ns(Attribute_value_add(B, value));

        bop = (int) ps->op;
		ns(Attribute_op_add(B, bop));
		ns(Attribute_ref_t) attr = ns(Attribute_end(B));
	
		ns(Attribute_vec_push(B, attr));
	}
	
	return ((ns(Attribute_vec_end(B))));
}

flatbuffers_ref_t
encode_wire_QueueJob(void *buf, char *jobid, char *destin, struct attropl *aoplp)
{
	flatcc_builder_t *B = (flatcc_builder_t *) buf;
	ns(Attribute_vec_ref_t) attrs = 0;
	ns(Qjob_ref_t) qjob;

	if (jobid == NULL)
		jobid = "";
	if (destin == NULL)
		destin = "";

	flatbuffers_string_ref_t jid = flatbuffers_string_create_str(B, jobid);
	flatbuffers_string_ref_t dst = flatbuffers_string_create_str(B, destin);

	attrs =  encode_wire_attropl(buf, aoplp);

	ns(Qjob_start(B));
	ns(Qjob_jobId_add(B, jid));
	ns(Qjob_destin_add(B, dst));
	ns(Qjob_attrs_add(B, attrs));

	qjob = ns(Qjob_end(B));

	return qjob;
}

flatcc_builder_t builder, *B;

void * 
get_encode_buffer(int connect)
{
    B = &builder;
    // Initialize the builder object.
    flatcc_builder_init(B);

    return B;
}

void decode_quejob(void *buf)
{
	int i;
    ns(Req_table_t) req = ns(Req_as_root(buf));
    ns(Header_table_t) hdr = ns(Req_hdr(req));
	char *resc;

    uint16_t proto = ns(Header_protType(hdr));
    uint16_t batchId = ns(Header_batchId(hdr));
    flatbuffers_string_t user = ns(Header_user(hdr));

    printf("Proto: %d, batchId: %d, user: %s\n", proto, batchId, (user?user:"null"));

	if (ns(Req_body_type(req)) == ns(ReqBody_Qjob)) {
        // Cast to appropriate type:
        // C does not require the cast to Weapon_table_t, but C++ does.
        ns(Qjob_table_t) qjob = (ns(Qjob_table_t)) ns(Req_body(req));
		printf("Jobid: %s\n", ns(Qjob_jobId(qjob)));
		printf("Destn: %s\n", ns(Qjob_destin(qjob)));
		ns(Attribute_vec_t) attrs = ns(Qjob_attrs(qjob));
		size_t attrs_len = ns(Attribute_vec_len(attrs));
		printf("Total number of attributes = %d\n", attrs_len);
		for(i=0 ; i < attrs_len; i++ ) {
			ns(Attribute_table_t) attr = ns(Attribute_vec_at(attrs, i));
			
			if (ns(Attribute_resc_is_present(attr))) {
				resc =  (char *) ns(Attribute_resc(attr));
			} else {
				resc = "Unset";
			}
			printf("\t Name: %s, value: %s, resc: %s, op: %d\n", ns(Attribute_name(attr)), resc,  ns(Attribute_value(attr)), ns(Attribute_op(attr)));
		}
	}

	if (ns(Req_extend_is_present(req))) {
		ns(Extend_table_t) ext = ns(Req_extend(req));
		printf("extend present: %s\n", ns(Extend_extend(ext)));
	}
}

char *
PBSD_queuejob(int connect, char *jobid, char *destin, struct attropl *attrib, char *extend, int rpp, char **msgid, int *commit_done)
{
        struct batch_reply *reply;
        char  *return_jobid = NULL;
        int    rc;
        int    sock;
        void   *buf;
        void   *obuf;
        ns(Header_ref_t) hdr_ref;
        ns(Qjob_ref_t) quejob_ref;
        ns(ReqBody_union_ref_t) reqbody_ref;
        ns(Extend_ref_t) ext_ref;
        flatcc_builder_t *B; 
        size_t size;

        /* get buffer here */
        buf = get_encode_buffer(connect);
        B = (flatcc_builder_t *) buf;

        /* first, set up the body of the Queue Job request */
		ns(Req_start_as_root(B));
        hdr_ref = encode_wire_ReqHdr(buf, PBS_BATCH_QueueJob, "subhasis");
        quejob_ref = encode_wire_QueueJob(buf, jobid, destin, attrib);
        reqbody_ref = ns(ReqBody_as_Qjob(quejob_ref));
        ext_ref = encode_wire_ReqExtend(buf, extend);

        if (hdr_ref == 0 || quejob_ref == 0)
            return NULL;

		ns(Req_hdr_add(B, hdr_ref));
		ns(Req_body_add(B, reqbody_ref));

		if (ext_ref)
			ns(Req_extend_add(B, ext_ref));

		ns(Req_end_as_root(B));

        //ns(Req_create(B, hdr_ref,  reqbody_ref, ext_ref));

        obuf = flatcc_builder_finalize_aligned_buffer(B, &size);

        printf("obuf = %p, size = %d\n", obuf, size);
        decode_quejob(obuf);

        free(obuf);
       
        flatcc_builder_reset(B);

        // read resonse from server

        return "1.server";
}

struct attrl *
new_attrl()
{
	struct attrl *at;

	if ((at = malloc(sizeof(struct attrl))) == NULL)
		return NULL;

	at->next = NULL;
	at->name = NULL;
	at->resource = NULL;
	at->value = NULL;
	at->op = DECR;

	return at;
}

static struct attrl* new_attr;

void
set_attr(struct attrl **attrib, char *attrib_name, char *attrib_value)
{
	struct attrl *attr, *ap;

	attr = new_attrl();
	if (attr == NULL) {
		fprintf(stderr, "Out of memory\n");
		exit(2);
	}
	if (attrib_name == NULL)
		attr->name = NULL;
	else {
		attr->name = (char *) malloc(strlen(attrib_name)+1);
		if (attr->name == NULL) {
			fprintf(stderr, "Out of memory\n");
			exit(2);
		}
		strcpy(attr->name, attrib_name);
	}
	if (attrib_value == NULL)
		attr->value = NULL;
	else {
		attr->value = (char *) malloc(strlen(attrib_value)+1);
		if (attr->name == NULL) {
			fprintf(stderr, "Out of memory\n");
			exit(2);
		}
		strcpy(attr->value, attrib_value);
	}
	new_attr = attr; /* set global var new_attrl in case set_attr_resc want to add resource to it */
	if (*attrib == NULL) {
		*attrib = attr;
	} else {
		ap = *attrib;
		while (ap->next != NULL) ap = ap->next;
		ap->next = attr;
	}

	return;
}

int main()
{
    char *msgid;
    int commit_done;
    struct attrl *attrib = NULL;

    set_attr(&attrib, "ATTR_X11_cookie", "x11authstr");
	set_attr(&attrib, "Gogo", "bobo");
	set_attr(&attrib, "roro", "roro");

    PBSD_queuejob(0, "1.server", "blrmac64", (struct attropl *) attrib, "EX", 0, &msgid, &commit_done);
}