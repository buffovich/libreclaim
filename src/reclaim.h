#ifndef LIBRECLAIM
#define LIBRECLAIM

#define POINTERS_NUMBER ( sizeof( AO_t ) * 8 )

typedef struct _thread_list_t {
	thread_ctx_t *next;
} thread_list_t;

typedef struct {
	pthread_key_t thread_ctx;
	pthread_mutex_t write_guard;
	AO_t readers;
	thread_list_t hplist;
	size_t num_hazards;
} reclaimer_t;

typedef struct {
	thread_list_t header;
	thread_list_t *prev;
	reclaimer_t *list_header;
	
	void *hazard_ptrs[ POINTERS_NUMBER ];
} thread_ctx_t;

#endif
