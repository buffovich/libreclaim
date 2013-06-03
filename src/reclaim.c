#include <reclaim.h>
#include <reclaim_config.h>

reclaimer_t *reclaim_init(
	void ( *terminate )( void *ptr, int is_concurrent ),
	void ( *clean_up )( void *ptr ),
	size_t inst_size,
	size_t inst_offset
) {
	assert( inst_size > 0 );
	assert( inst_offset > 0 );
	
	reclaimer_t * r = malloc( sizeof( reclaimer_t ) );
	pthread_key_create( &( r->thread_ctx ), &_destroy_ctx );
	pthread_mutex_init( &( r->write_guard ), NULL );
	r->list_reader_tag = 0;
	r->callbacks.clean_up = clean_up;
	r->callbacks.terminate = terminate;
	r->instance.size = inst_size;
	r->instance.offset = inst_offset;
	r->ctx_list.next = NULL;
	return r;
}

static void _destroy_ctx( thread_ctx_t *thread_ctx ) {
	// TODO: RCU is waiting for you
}

void reclaim_fini( reclaimer_t *r ) {
	assert( r != NULL );
	
	pthread_mutex_destroy( &( r->write_guard ) );
	pthread_key_delete( r->thread_ctx );
	free( r );
}

thread_ctx_t *reclaim_get_context( reclaimer_t *r ) {
	assert( r != NULL );
	
	thread_ctx_t *ctx = pthread_getspecific( r->thread_ctx );
	
	if( ctx == NULL ) {
		thread_ctx_t *ctx = _create_ctx( r );
		_put_into_ctx_list( ctx );
	}

	return ctx;
}

inline static thread_ctx_t *_create_ctx( reclaimer_t *r ) {
	assert( r != NULL );
	
	thread_ctx_t *ctx = malloc( sizeof( thread_ctx_t ) );
	memset( ctx, 0, sizeof( thread_ctx_t ) );
	ctx->reclaimer = r;
	return ctx;
}

inline static void _put_into_ctx_list( thread_ctx_t *ctx ) {
	assert( r != NULL );

	pthread_mutex_lock( &( ctx->reclaimer->write_guard ) );
	ctx->header.next = ctx->reclaimer.ctx_list.next;
	ctx->header.prev = &( ctx->reclaimer.ctx_list );
	
	if( ctx->header.next != NULL )
		ctx->header.next->prev = ctx;
		
	ctx->reclaimer.ctx_list.next = ctx;
	pthread_mutex_unlock( &( ctx->reclaimer->write_guard ) );
}

AO_store_full( &( ctx->list_reader_tag ),
		AO_load_full( &( ctx->reclaimer->list_reader_tag ) )
	);
	
	AO_store_full( &( ctx->is_list_reader ), 1 );

	#ifdef AO_HAVE_fetch_and_add_full
		AO_fetch_and_add_full( &( ctx->reclaimer->list_reader_tag ), 1 );
	#else
		AO_fetch_and_add1_full( &( ctx->reclaimer->list_reader_tag ) );
	#endif

void *reclaim_deref_link( thread_ctx_t *ctx, void **ptr_to_link ) {
}

void *reclaim_release_link( thread_ctx_t *ctx, void *link ) {
}

int reclaim_compare_and_swap_link( thread_ctx_t *ctx,
	void **where,
	void *old,
	void *new
) {
}

void reclaim_store_link( thread_ctx_t *ctx, void **where, void *link ) {
}

void *reclaim_alloc( thread_ctx_t *ctx ) {
}

void reclaim_free( thread_ctx_t *ctx, void *what ) {
}

static void _scan( thread_ctx_t *ctx ) {
}

static void _clean_all( thread_ctx_t *ctx ) {
}

static void _clean_local( thread_ctx_t *ctx ) {
}
