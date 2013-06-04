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

static void _destroy_ctx( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	_remove_from_list( ctx );
	_reclaim_ctx( ctx );
}

inline _reclaim_ctx( thread_ctx_t *ctx ) {
	AO_t my_tag = AO_load_full( &( ctx->reclaimer->list_reader_tag ) );
	AO_t thread_tag = 0;

	_fetch_and_inc( &( ctx->reclaimer->writers_num ) );

	for( thread_ctx_t *ctx = ctx->reclaimer->ctx_list.next;
		ctx != NULL;
		ctx = ctx->next
	)
		if( AO_load_full( &( ctx->is_list_reader ) ) ) {
			thread_tag = AO_load_full( &( ctx->list_reader_tag ) );
			
			if( thread_tag < my_tag )
				do {
					pthread_yield();
				} while(
					AO_load_full( &( ctx->is_list_reader ) ) &&
					( AO_load_full( &( ctx->list_reader_tag ) ) == thread_tag )
				);
		}

	_fetch_and_dec( &( ctx->reclaimer->writers_num ) );
	
	do {
		pthread_yield();
	} while ( AO_load_full( &( ctx->reclaimer->writers_num ) ) > 0 );
	
	free( ctx );
}

inline static void _fetch_and_inc( volatile AO_t *vptr ) {
	#ifdef AO_HAVE_fetch_and_add_full
		AO_fetch_and_add_full( vptr, 1 );
	#elseif defined( AO_HAVE_fetch_and_add )
		AO_fetch_and_add( vptr, 1 );
	#else
		AO_t v;
		
		do {
			v = AO_load_full( vptr );
		} while(
			! AO_compare_and_swap_full( vptr, v, v + 1 )
		)
	#endif
}

inline static void _fetch_and_dec( volatile AO_t *v ) {
	#ifdef AO_HAVE_fetch_and_sub1_full
		AO_fetch_and_sub1_full( &( ctx->reclaimer->list_reader_tag ) );
	#elseif defined( AO_HAVE_fetch_and_sub1 )
		AO_fetch_and_add( &( ctx->reclaimer->list_reader_tag ), 1 );
	#else
		AO_t writers_num;
		
		do {
			writers_num = AO_load_full( &( ctx->reclaimer->list_reader_tag ) );
		} while(
			! AO_compare_and_swap_full( &( ctx->reclaimer->list_reader_tag ),
				writers_num,
				writers_num + 1
			)
		)
	#endif
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
	assert( ctx != NULL );

	pthread_mutex_lock( &( ctx->reclaimer->write_guard ) );
	ctx->header.next = ctx->reclaimer.ctx_list.next;
	ctx->prev = &( ctx->reclaimer.ctx_list );

	ctx->reclaimer.ctx_list.next = ctx;

	AO_nop_full();

	if( ctx->header.next != NULL )
		ctx->header.next->prev = ctx;

	pthread_mutex_unlock( &( ctx->reclaimer->write_guard ) );
}

inline static void _remove_from_list( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	pthread_mutex_lock( &( ctx->reclaimer->write_guard ) );

	ctx->prev->next = ctx->header.next;
	
	if( ctx->header.next != NULL )
		ctx->header.next->prev = ctx->prev;

	pthread_mutex_unlock( &( ctx->reclaimer->write_guard ) );
}

inline static void _mark_as_reader( thread_ctx_t *ctx ) {
	AO_store_full( &( ctx->list_reader_tag ),
		AO_load_full( &( ctx->reclaimer->list_reader_tag ) )
	);
	
	AO_store_full( &( ctx->is_list_reader ), 1 );

	_fetch_and_inc( &( ctx->reclaimer->list_reader_tag ) );
}

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
