#include <reclaim.h>
#include <reclaim_config.h>

#define EMPTY_MAP ( ~( 0u ) )

reclaimer_t *reclaim_init(
	void ( *terminate )( void *ptr, int is_concurrent ),
	void ( *clean_up )( void *ptr ),
	size_t inst_size,
	size_t inst_align
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

	if( inst_align % alignof( void* ) )
		inst_align = ( inst_align / alignof( void* ) + 1 ) * alignof( void* );
	r->instance.align = inst_align;

	r->writers_num = 0;
	r->threads_num = 0;
	r->ctx_list.next = NULL;
	return r;
}

static void _destroy_ctx( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	_ctx_remove_from_list( ctx );
	_ctx_rcu_reclaim_ctx( ctx );
}

inline static void _ctx_rcu_reclaim( thread_ctx_t *ctx ) {
	AO_t my_tag = ctx->reclaimer->list_reader_tag;
	AO_nop_full();
	AO_t thread_tag = 0;

	_fetch_and_inc( &( ctx->reclaimer->writers_num ) );
	_ctx_rcu_wait_for_readers( ctx->reclaimer );
	_fetch_and_dec( &( ctx->reclaimer->writers_num ) );
	_ctx_rcu_wait_for_writers( ctx->reclaimer );

	free( ctx->deleted.chunk.claims );
	free( ctx->deleted.chunk.ptrs );
	for( deleted_t *cur = ctx->deleted.chunk.next;
		cur != NULL;
		cur = cur->next
	) {
		free( cur->claims );
		free( cur->ptrs );
		free( cur );
	}
	
	free( ctx->deleted.shadow );
	
	free( ctx );
}

inline static void _ctx_rcu_wait_for_readers( reclaimer_t *r ) {
	for( thread_ctx_t *ctx = r->ctx_list.next;
		ctx != NULL;
		ctx = ctx->next
	)
		if( ctx->is_list_reader ) {
			if( ctx->list_reader_tag < my_tag )
				do {
					pthread_yield();
					AO_nop_full();
				} while(
					ctx->is_list_reader &&
					( ctx->list_reader_tag < my_tag )
				);
		}
}

inline static void _ctx_rcu_wait_for_writers( reclaimer_t *r ) {
	do {
		pthread_yield();
		AO_nop_full();
	} while ( r->writers_num > 0 );
}

inline static void _fetch_and_inc( volatile AO_t *vptr ) {
	#ifdef AO_HAVE_fetch_and_add_full
		AO_fetch_and_add_full( vptr, 1 );
	#elseif defined( AO_HAVE_fetch_and_add )
		AO_fetch_and_add( vptr, 1 );
	#else
		AO_t v;
		
		do {
			v = *vptr;
		} while(
			! AO_compare_and_swap_full( vptr, v, v + 1 )
		)
	#endif
}

inline static void _fetch_and_dec( volatile AO_t *vptr ) {
	#ifdef AO_HAVE_fetch_and_sub1_full
		AO_fetch_and_sub1_full( vptr );
	#elseif defined( AO_HAVE_fetch_and_sub1 )
		AO_fetch_and_sub1( vptr );
	#else
		AO_t v;
		
		do {
			v = *vptr;
		} while(
			! AO_compare_and_swap_full( vptr, v, v - 1 )
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
		thread_ctx_t *ctx = _ctx_create( r );
		_ctx_put_into_list( ctx );
	}

	return ctx;
}

#define DELETED_POINTERS_NUMBER ( ( 1 << ( POINTERS_NUMBER_POWER2 + 1 ) ) )

inline static thread_ctx_t *_ctx_create( reclaimer_t *r ) {
	assert( r != NULL );
	
	thread_ctx_t *ctx = malloc( sizeof( thread_ctx_t ) );
	memset( ctx, 0, sizeof( thread_ctx_t ) );
	ctx->hazard.map = EMPTY_MAP;

	memset(
		ctx->deleted.chunk.claims = malloc(
			sizeof( AO_t ) * DELETED_POINTERS_NUMBER
		),
		0,
		sizeof( AO_t ) * DELETED_POINTERS_NUMBER
	);
	
	memset(
		ctx->deleted.chunk.ptrs = malloc(
			sizeof( void* ) * DELETED_POINTERS_NUMBER
		),
		0,
		sizeof( void* ) * DELETED_POINTERS_NUMBER 
	);

	ctx->deleted.first_free_chunk =
		ctx->deleted.last_chunk =
			&( ctx->deleted.chunk );

	ctx->deleted.capacity = DELETED_POINTERS_NUMBER;
	ctx->reclaimer = r;
	
	return ctx;
}

inline static void _ctx_put_into_list( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	_fetch_and_inc( &( ctx->reclaimer->threads_num ) );
	
	pthread_mutex_lock( &( ctx->reclaimer->write_guard ) );
	ctx->header.next = ctx->reclaimer.ctx_list.next;
	ctx->prev = &( ctx->reclaimer.ctx_list );

	ctx->reclaimer.ctx_list.next = ctx;

	AO_nop_full();

	if( ctx->header.next != NULL )
		ctx->header.next->prev = ctx;

	pthread_mutex_unlock( &( ctx->reclaimer->write_guard ) );
}

inline static void _ctx_remove_from_list( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	pthread_mutex_lock( &( ctx->reclaimer->write_guard ) );

	ctx->prev->next = ctx->header.next;
	
	if( ctx->header.next != NULL )
		ctx->header.next->prev = ctx->prev;

	pthread_mutex_unlock( &( ctx->reclaimer->write_guard ) );

	_fetch_and_dec( &( ctx->reclaimer->threads_num ) );
}

inline static void _ctx_rcu_mark_as_reader( thread_ctx_t *ctx ) {
	ctx->list_reader_tag = ctx->reclaimer->list_reader_tag;	
	ctx->is_list_reader = 1;
	AO_nop_full();

	_fetch_and_inc( &( ctx->reclaimer->list_reader_tag ) );
}

inline static void _ctx_rcu_unmark_as_reader( thread_ctx_t *ctx ) {
	ctx->is_list_reader = 0;
	AO_nop_full();
}

void *reclaim_deref_link( thread_ctx_t *ctx, void * volatile *ptr_to_link ) {
	AO_t *hmap = &( ctx->hazard.map );
	assert( *hmap != 0 );
	void **hptrs = ctx->hazard.ptrs;
	
	int num = ffs( ( int ) *hmap ) - 1;

	do {
		*hmap &= ~ ( 1ul << num );
		hptrs[ num ] = *ptr_to_link;
		AO_nop_full();
	} while (
		*ptr_to_link != hptrs[ num ]
	)

	return hptrs[ num ];
}

int reclaim_release_link( thread_ctx_t *ctx, void *link ) {
	AO_t *hmap = &( ctx->hazard.map );
	void **hptrs = ctx->hazard.ptrs;

	AO_t hcyc = *hmap;
	AO_t hstop = ~ 0u;
	for( int i = 0;
		( i < 32 ) && ( hcyc != hstop );
		++i, hcyc >>= 1, hstop >>= 1
	)
		if( ( ! ( hcyc & 1u ) ) && ( hptrs[ i ] == link ) ) {
			*hmap |= ( 1ul << i );
			hptrs[ i ] = NULL;
			AO_nop_full();
			return 1;
		}

	return 0;
}

int reclaim_compare_and_swap_link( thread_ctx_t *ctx,
	void **where,
	void *old,
	void *new
) {
	if( AO_compare_and_swap_full( where, old, new ) ) {
		if( new != NULL )
			_link_inc_ref_cnt( new );
		if( old != NULL )
			_link_dec_ref_cnt( old );

		return 1;
	}

	return 0;
}

#define LINK_IS_DELETED ( 1ul << ( sizeof( AO_t ) * 8 - 1 ) )
#define LINK_IS_TRACED ( LINK_IS_DELETED >> 1 )

inline static void _link_inc_ref_cnt( void *link ) {
	AO_t *auxrec = _link_get_ancillary( link );
	AO_t aux;
	
	do {
		aux = *auxrec;
	} while (
		! AO_compare_and_swap_full( auxrec,
			aux,
			( aux + 1 ) & ( ~LINK_IS_TRACED )
		)
	)
}

inline static AO_t *_link_get_ancillary( void *link ) {
	return ( AO_t*  ) ( ( ( char* ) link ) - sizeof( AO_t ) );
}

inline static void _link_dec_ref_cnt( void *link ) {
	_fetch_and_dec( ( ( char* ) link ) - sizeof( AO_t ) );
}

void reclaim_store_link( thread_ctx_t *ctx, void **where, void *link ) {
	void *old = *where;
	*where = link;

	if( link != NULL )
		_link_inc_ref_cnt( new );
	if( old != NULL )
		_link_dec_ref_cnt( old );
}

void *reclaim_alloc( thread_ctx_t *ctx ) {
	char *res;

	posix_memalign( &res,
		ctx->reclaimer->instance.align,
		ctx->reclaimer->instance.size + ctx->reclaimer->instance.align
	);

	res += ctx->reclaimer->instance.align;
	*( ( AO_t* ) ( res - sizeof( AO_t ) ) ) = 0;
	reclaim_deref_link( ctx, &res );
	
	return res;
}

#define DELETED_LINK_IS_DONE 1ul

void reclaim_free( thread_ctx_t *ctx, void *what ) {
	assert( ctx->deleted.ptrs_number < ctx->deleted.capacity );

	reclaim_release_link( ctx, what );
	_link_mark_as_deleted( what );
	_link_put_into_deletion_list( ctx, what );
	AO_nop_full();

	if( ctx->deleted.ptrs_number >= ctx->deleted.capacity )
		_clean_local( ctx );

	if( ctx->deleted.ptrs_number >= ( ctx->deleted.capacity / 4 * 3 ) )
		_scan( ctx );

	if( ctx->deleted.ptrs_number >= ctx->deleted.capacity )
		_clean_all( ctx );

	if( ctx->deleted.ptrs_number >= ( ctx->deleted.capacity / 4 * 3 ) ) {
		_scan( ctx );

		if( ctx->deleted.ptrs_number >= ( ctx->deleted.capacity / 2 ) )
			ctx->deleted.first_free_chunk = _ctx_extend_deletion_rope( ctx );
	}
}

static inline deleted_t *_ctx_extend_deletion_rope( thread_ctx_t *ctx ) {
	deleted_t *nextd = malloc( sizeof( deleted_t ) );
	nextd->next = NULL;
			
	memset(
		nextd->ptrs = malloc(
			sizeof( void* ) * ctx->deleted.capacity
		),
		0,
		sizeof( void* ) * ctx->deleted.capacity
	);

	memset(
		nextd->claims = malloc(
			sizeof( AO_t ) * ctx->deleted.capacity
		),
		0,
		sizeof( AO_t ) * ctx->deleted.capacity
	);

	nextd->capacity = ctx->deleted.capacity;
	
	AO_nop_full();

	ctx->deleted.last_chunk->next = nextd;
	ctx->deleted.last_chunk = ctx->deleted.last_chunk->next;
	ctx->deleted.capacity *= 2;

	return nextd;
}

static inline void _link_mark_as_deleted( void *what ) {
	AO_t *auxrec = _link_get_ancillary( what );
	AO_t aux;
	
	do {
		aux = *auxrec;
	} while (
		! AO_compare_and_swap_full( auxrec,
			aux,
			aux | LINK_IS_DELETED & ( ~LINK_IS_TRACED )
		)
	)
}

static inline deleted_t *_link_put_into_deletion_list( thread_ctx_t *ctx,
	void *what
) {
	assert( ctx->deleted.first_free_chunk != NULL );
	
	ctx->deleted.first_free_chunk->ptrs[ ctx->deleted.first_free_idx ] = what;
	++ctx->deleted.ptrs_number;

	for( deleted_t *curd = ctx->deleted.first_free_chunk;
		curd != NULL;
		curd = curd->next
	) {
		for( AO_t cyc = ctx->deleted.first_free_idx;
			cyc < curd->capacity;
			++cyc
		)
			if( curd->ptrs[ cyc ] == NULL ) {
				ctx->deleted.first_free_idx = cyc;
				ctx->deleted.first_free_idx + ctx->
				if(   )
				return ( ctx->deleted.first_free_chunk = curd );
			}

		ctx->deleted.first_free_idx = 0;
	}

	return ( ctx->deleted.first_free_chunk = NULL );
}

static void _scan( thread_ctx_t *ctx ) {
	AO_t anc_block;
	AO_t *anc_ptr;
	AO_t linear_idx = 0;
	AO_t limit = DELETED_POINTERS_NUMBER;

	for( deleted_t *curd = &( ctx->deleted.chunk )
		curd != NULL;
		curd = curd->next, limit <<= 1
	)
		for( AO_t cyc = 0;
			( cyc < limit ) && ( linear_idx <= ctx->deleted.max_idx );
			++cyc, ++linear_idx
		)
			if( curd->ptrs[ cyc ] != NULL ) {
				anc_ptr = _link_get_ancillary(
					dptrs[ i ] & ( ~ DELETED_LINK_IS_DONE )
				);
				anc_block = *anc_ptr;
				if(
					( anc_block &
						( ~ ( LINK_IS_TRACED | LINK_IS_DELETED ) )
					) == 0
				)
					AO_compare_and_swap_full( anc_ptr,
						anc_block,
						anc_block | LINK_IS_TRACED
					);
			}

	if( ctx->shadow.capacity < ctx->deleted.ptrs_number ) {
		free( ctx->shadow.ptrs );
		
		ctx->shadow.ptrs = malloc(
			ctx->deleted.ptrs_number * sizeof( shadow_ptr_t )
		);

		ctx->shadow.capacity = ctx->deleted.ptrs_number;
	}

	for( thread_ctx_t *curx = ctx->reclaimer->ctx_list.next;
		curx != NULL;
		curx = curx->next
	)
}

static void _clean_all( thread_ctx_t *own_ctx ) {
	_ctx_rcu_mark_as_reader( own_ctx );

	void *link;
	AO_t ptrs_passed, ptrs_number;
	
	void ( *clean_up )( void *ptr ) = own_ctx->reclaimer->callbacks.clean_up;
	for( thread_ctx_t *ctx = own_ctx->reclaimer->ctx_list.next;
		ctx != NULL;
		ctx = ctx->next
	) {
		ptrs_passed = 0;
		ptrs_number = ctx->deleted.ptrs_number;

		for( deleted_t *curd = &( ctx->deleted.chunk )
			curd != NULL;
			curd = curd->next
		)
			for( AO_t cyc = 0;
				( cyc < curd->capacity ) && ( ptrs_passed < ptrs_number );
				++cyc
			) {
				link = curd->ptrs[ cyc ];

				if( link != NULL ) {
					++ptrs_passed;
					
					if( ! ( link & DELETED_LINK_IS_DONE ) ) {
						_fetch_and_inc( &( curd->claims[ cyc ] ) );
						if( dptrs[ i ] == link )
							clean_up( link & ( ~ DELETED_LINK_IS_DONE ) );
						_fetch_and_dec( &( curd->claims[ cyc ] ) );
					}
				}
			}
	}

	_ctx_rcu_unmark_as_reader( ctx );
}

static void _clean_local( thread_ctx_t *ctx ) {
	void ( *clean_up )( void *ptr ) = ctx->reclaimer->callbacks.clean_up;
	AO_t ptrs_passed = 0, ptrs_number = ctx->deleted.ptrs_number;

	for( deleted_t *curd = &( ctx->deleted.chunk )
		curd != NULL;
		curd = curd->next
	)
		for( AO_t cyc = 0;
			( cyc < curd->capacity ) && ( ptrs_passed < ptrs_number );
			++cyc
		)
			if( curd->ptrs[ cyc ] != NULL ) {
				++ptrs_passed;
				clean_up( curd->ptrs[ cyc ] & ( ~ DELETED_LINK_IS_DONE ) );
			}
}
