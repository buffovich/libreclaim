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
	r->ctx_list.next = NULL;
	return r;
}

static void _destroy_ctx( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	_ctx_remove_from_list( ctx );
	_ctx_rcu_reclaim_ctx( ctx );
}

inline static void _ctx_rcu_reclaim( thread_ctx_t *ctx ) {
	AO_t my_tag = AO_load_full( &( ctx->reclaimer->list_reader_tag ) );
	AO_t thread_tag = 0;

	_fetch_and_inc( &( ctx->reclaimer->writers_num ) );
	_ctx_rcu_wait_for_readers( ctx->reclaimer );
	_fetch_and_dec( &( ctx->reclaimer->writers_num ) );
	_ctx_rcu_wait_for_writers( ctx->reclaimer );

	free( ctx->deleted.claims );
	free( ctx->deleted.shadow );
	free( ctx->deleted.ptrs );
	
	free( ctx );
}

inline static void _ctx_rcu_wait_for_readers( reclaimer_t *r ) {
	for( thread_ctx_t *ctx = r->ctx_list.next;
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
}

inline static void _ctx_rcu_wait_for_writers( reclaimer_t *r ) {
	do {
		pthread_yield();
	} while ( AO_load_full( &( r->writers_num ) ) > 0 );
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

inline static void _fetch_and_dec( volatile AO_t *vptr ) {
	#ifdef AO_HAVE_fetch_and_sub1_full
		AO_fetch_and_sub1_full( vptr );
	#elseif defined( AO_HAVE_fetch_and_sub1 )
		AO_fetch_and_sub1( vptr );
	#else
		AO_t v;
		
		do {
			v = AO_load_full( vptr );
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

#define DELETED_POINTERS_NUMBER ( POINTERS_NUMBER * 2 )

inline static thread_ctx_t *_ctx_create( reclaimer_t *r ) {
	assert( r != NULL );
	
	thread_ctx_t *ctx = malloc( sizeof( thread_ctx_t ) );
	memset( ctx, 0, sizeof( thread_ctx_t ) );
	ctx->hazard.map = EMPTY_MAP;

	memset(
		ctx->deleted.claims = malloc(
			sizeof( AO_t ) * DELETED_POINTERS_NUMBER
		),
		0,
		sizeof( AO_t ) * DELETED_POINTERS_NUMBER
	);
	
	memset(
		ctx->deleted.ptrs = malloc(
			sizeof( void* ) * DELETED_POINTERS_NUMBER
		),
		0,
		sizeof( void* ) * DELETED_POINTERS_NUMBER
	);

	memset(
		ctx->deleted.shadow = malloc(
			sizeof( void* ) * DELETED_POINTERS_NUMBER
		),
		0,
		sizeof( void* ) * DELETED_POINTERS_NUMBER
	);

	ctx->deleted.capacity = DELETED_POINTERS_NUMBER;
	
	ctx->reclaimer = r;
	return ctx;
}

inline static void _ctx_put_into_list( thread_ctx_t *ctx ) {
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

inline static void _ctx_remove_from_list( thread_ctx_t *ctx ) {
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
	AO_t *hmap = &( ctx->hazard.map );
	assert( *hmap != 0 );
	void **hptrs = ctx->hazard.ptrs;
	
	int num = ffs( ( int ) *hmap ) - 1;

	do {
		*hmap &= ~ ( 1ul << num );
		AO_store_full( hptrs + num, *ptr_to_link );
	} while (
		AO_load_full( ptr_to_link ) != hptrs[ num ]
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
			AO_store_full( hptrs + i, NULL );
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
	AO_t new, old;
	
	do {
		old = *auxrec;
		new = ( old + 1 ) & ( ~LINK_IS_TRACED );
	} while (
		! AO_compare_and_swap_full( auxrec, old, new )
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
	assert( ctx->deleted.ptrs_number >= ctx->deleted.capacity );

	reclaim_release_link( ctx, what );
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

	void **hptrs = ;
	
	int num = ffs( ( int ) *( ctx->deleted.map ) ) - 1;
	ctx->deleted.ptrs[ num ] = what;
	AO_store_full( &( ctx->deleted.map ),
		ctx->deleted.map & ~ ( 1ul << num )
	);

	if( ctx->deleted.map == 0 ) {
		_clean_local( ctx );
		_scan( ctx );

		if( ctx->deleted.map == 0 ) {
			_clean_all( ctx );
			_scan( ctx );
		}
	}
}

static void _scan( thread_ctx_t *ctx ) {
	void **dptrs = ctx->deleted.ptrs;
	AO_t dcyc = ctx->deleted.map;
	AO_t dstop = ~ 0u;
	AO_t anc_block;
	AO_t *anc_ptr;

	for( int i = 0;
		( i < 32 ) && ( dcyc != dstop );
		++i, dcyc >>= 1, dstop >>= 1
	) {
		anc_ptr = _link_get_ancillary(
			dptrs[ i ] & ( ~ DELETED_LINK_IS_DONE )
		);
		anc_block = AO_load_full( anc_ptr );
		
		if( ( anc_block & ( ~ ( LINK_IS_TRACED | LINK_IS_DELETED ) ) ) == 0 )
			AO_compare_and_swap( anc_ptr,
				anc_block,
				anc_block | LINK_IS_TRACED
			);
	}
}

static void _clean_all( thread_ctx_t *own_ctx ) {
	void ( *clean_up )( void *ptr ) = own_ctx->reclaimer->callbacks.clean_up;
	void **dptrs = NULL;
	AO_t dcyc = 0, dstop = 0;
	void *link;

	_mark_as_reader( own_ctx );

	for( thread_ctx_t *ctx = own_ctx->reclaimer->ctx_list.next;
		ctx != NULL;
		ctx = ctx->next
	) {
		dptrs = ctx->deleted.ptrs;
		dcyc = ctx->deleted.map;
		dstop = ~ 0u;

		for( int i = 0;
			( i < 32 ) && ( dcyc != dstop );
			++i, dcyc >>= 1, dstop >>= 1
		) {
			link = dptrs[ i ];
			if( ( ! ( dcyc & 1u ) ) &&
				( link != NULL ) &&
				( ! ( link & DELETED_LINK_IS_DONE ) )
			) {
				_fetch_and_inc( &( ctx->deleted.claims[ i ] ) );
				if( AO_load_full( &( dptrs[ i ] ) ) == link )
					clean_up( link & ( ~ DELETED_LINK_IS_DONE ) );
				_fetch_and_dec( &( ctx->deleted.claims[ i ] ) );
			}
		}
	}
}

static void _clean_local( thread_ctx_t *ctx ) {
	void ( *clean_up )( void *ptr ) = ctx->reclaimer->callbacks.clean_up;
	void **dptrs = ctx->deleted.ptrs;

	AO_t dcyc = ctx->deleted.map;
	AO_t dstop = ~ 0u;
	for( int i = 0;
		( i < 32 ) && ( dcyc != dstop );
		++i, dcyc >>= 1, dstop >>= 1
	)
		if( ! ( dcyc & 1u ) )
			clean_up( dptrs[ i ] & ( ~ DELETED_LINK_IS_DONE ) );
}
