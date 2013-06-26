#include <reclaim/reclaim.h>
#include <reclaim_config.h>

#include <utils/rope.h>
#include <utils/faa.h>

#define EMPTY_MAP ( ~( 0ul ) )

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

	AO_nop_full();

	return r;
}

static void _destroy_ctx( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	_ctx_remove_from_list( ctx );
	_ctx_rcu_reclaim_ctx( ctx );
}

inline static void _ctx_rcu_reclaim( thread_ctx_t *ctx ) {
	AO_t my_tag = AO_load( &( ctx->reclaimer->list_reader_tag ) );
	AO_t thread_tag = 0;

	fetch_and_inc( &( ctx->reclaimer->writers_num ) );
	_ctx_rcu_wait_for_readers( ctx->reclaimer );
	fetch_and_dec( &( ctx->reclaimer->writers_num ) );
	_ctx_rcu_wait_for_writers( ctx->reclaimer );

	rope_destroy( ctx->deleted );

	free( ctx );
}

inline static void _ctx_rcu_wait_for_readers( reclaimer_t *r ) {
	for( thread_ctx_t *ctx = r->ctx_list.next;
		ctx != NULL;
		ctx = AO_load( &( ctx->next ) )
	)
		if( AO_load( &( ctx->is_list_reader ) ) ) {
			if( AO_load( &( ctx->list_reader_tag ) < my_tag )
				do {
					pthread_yield();
				} while(
					AO_load( &( ctx->is_list_reader ) ) &&
					( AO_load( &( ctx->list_reader_tag ) ) < my_tag )
				);
		}
}

inline static void _ctx_rcu_wait_for_writers( reclaimer_t *r ) {
	do {
		pthread_yield();
	} while ( AO_load( &( r->writers_num ) ) > 0 );
}

void reclaim_fini( reclaimer_t *r ) {
	assert( r != NULL );
	//TODO: implement something more clever and sync-aware
	// now, we can steal a chair under logic which
	// is working with it currently
	// Moreover, implement thread_ctx purging logic
	
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

	ctx->deleted = rope_create();
	ctx->reclaimer = r;

	AO_nop_full();
	
	return ctx;
}

inline static void _ctx_put_into_list( thread_ctx_t *ctx ) {
	assert( ctx != NULL );

	pthread_mutex_lock( &( ctx->reclaimer->write_guard ) );

	// we need to be sure that next pointer is in place
	// before we link context with list header
	// (i.e. allow concurrent access to it)
	ctx->header.next = ctx->reclaimer->ctx_list.next;
	AO_nop_full();

	ctx->prev = &( ctx->reclaimer->ctx_list );
	ctx->reclaimer->ctx_list.next = ctx;

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

inline static void _ctx_rcu_mark_as_reader( thread_ctx_t *ctx ) {
	ctx->list_reader_tag = AO_load( & ( ctx->reclaimer->list_reader_tag) );	
	ctx->is_list_reader = 1;
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
	
	int num = ffsl( *hmap ) - 1;

	do {
		*hmap &= ~ ( 1ul << num );
		hptrs[ num ] = *ptr_to_link;
		AO_nop_full();
	} while (
		AO_load( ptr_to_link ) != hptrs[ num ]
	)

	return hptrs[ num ];
}

int reclaim_release_link( thread_ctx_t *ctx, void *link ) {
	AO_t *hmap = &( ctx->hazard.map );
	void **hptrs = ctx->hazard.ptrs;

	AO_t hcyc = *hmap;
	AO_t hstop = ~ 0ul;
	for( int i = 0;
		hcyc != hstop;
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
		aux = AO_load( auxrec );
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
	fetch_and_dec( ( ( char* ) link ) - sizeof( AO_t ) );
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

void reclaim_free( thread_ctx_t *ctx, void *what ) {
	assert( ctx->deleted.ptrs_number < ctx->deleted.capacity );

	reclaim_release_link( ctx, what );
	_link_mark_as_deleted( what );
	rope_owner_put( ctx->deleted, what );

	if( ctx->deleted->ptrs_number == ( ctx->deleted->capacity - 1 ) )
		_scan( ctx );

	if( ctx->deleted->ptrs_number == ( ctx->deleted->capacity - 1 ) ) {
		_clean_local( ctx );
		_scan( ctx );
	}

	if( ctx->deleted->ptrs_number == ( ctx->deleted->capacity - 1 ) ) {
		_clean_all( ctx );
		_scan( ctx );
	}
}

static inline void _link_mark_as_deleted( void *what ) {
	AO_t *auxrec = _link_get_ancillary( what );
	AO_t aux;
	
	do {
		aux = AO_load( auxrec );
	} while (
		! AO_compare_and_swap_full( auxrec,
			aux,
			aux | LINK_IS_DELETED & ( ~LINK_IS_TRACED )
		)
	)
}

#define IS_HAZARDED ( 1ul )

static void _scan( thread_ctx_t *own_ctx ) {
	AO_t anc_block;
	AO_t *anc_ptr;
	
	rope_ptr_t iter;
	for( int cont = rope_iterator_create( own_ctx->deleted, &iter );
		cont;
		cont = rope_iterator_next( &iter );
	) {
		anc_ptr = _link_get_ancillary( rope_owner_iterator_deref( &iter ) );
		anc_block = AO_load( anc_ptr );

		if( ( anc_block & ( ~ ( LINK_IS_TRACED | LINK_IS_DELETED ) ) ) == 0 )
			AO_compare_and_swap( anc_ptr,
				anc_block,
				anc_block | LINK_IS_TRACED
			);
	}

	sorted_rope_t ptr_set;
	rope_owner_sort( own_ctx->deleted, &ptr_set );

	void **hptrs = NULL;
	AO_t hcyc = NULL;
	AO_t hstop = ~ 0ul;
	void *ptr = NULL;
	shadow_ptr_t *sptr;

	_ctx_rcu_mark_as_reader( own_ctx );

	for( thread_ctx_t *ctx = AO_load( &( own_ctx->reclaimer->ctx_list.next ) );
		ctx != NULL;
		ctx = AO_load( &( ctx->next ) );
	) {
		hptrs = ctx->hazard.ptrs;
		hcyc = AO_load( &( ctx->hazard.map ) );
		hstop = ~ 0ul;
		for( int i = 0;
			hcyc != hstop;
			++i, hcyc >>= 1, hstop >>= 1
		)
			if( ( ! ( hcyc & 1u ) ) &&
				( ( ptr = AO_load( &( hptrs[ i ] ) ) ) != NULL ) &&
				( ( sptr = rope_owner_find( &ptr_set, ptr ) ) != NULL )
			) 
				sptr->ptr |= IS_HAZARDED;
	}
	
	_ctx_rcu_unmark_as_reader( ctx );

	void ( *terminate )( void *ptr, int is_concurrent ) =
		own_ctx->reclaimer->callbacks.terminate;
	for( int i = 0;
		i < ptr_set.ptrs_number;
		++i
	)
		if( ! ( ptr_set.ptrs[ i ] & IS_HAZARDED ) ) {
			anc_ptr = _link_get_ancillary( ptr_set.ptrs[ i ].ptr );
			anc_block = AO_load( anc_ptr );

			if( ( anc_block & ( ~ LINK_IS_DELETED ) ) == LINK_IS_TRACED ) {
				if( rope_owner_delete_at(
					own_ctx->rope,
					ptr_set.ptrs[ i ].chunk,
					ptr_set.ptrs[ i ].idx
				) ) {
					terminate( ptr_set.ptrs[ i ].ptr, 0 );
					free( ptr_set.ptrs[ i ].ptr );
				} else
					terminate( ptr_set.ptrs[ i ].ptr, 1 );
			}
		}
}

static void _clean_all( thread_ctx_t *own_ctx ) {
	void ( *clean_up )( void *ptr ) = own_ctx->reclaimer->callbacks.clean_up;
	
	_ctx_rcu_mark_as_reader( own_ctx );

	rope_ptr_t iter;
	void *ptr = NULL;
	for( thread_ctx_t *ctx = AO_load( &( own_ctx->reclaimer->ctx_list.next ) );
		ctx != NULL;
		ctx = AO_load( &( ctx->next ) );
	)
		for( int cont = rope_iterator_create( ctx->deleted, &iter );
			cont;
			cont = rope_iterator_next( &iter )
		)
			if( ( ptr = rope_alien_iterator_deref( &iter ) ) != NULL ) {
				clean_up( ptr );
				rope_alien_iterator_release( &iter );
			}

	_ctx_rcu_unmark_as_reader( ctx );
}

static void _clean_local( thread_ctx_t *ctx ) {
	void ( *clean_up )( void *ptr ) = ctx->reclaimer->callbacks.clean_up;

	rope_ptr_t iter;
	void *ptr = NULL;
	for( int cont = rope_iterator_create( ctx->deleted, &iter );
		cont;
		cont = rope_iterator_next( &iter )
	)
		if( ( ptr = rope_owner_iterator_deref( &iter ) ) != NULL )
			clean_up( ptr );
}
