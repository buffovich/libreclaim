#include <utils/rope.h>

#include <stdlib.h>
#include <atomic_ops.h>

#include <reclaim_config.h>
#include <utils/faa.h>

#define INITIAL_POINTERS_NUMBER ( sizeof( AO_t ) * 8 )
#define INITIAL_TOTAL_SIZE ( sizeof( rope_t ) + \
		sizeof( AO_t ) + \
		sizeof( AO_t ) * INITIAL_POINTERS_NUMBER + \
		sizeof( void* ) * INITIAL_POINTERS_NUMBER \
)

rope_t *rope_create( void ) {
	rope_t *rope = malloc( INITIAL_TOTAL_SIZE );
	memset( rope, 0, INITIAL_TOTAL_SIZE );
	
	rope->capacity =
		rope->first_chunk.capacity =
			INITIAL_POINTERS_NUMBER;

	rope->last_chunk =
		rope->first_free_ptr.chunk =
			&( rope->first_chunk );
			
	rope->first_chunk.map = ( AO_t* ) ( ( ( char* ) rope ) + sizeof( rope_t ) );
	rope->first_chunk.ptrs = ( void** ) (
		( ( char* ) rope->first_chunk.map ) + sizeof( AO_t )
	);
	rope->first_chunk.claims = ( AO_t* ) (
		( ( char* ) rope->first_chunk.ptrs ) +
			( sizeof( void* ) * INITIAL_POINTERS_NUMBER )
	);

	rope->first_free_ptr.ptr = rope->first_chunk.ptrs;
	rope->first_free_ptr.claim = rope->first_chunk.claims;

	AO_nop_full();

	return rope;
}

#define BIT_NUMBER_MASK ( INITIAL_POINTERS_NUMBER - 1 )

#define WORD_POW ( ffsl( INITIAL_POINTERS_NUMBER ) - 1 )

inline static void _map_change_for( AO_t map[],
	size_t capacity,
	AO_t idx,
	int is_put
) {
	assert( map != NULL );

	int widx = idx >> WORD_POW;
	map[ widx ] |= 1 << ( idx & BIT_NUMBER_MASK );
	AO_t mask = 3 << ( idx & BIT_NUMBER_MASK & ( ~1ul ) );
	int is_full = ( map[ widx ] & mask ) == mask;
	
	idx >>= 1;
	widx >>= 1;
	mask = 3 << ( idx & BIT_NUMBER_MASK & ( ~1ul ) );
	capacity >>= WORD_POW;
	AO_t wbit = 1 << ( idx & BIT_NUMBER_MASK );
	for( int level_border = capacity, midx = level_border + widx * 2;
		capacity > 1;
			idx >>= 1,
			wbit = 1 << ( idx & BIT_NUMBER_MASK ),
			capacity >>= 1,
			level_border += capacity * 2,
			widx >>= 1,
			midx = level_border + widx * 2,
			mask = 3 << ( idx & BIT_NUMBER_MASK & ( ~1ul ) )
	) {
		map[ midx + ( ! is_put ) ] |= wbit
		if( is_full )
			map[ midx + is_put ] &= ~ wbit;

		is_full = ( map[ midx + ( ! is_put ) ] & mask ) == mask;
	}
}

inline static size_t _calc_map_len( size_t capacity ) {
	return 3 * ( capacity >> WORD_POW ) - 2;
}

inline static rope_chunk_t *_create_chunk( size_t capacity ) {
	size_t map_len = _calc_map_len( capacity );
	size_t chunk_sz = sizeof( rope_chunk_t ) +
		sizeof( AO_t ) * capacity +
		sizeof( void* ) * capacity +
		sizeof( AO_t ) * map_len;

	rope_chunk_t *rope_chunk = malloc( chunk_sz );
	memset( rope_chunk, 0, chunk_sz );
	
	rope_chunk->capacity = capacity;
	rope_chunk->map = ( AO_t* ) (
		( ( char* ) rope_chunk ) + sizeof( rope_chunk_t )
	);
	rope_chunk->ptrs = ( void** ) (
		( ( char* ) rope_chunk->map ) + sizeof( AO_t ) * map_len
	);
	
	for( AO_t *cur = rope_chunk->map + 1,
			limit = ( ( AO_t* ) rope_chunk->ptrs ) - ( capacity >> WORD_POW );
		cur < limit;
		cur += 2
	)
		*cur = SIZE_MAX;

	rope_chunk->claims = ( AO_t* ) (
		( ( char* ) rope_chunk->ptrs ) + ( sizeof( void* ) * capacity )
	);

	AO_nop_full();

	return rope_chunk;
}

enum _bit_mark {
	MARK_AS_FREE = 0,
	MARK_AS_OCCUPIED = 1
};

enum _find_what {
	FIND_FREE = 1,
	FIND_OCCUPIED = 0
};

static void _assign_to_rope_ptr( rope_ptr_t *ptr, size_t idx ) {
	init_ptr->idx = idx;
	init_ptr->ptr = &( init_ptr->chunk->ptrs[ init_ptr->idx ] );
	init_ptr->claim = &( init_ptr->chunk->claims[ init_ptr->idx ] );
}

static void _nullify_ptr( rope_ptr_t *ptr ) {
	ptr->chunk = NULL;
	ptr->ptr = NULL;
	ptr->claim = NULL;
	ptr->idx = 0;
}

inline static size_t _ffsl_after( AO_t mword, size_t idx ) {
	int ret = ffsl( mword >> idx );
	
	if( ret )
		return ( idx + ret );
	else
		return 0;

	return 0;
}

static int _ffcl_after( AO_t mword, size_t idx ) {
	AO_t mask = 1 << idx;
	for( int cyc = idx + 1;
		cyc <= ( sizeof( unsigned long ) * 8 );
		++cyc, mask <<= 1
	)
		if( mval & mask )
			return cyc;

	return 0;
}

static int _map_bob_up( AO_t map[],
	size_t capacity,
	int *level,
	size_t *idx,
	int which
) {
	int level_limit = ffsl( capacity ) - WORD_POW;

	if( *level < 1 ) {
		AO_t mword = AO_load( &( map[ idx >> WORD_POW ] ) );
		size_t bitn = ( which == FIND_OCCUPIED ) ?
			_ffsl_after( mword, idx & BIT_NUMBER_MASK ) :
			_ffcl_after( mword, idx & BIT_NUMBER_MASK );
			
		if( bitn != 0 ) {
			*idx = *idx & ( ~ BIT_NUMBER_MASK ) | ( bitn - 1 );
			return 1;
		}

		if( ( level + 1 ) >= level_limit )
			return 0;

		
		size_t new_idx = *idx & ( ~ BIT_NUMBER_MASK ) +
			INITIAL_POINTERS_NUMBER;

		if( new_idx >= capacity  )
			return 0;

		++( *level );
		*idx = new_idx >> 1;
	}

	AO_t *level_border = map +
		_calc_map_len( capacity ) -
		( 1 << ( level_limit - level ) - 1 ) * 2;

	capacity >>= level;

	for( size_t bitn = 0, new_idx = 0;
		( *level ) < level_limit;
		++( *level )
	) {
		bitn = _ffsl_after(
			AO_load( &( level_border[ idx >> WORD_POW + which ] ) ),
			idx & BIT_NUMBER_MASK
		);
			
		if( bitn != 0 ) {
			*idx = *idx & ( ~ BIT_NUMBER_MASK ) | ( bitn - 1 );
			return 1;
		}

		
		new_idx = *idx & ( ~ BIT_NUMBER_MASK ) +
			INITIAL_POINTERS_NUMBER;

		if( new_idx >= capacity  )
			return 0;

		*idx = new_idx >> 1;
		level_border += capacity * 2;
		capacity >>= 1;
	}

	return 0;
}

static int _map_sink( AO_t map[],
	size_t capacity,
	int *level,
	size_t *idx,
	int which
) {
	if( *level < 1 )
		return 1;

	int level_limit = ffsl( capacity ) - WORD_POW;

	--( *level );

	capacity >>= level;
	*idx <<= 1;

	if( *level > 0 ) {
		AO_t *level_border = map +
			_calc_map_len( capacity ) -
			( 1 << ( level_limit - level ) - 1 ) * 2;

		for( size_t bitn = 0;
			( *level ) > 0 ;
			--( *level )
		) {
			bitn = _ffsl_after(
				AO_load( &( level_border[ idx >> WORD_POW + which ] ) ),
				idx & BIT_NUMBER_MASK
			);

			if( bitn == 0 )
				return 0;

			*idx = *idx & ( ~ BIT_NUMBER_MASK ) | ( bitn - 1 );

			*idx <<= 1;
			capacity <<= 1;
			level_border -= capacity * 2;
		}
	}

	AO_t mword = AO_load( &( map[ idx >> WORD_POW ] ) );
	size_t bitn = ( which == FIND_OCCUPIED ) ?
		_ffsl_after( mword, idx & BIT_NUMBER_MASK ) :
		_ffcl_after( mword, idx & BIT_NUMBER_MASK );

	if( bitn != 0 ) {
		*idx = *idx & ( ~ BIT_NUMBER_MASK ) | ( bitn - 1 );
		return 1;
	}

	return 0;
}

static size_t _discover_map( AO_t map[],
	size_t capacity,
	size_t start_idx,
	int which
) {
	int level = 0;

	do {
		if( ! _map_bob_up( map, capacity, &level, &start_idx, which ) )
			return SIZE_MAX;
	} while (
		! _map_sink( map, capacity, &level, &start_idx, which )
	)

	return idx;
}

static int _find_next( rope_ptr_t *init_ptr, int which ) {
	assert( init_ptr != NULL );
	
	if( init_ptr->chunk == NULL )
		return 0;

	int is_changed = 0;
	if( init_ptr->chunk->capacity == INITIAL_POINTERS_NUMBER ) {
		size_t new_idx = _discover_map( init_ptr->chunk->map,
			INITIAL_POINTERS_NUMBER,
			init_ptr->idx,
			which
		)

		if( new_idx != SIZE_MAX ) {
			_assign_to_rope_ptr( init_ptr, new_idx );
			return 1;
		}

		init_ptr->chunk = AO_load( &( init_ptr->chunk->next ) );
		is_changed = 1;
	}

	size_t new_idx = 0;
	for( rope_chunk_t *curc = init_ptr->chunk;
		curc != NULL;
		curc = AO_load( curc->next ), is_changed = 1
	)
		if( AO_load(
				&( curc->map[ _calc_map_len( curc->capacity ) - 2 + which ] )
			) != 0
		) {
			new_idx = _discover_map( curc->map, curc->capacity, 0, which );

			if( new_idx != SIZE_MAX ) {
				_assign_to_rope_ptr( init_ptr, new_idx );
				return 1;
			}
		}

	_nullify_ptr( init_ptr );
	return 0;
}

void rope_owner_put( rope_t *where, void *ptr ) {
	assert( where != NULL );
	assert( where->first_free_chunk != NULL );
	assert( ptr != NULL );
	
	*( where->first_free_ptr.ptr ) = what;
	++where->ptrs_number;
	_map_change_for(
		where->first_free_ptr.chunk->map,
		where->first_free_ptr.chunk->capacity,
		where->first_free_ptr.idx,
		MARK_AS_OCCUPIED
	);

	if( where->ptrs_number >= where->capacity ) {
		where->last_chunk->next =
			where->first_free_ptr.chunk =
				_create_chunk( where->capacity );
		where->first_free_ptr.idx = 0;
		where->first_free_ptr.claim = where->first_free_ptr.chunk->claims;
		where->first_free_ptr.ptr = where->first_free_ptr.chunk->ptrs;
		where->last_chunk = where->last_chunk->next;
		where->capacity *= 2;
	} else
		assert( _find_next( &( where->first_free_ptr ), FIND_FREE ) );
}

int rope_iterator_create( const rope_t *rope, rope_ptr_t *iter ) {
	iter->chunk = rope->first_chunk;
	iter->idx = 0;
	iter->ptr = iter->chunk->ptrs;
	iter->claim = iter->chunk->claims;
	return _find_next( iter, FIND_OCCUPIED );
}

void rope_iterator_create_with( rope_ptr_t *iter,
	rope_chunk_t *chunk,
	size_t idx
) {
	iter->chunk = chunk;
	iter->idx = idx;
	iter->ptr = &( chunk->ptrs[ idx ] );
	iter->claim = &( chunk->claims[ idx ] );
}

int rope_iterator_next( rope_ptr_t *iter ) {
	if( ( iter->idx + 1 ) < iter->chunk->capacity )
		_assign_to_rope_ptr( iter, iter->idx + 1 );
	else {
		iter->chunk = iter->chunk->next;
		if( iter->chunk != NULL )
			_assign_to_rope_ptr( iter, 0 );
		else {
			_nullify_ptr( iter );
			return 0;
		}
	}
	
	return _find_next( iter, FIND_OCCUPIED );
}

#define PTR_DONE ( 1ul )

void *rope_owner_iterator_deref( rope_ptr_t *iter ) {
	if( iter->ptr == NULL )
		return NULL;

	return *( iter->ptr ) & ( ~ PTR_DONE );
}

int rope_owner_delete( rope_t *rope, rope_ptr_t *iter ) {
	void *ptr = *( iter->ptr );
	AO_store( iter->ptr, NULL );

	AO_nop_full();
	
	if( AO_load( iter->claim ) == 0 ) {
		_map_change_for(
			iter->chunk->map,
			iter->chunk->capacity,
			iter->idx,
			MARK_AS_FREE
		);

		--rope->ptrs_number;

		if ( iter->chunk == rope->first_free_ptr.chunk ) {
			if( iter->idx < rope->first_free_ptr.idx )
				rope->first_free_ptr = *iter;
		} else {
			if( iter->chunk->capacity < rope->first_free_ptr.chunk->capacity )
				rope->first_free_ptr = *iter;
		}

		return 1;
	} else {
		AO_store( iter->ptr, ptr | PTR_DONE );
		return 0;
	}
}

int rope_owner_delete_at( rope_t *rope, rope_chunk_t *chunk, size_t idx ) {
	rope_ptr_t ptr = {
		.idx = idx,
		.chunk = chunk,
		.ptr = &( chunk->ptrs[ idx ] ),
		.claim = &( chunk->claims[ idx ] )
	};

	return rope_owner_delete( rope, &iter );
}

void *rope_alien_iterator_deref( rope_ptr_t *iter ) {
	void *ptr = AO_load( iter->ptr );

	if( ( ptr != NULL ) && ( ! ( ptr & PTR_DONE ) ) ) {
		fetch_and_inc( iter->claim );
		
		if( AO_load( iter->ptr ) == ptr )
			return ptr;

		fetch_and_dec( iter->claim );
		return NULL;
	} else
		return NULL;

	return NULL;
}

void rope_alien_iterator_release( rope_ptr_t *iter ) {
	fetch_and_dec( iter->claim );
}

static int _compare_ptrs( const void *a, const void *b ) {
	void *ptr_a = ( ( shadow_ptr_t* ) a )->ptr,
		*ptr_b = ( ( shadow_ptr_t* ) b )->ptr;

	if( ptr_a < ptr_b )
		return -1;

	if( ptr_a > ptr_b )
		return 1;

	return 0;
}

void rope_owner_sort( rope_t *what, sorted_rope_t *to ) {
	shadow_ptr_t *ptrs = what->shadow.ptrs;
	if( what->shadow.capacity < what->ptrs_number ) {
		free( ptrs );
		ptrs =
			what->shadow.ptrs =
				malloc(
					sizeof( shadow_ptr_t ) * what->ptrs_number
				);
	}

	rope_ptr_t iter;
	rope_iterator_create( what, &iter )
	for( int cyc = 0;
		cyc < what->ptrs_number;
		rope_iterator_next( &iter ), ++cyc
	) {
		ptrs[ cyc ].ptr = rope_owner_iterator_deref( &iter );
		ptrs[ cyc ].idx = iter.idx;
		ptrs[ cyc ].chunk = iter.chunk;
	}

	qsort( ptrs, what->ptrs_number, sizeof( shadow_ptr_t ), _compare_ptrs );

	to->ptrs_number = what->ptrs_number;
	to->ptrs = ptrs;
}

shadow_ptr_t *rope_owner_find( sorted_rope_t *where, void *what ) {
	shadow_ptr_t ptr = { .ptr = what };

	return bsearch( ,
		where->ptrs,
		where->ptrs_number,
		sizeof( shadow_ptr_t ),
		_compare_ptrs
	);
}

void rope_destroy( rope_t *what ) {
	assert( what != NULL );
	//TODO: implement something more clever and sync-aware
	// now, we can steal a chair under logic which
	// is working with it currently
	
	for( rope_chunk_t *curc = what->first_chunk.next;
		curc != NULL;
		curc = curc->next
	)
		free( curc );

	free( what );
}
