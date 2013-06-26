#ifndef LIBROPE
#define LIBROPE

#include <atomic_ops.h>

typedef struct _rope_chunk {
	struct _rope_chunk *next;
	size_t capacity;
	// auxiliary list allows another threads walking through the thread
	// context list mark particular node in deleted list with reference
	// counter
	AO_t *claims;
	// list of objects deleted by the thread; first bit of pointer value
	// is flag signalized if object has been terminated already
	void **ptrs;
	AO_t *map;
} rope_chunk_t;

typedef struct {
	void **ptr;
	AO_t *claim;
	size_t idx;
	rope_chunk_t *chunk;
} rope_ptr_t;

typedef struct {
	void *ptr;
	size_t idx;
	rope_chunk_t *chunk;
} shadow_ptr_t;

typedef struct {
	size_t ptrs_number;
	shadow_ptr_t *ptrs;
} sorted_rope_t;

typedef struct {
	size_t capacity;
	size_t ptrs_number;
	rope_ptr_t first_free_ptr;
	rope_chunk_t *last_chunk;

	struct {
		size_t capacity;
		shadow_ptr_t *ptrs;
	} shadow;
	
	rope_chunk_t first_chunk;
} rope_t;

extern rope_t *rope_create( void );
extern void rope_owner_put( rope_t *where, void *ptr );
extern int rope_iterator_create( const rope_t *rope, rope_ptr_t *iter );
extern int rope_iterator_next( rope_ptr_t *iter );
extern void *rope_owner_iterator_deref( rope_ptr_t *iter );
extern int rope_owner_delete( rope_t *rope, rope_ptr_t *iter );
extern int rope_owner_delete_at( rope_t *rope,
	rope_chunk_t *chunk,
	size_t idx
);
extern void *rope_alien_iterator_deref( rope_ptr_t *iter );
extern void rope_alien_iterator_release( rope_ptr_t *iter );
extern void rope_owner_sort( rope_t *what, sorted_rope_t *to );
extern shadow_ptr_t *rope_owner_find( sorted_rope_t *where, void *what );
extern void rope_destroy( rope_t *what );

#endif
