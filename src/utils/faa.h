#ifndef LIBFAA
#define LIBFAA

#include <atomic_ops.h>

inline static void fetch_and_inc( volatile AO_t *vptr ) {
	#ifdef AO_HAVE_fetch_and_add_full
		AO_fetch_and_add_full( vptr, 1 );
	#elseif defined( AO_HAVE_fetch_and_add )
		AO_fetch_and_add( vptr, 1 );
		AO_nop_full();
	#else
		AO_t v;
		
		do {
			v = *vptr;
		} while(
			! AO_compare_and_swap_full( vptr, v, v + 1 )
		)
	#endif
}

inline static void fetch_and_dec( volatile AO_t *vptr ) {
	#ifdef AO_HAVE_fetch_and_sub1_full
		AO_fetch_and_sub1_full( vptr );
	#elseif defined( AO_HAVE_fetch_and_sub1 )
		AO_fetch_and_sub1( vptr );
		AO_nop_full();
	#else
		AO_t v;
		
		do {
			v = *vptr;
		} while(
			! AO_compare_and_swap_full( vptr, v, v - 1 )
		)
	#endif
}

#endif
