#ifndef LIBRECLAIM
#define LIBRECLAIM

// TODO: Time bomb: fixed number of elements in deletion list
// Just to push development further, I'm leaving this to-do

#define POINTERS_NUMBER ( sizeof( AO_t ) * 8 )

typedef struct _thread_list_t {
	thread_ctx_t *next;
} thread_list_t;

typedef struct {
	pthread_key_t thread_ctx;
	pthread_mutex_t write_guard;
	// thread context list (ctx_list) is RCU; following value is used for
	// readers "tagging"
	AO_t list_reader_tag;
	struct {
		void ( *terminate )( void *ptr, int is_concurrent );
		void ( *clean_up )( void *ptr );
	} callbacks;
	thread_list_t ctx_list;
} reclaimer_t;

typedef struct {
	thread_list_t header;
	thread_list_t *prev;
	reclaimer_t *list_header;
	// reader thread tag and reading flag; whenever thread walking through
	// the list it tags itself with current tag, increments it and set
	// the reading flag
	AO_t is_list_reader;
	AO_t list_reader_tag;
	struct {
		// map of occupied and free slots in deleted_ptrs array
		AO_t map;
		// list of objects deleted by the thread; first bit of pointer value is flag
		// signalized if object has been terminated already
		void *ptrs[ POINTERS_NUMBER ];
		// auxiliary list allows another threads walking through the thread context
		// list mark particular node in deleted list with reference counter
		AO_t claims[ POINTERS_NUMBER ];
	} deleted;

	struct {
		AO_t map;
		void *ptrs[ POINTERS_NUMBER ];
	} hazard;
} thread_ctx_t;

#endif
