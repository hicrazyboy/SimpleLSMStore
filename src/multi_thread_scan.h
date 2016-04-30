#include <pthread.h>
#include <errno.h>
#include <signal.h>

#include "appscan.h"
#define THREADNUMBER 3

#define CTRL_TIME_ELAPSE 15
#define MIN_WAIT_TASK_NUMBER 10
#define DEFAULT_THREAD_VARIETY 10

//all defined in main.cpp
//to collect numbers of blocks is scaned or not.
//++ in function thread_handle.
extern pthread_mutex_t scan_block_count_mutex;
extern uint64_t scaned_block_count;
extern uint64_t not_scaned_block_count;


typedef struct threadpool_task_t_
{
	void* (*function)(void*);
	void* arg;
}threadpool_task_t;

struct threadpool_t_
{
	pthread_mutex_t lock; //mutex for taskpool
	pthread_mutex_t busy_thread_counter; //mutex for count the busy thread
	pthread_mutex_t request_block; //block for request block node
	//since when a task runs over, busy_thread_counter need to --, so a mutex is necessary.
	pthread_cond_t queue_not_full;
	pthread_cond_t queue_not_empty;
	pthread_t *threads;
	pthread_t ctrl_thread_tid;
	threadpool_task_t *task_queue;
	uint32_t min_thread_num;
	uint32_t max_thread_num;
	uint32_t live_thread_num;
	uint32_t busy_thread_num;
	uint32_t wait_for_exit_thread_num;
	uint32_t queue_front;
	uint32_t queue_rear;
	uint32_t queue_curr_size;
	uint32_t queue_max_size;
	bool shutdown;	
};
typedef threadpool_t_ threadpool_t;

/*
struct thread_handle_args_
{
	Db_state *db_state;
	block_index_node* block_node;
	std::vector<slice>* v0;
    std::vector<slice>* v0_update_set;
};
typedef thread_handle_args_ thread_handle_args; 
*/

threadpool_t* create_threadpool(uint32_t min_thread_num, uint32_t max_thread_num, uint32_t queue_max_size);

void* threadpool_thread(void* threadpool);

int add_task_to_threadpool_queue(threadpool_t *pool, void*(*function)(void *arg), void* arg);

bool is_thread_alive(pthread_t tid);

void* ctrl_thread(void* threadpool);

int threadpool_free(threadpool_t* pool);

int threadpool_destory(threadpool_t *pool);

void* thread_handle(void *arg);



