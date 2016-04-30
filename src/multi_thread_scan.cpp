#include "multi_thread_scan.h"

/*
//all defined in main.cpp
//to collect numbers of blocks is scaned or not.
//++ in function thread_handle.
extern pthread_mutex_t scan_block_count_mutex;
extern uint64_t scaned_block_count;
extern uint64_t not_scaned_block_count;
*/

threadpool_t* create_threadpool(uint32_t min_thread_num, uint32_t max_thread_num, uint32_t queue_max_size) 
{
	threadpool_t *pool = NULL;
	do
	{
		if((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL)
		{
			printf("malloc threadpool failed!\n");
			break;
		}
		//init pool
		pool->min_thread_num = min_thread_num;
		pool->max_thread_num = max_thread_num;
		pool->live_thread_num = min_thread_num;
		pool->busy_thread_num = 0;
		pool->wait_for_exit_thread_num = 0;
		pool->queue_front = 0;
		pool->queue_rear = 0;
		pool->queue_curr_size = 0;
		pool->queue_max_size = queue_max_size;
		pool->shutdown = false;

		pool->threads = (pthread_t*) malloc (sizeof(pthread_t) * max_thread_num);
		if(pool->threads == NULL)
		{
			printf("malloc space for threads failed!\n");
			break;
		}
		memset(pool->threads, 0, sizeof(pthread_t) * max_thread_num);
        
        pool->task_queue = (threadpool_task_t*)malloc(sizeof(threadpool_task_t) * queue_max_size);
        if(pool->task_queue == NULL)
        {
            printf("malloc space for task_queue failed!\n");
            break;
        }

		if(    pthread_mutex_init(&(pool->lock), NULL) != 0
			|| pthread_mutex_init(&(pool->busy_thread_counter), NULL) != 0
			|| pthread_mutex_init(&(pool->request_block), NULL) != 0 
			|| pthread_cond_init(&(pool->queue_not_empty), NULL) != 0
			|| pthread_cond_init(&(pool->queue_not_full), NULL) != 0 )
		{
			printf("init the lock or mutex failed!\n");
			break;
		}
		//start work thread with min_thread_num
		for(int i = 0; i < min_thread_num; i++)
		{
			if(!pthread_create(&(pool->threads[i]), NULL, threadpool_thread, (void*)pool))
				printf("thread 0x%x  started\n", pool->threads[i]);
			else
				printf("create thread failed\n");
		}
		//start control thread
		pthread_create(&(pool->ctrl_thread_tid), NULL, ctrl_thread, (void*)pool);
		return pool;
		
	}while(0);

	printf("failed in func: create_threadpool()\n");
	threadpool_free(pool);
	return NULL;
}



void* threadpool_thread(void* threadpool)
{
	threadpool_t* pool = (threadpool_t*)threadpool;
	threadpool_task_t task;
	while(true)
	{
		pthread_mutex_lock(&(pool->lock));

		while((pool->queue_curr_size == 0) && (!pool->shutdown))
		{
			printf("thread 0x%x is waiting\n", pthread_self());
			pthread_cond_wait(&(pool->queue_not_empty), &(pool->lock));
			
			if(pool->wait_for_exit_thread_num > 0)
			{
				pool->wait_for_exit_thread_num--;
				if(pool->live_thread_num > pool->min_thread_num)
				{
					printf("thread 0x%x is exiting. (wait_for_exit_thread_num > 0)\n", pthread_self());
					pool->live_thread_num--;
					pthread_mutex_unlock(&(pool->lock)); //since the thread is quiting, no need to hold the lock
					pthread_exit(NULL); 
				}
			}
		}

		if(pool->shutdown)
		{
			pthread_mutex_unlock(&(pool->lock));
			printf("thread 0x%x is exiting. (thread pool has been shutdown.)\n", pthread_self());
			pthread_exit(NULL);
		}

		//get a task from queue        
        printf("\n~~~~~~~~~~~~~~\n");
        printf("in function threadpool_thread. (get task from queue), before task is accessed.\n");
        printf(" queue front is: %d ", pool->queue_front);
        printf(" queue rear is: %d ", pool->queue_rear);
        printf(" queue size is: %d ", pool->queue_curr_size);
        printf("\n!!!!!!!!!!!!!!\n");

		task.function = pool->task_queue[pool->queue_front].function;
		task.arg = pool->task_queue[pool->queue_front].arg;
        uint32_t tmp_front = pool->queue_front;
		pool->queue_front = (pool->queue_front + 1) % (pool->queue_max_size);
		pool->queue_curr_size--;

		//now queue must be not full
		pthread_cond_broadcast(&(pool->queue_not_full));

		pthread_mutex_unlock(&(pool->lock));
        
        thread_handle_args* tmp_test_arg = (thread_handle_args*)(task.arg);
		//get to work
		printf("thread 0x%x start working\n", pthread_self());
        printf("begin to handle block_id: %llu =====\n", tmp_test_arg->block_node->block_id);
		pthread_mutex_lock(&(pool->busy_thread_counter));
		pool->busy_thread_num++;
		pthread_mutex_unlock(&(pool->busy_thread_counter));
		(*(task.function))(task.arg); // work.
        
        pool->task_queue[tmp_front].arg = NULL;
        pool->task_queue[tmp_front].function = NULL;

		//task end
		printf("thread 0x%x end working\n", pthread_self());

        //printf("end to handle block_id: %llu =====\n", tmp_test_arg->block_node->block_id);
		pthread_mutex_lock(&(pool->busy_thread_counter));
		pool->busy_thread_num--;
		pthread_mutex_unlock(&(pool->busy_thread_counter));
	}

	//seems impossible to reach here. if enter here, means error happened.
	pthread_exit(NULL);
	return (NULL);
}

//the task is added to threadpool queue. waiting for execute.
int add_task_to_threadpool_queue(threadpool_t *pool, void*(*function)(void *arg), void* arg)
{
	assert(pool != NULL);
	assert(function != NULL);
	assert(arg != NULL);

	pthread_mutex_lock(&(pool->lock));

	while((pool->queue_curr_size == pool->queue_max_size) && (!pool->shutdown))
	{
		pthread_cond_wait(&(pool->queue_not_full), &(pool->lock));
	}

	if(pool->shutdown)
	{
		pthread_mutex_unlock(&(pool->lock));
		printf("thread pool terminated. (return in add_task_to_threadpool_queue)\n");
		// return when threadpool is shutdown.
		return -1;
	}

	
	
	//add a task to queue
	//1.0 clear former arg if exists.
	//2.0 reassign function and arg
	//3.0 update queue_curr_size & queue_rear
	
	//1.0
	//if(pool->task_queue[pool->queue_rear].arg != NULL)
	while(true)
	{
	    if(pool->task_queue[pool->queue_rear].arg != NULL)
        {
            printf("waiting...\n");
            usleep(10000); //sleep 10ms.
            continue;
        }
        else
        {
            printf("continue...\n");
            break;
        }
	}
	//2.0
    printf("\n**************\n");
    printf("in function add_task, before task is accessed.\n");
    printf(" queue front is: %d ", pool->queue_front);
    printf(" queue rear is: %d ", pool->queue_rear);
    printf(" queue size is: %d ", pool->queue_curr_size);
    printf("\n//////////////\n");

	pool->task_queue[pool->queue_rear].function = function; //function is thread_handle
	pool->task_queue[pool->queue_rear].arg = arg;
	pool->queue_rear = (pool->queue_rear + 1) % (pool->queue_max_size);
	pool->queue_curr_size++;

	//now queue must not be empty
	pthread_cond_signal(&(pool->queue_not_empty));
	
	pthread_mutex_unlock(&(pool->lock));

	return 0;
}


bool is_thread_alive(pthread_t tid)
{
    printf("entering is_thread_alive...\n");
	int kill_ret = pthread_kill(tid, 0);
	if(kill_ret == ESRCH)
		return false;	
	else
		return true;
}

void* ctrl_thread(void* threadpool)
{
	threadpool_t* pool = (threadpool_t*)threadpool;
	while(!pool->shutdown)
	{   
        printf("entering ctrl thread...\n");
		sleep(CTRL_TIME_ELAPSE);
		pthread_mutex_lock(&(pool->lock));
		uint32_t queue_size = pool->queue_curr_size;
		uint32_t live_thread_num  = pool->live_thread_num;
        printf("in ctrl_thread ==> current live thread number is %d \n", live_thread_num);
		pthread_mutex_unlock(&(pool->lock));

		pthread_mutex_lock(&(pool->busy_thread_counter));
		uint32_t busy_thread_num = pool->busy_thread_num;
        printf("in ctrl_thread ==> current busy thread number is %d \n", busy_thread_num);
		pthread_mutex_unlock(&(pool->busy_thread_counter));

		if(queue_size >= MIN_WAIT_TASK_NUMBER && live_thread_num < pool->max_thread_num)
		{
            printf("++++++++++++++++++add thread++++++++++++++++++++++++\n");
			//need to add thread
			pthread_mutex_lock(&(pool->lock));
			int add_count = 0;
			for(uint32_t i = 0; i < pool->max_thread_num; i++)
			{
				if(pool->threads[i] == 0 || !is_thread_alive(pool->threads[i]))
				{
					pthread_create(&(pool->threads[i]), NULL, threadpool_thread, (void *)pool);
					add_count++;
					pool->live_thread_num++;
				}	
			}
			pthread_mutex_unlock(&(pool->lock));
		}

		if((busy_thread_num * 2) < live_thread_num && live_thread_num > pool->min_thread_num)
		{
            printf("++++++++++++++++++delete thread++++++++++++++++++++++++\n");
			//too much idle thread, need to delete thread
			pthread_mutex_lock(&(pool->lock));
			pool->wait_for_exit_thread_num = DEFAULT_THREAD_VARIETY;
			pthread_mutex_unlock(&(pool->lock));
			//wake up DEFAULT_THREAD_VARIETY threads to exit
			for(int i = 0 ;i < DEFAULT_THREAD_VARIETY; i++)
			{
				pthread_cond_signal(&(pool->queue_not_empty));
			}
		}
		
	}
}

int threadpool_free(threadpool_t* pool)
{
	if(pool == NULL)	
	{
		return -1;
	}

	if(pool->task_queue)
	{
		free(pool->task_queue);
	}
	if(pool->threads)
	{
		free(pool->threads);
		pthread_mutex_lock(&(pool->lock));
		pthread_mutex_destroy(&(pool->lock));
		pthread_mutex_lock(&(pool->busy_thread_counter));
		pthread_cond_destroy(&(pool->queue_not_empty));;
		pthread_cond_destroy(&(pool->queue_not_full));
	}
	free(pool);
	pool = NULL;
	return 0;
}


int threadpool_destory(threadpool_t *pool)
{
	if(pool == NULL)
		return -1;

	pool->shutdown = true;
	//ctrl_thread exit first
	pthread_join(pool->ctrl_thread_tid, NULL);

	//wake up the waiting thread
	pthread_cond_broadcast(&(pool->queue_not_empty));
	for(int i = 0; i < pool->live_thread_num; i++)
	{
		pthread_join(pool->threads[i], NULL);
	}

	threadpool_free(pool);
	return 0;
	
}

//void*(*function)(void *arg)

void* thread_handle(void *arg)
{
	thread_handle_args* thread_arg = (thread_handle_args*)arg;
	
	//int ret = process_block_index_node(thread_arg->db_state, (thread_arg->block_node), thread_arg->v0, thread_arg->v0_update_set);
	int ret = process_block_index_node(thread_arg);
	if(ret == 0)
	{
		printf("block node %llu is scaned.===\n", thread_arg->block_node->block_id);
		pthread_mutex_lock(&scan_block_count_mutex);
		scaned_block_count++;
		pthread_mutex_unlock(&scan_block_count_mutex);
	}
	else
	{
		printf("block node %llu is NOT scaned.===\n", thread_arg->block_node->block_id);
		pthread_mutex_lock(&scan_block_count_mutex);
		not_scaned_block_count++;
		pthread_mutex_unlock(&scan_block_count_mutex);
	}
    free(thread_arg->block_node);
	thread_arg->block_node = NULL;
	free(thread_arg);
    thread_arg = NULL;

}
#if 0
//test
int main(void)
{
	threadpool_t *pool = create_threadpool(3,100,100);
	printf("=======pool inited======\n");

	printf("start to prepare single level block info...\n");
	// TODO: get block node level info
	printf("end to prepare single level block info...\n");
	
	//should be place in a for loop to handle all the block_node.
	pthread_mutex_lock(&(pool->request_block));
	block_index_node block_node;
	request_block_index_node(level_block_index_node, &block_node);
	pthread_mutex_unlock(&(pool->request_block));

	printf("add task. block id is  %llu =====\n", block_node.block_id);
	// TODO: get dbstate, block_node, v0
	std::vector<slice> v0;
	thread_handle_args arg;
	arg.db_state = 
	arg.block_node = &block_node;
	arg.v0 = &v0;
	add_task_to_threadpool_queue(pool, thread_handle, (void*)(&arg) );
//=========end for loop

	sleep(3);
	threadpool_destory(pool);
	
	return 0;
}
#endif
