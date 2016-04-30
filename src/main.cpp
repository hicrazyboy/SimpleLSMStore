/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: main.cpp
  Author:        Version :          Date:
  Description:         
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/

#include "format.h"
#include "block.h"
#include "table.h"
#include "impl.h"
#include "random.h"
#include "testutil.h"
#include "log.h"
#include "kv_iter.h"

#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <list>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <time.h>

//#include <sys/time.h>

using namespace std;
using namespace test;

#define KEY_SEQUENTIAL 0

static const std::string log_fname = "Log.txt";
static const char store_fname[] = "./usertable.txt";
static const uint32_t key_range = 100000;
static const uint32_t beyond_key_range = 100000;
static const uint32_t test_number = 100000;

#if 1 //DEBUG
    FILE * fp;
#endif
extern FILE* file_log;

// ===  FUNCTION  ======================================================================
//         Name:  db_close
//  Description:  
// =====================================================================================
	void
db_close ( Db_state * db_state )
{
    fprintf(stdout, "%s begin!\n", __func__);
    close(db_state->storage_info->fd);
    
    pthread_mutex_destroy(&db_state->db_info->mem_mutex);
    pthread_cond_destroy(&db_state->db_info->mem_condv);
    pthread_mutex_destroy(&db_state->db_info->index_mutex);
    pthread_mutex_destroy(&db_state->db_info->sequence_mutex);

    delete[] db_state->db_info->list_of_sst_index_node;
    for(int i = 0; i < MAX_LEVEL; i++) {
        delete[] db_state->db_info->list_of_block_index_node[i];
    }
    
    for(uint32_t i = 0; i < MAX_LEVEL; ++i) {
        free(db_state->compaction_info->compact_pointer_[i]);
    }
    
    delete db_state->storage_info->map_l2p;
    
    free(db_state->db_info);
    free(db_state->compaction_info);
    free(db_state->storage_info);
    free(db_state->db_info->list_of_block_index_node);
    free(db_state);
    fprintf(stdout, "%s end!\n", __func__);
    return;
}		// -----  end of function db_close  ----- 

// ===  FUNCTION  ======================================================================
//         Name:  db_init
//  Description:  
// =====================================================================================
Db_state * db_init(Db_state * db_state)
{
    fprintf(stdout, "%s begin!\n", __func__);
    //-----------------------------------------------------------------------------
    // Init db_state below 
    //-----------------------------------------------------------------------------
    Db_state tmp_db_state;
    db_state = (Db_state *)malloc(sizeof(Db_state));
    memcpy(db_state, &tmp_db_state, sizeof(tmp_db_state));

    //init the memble of struct Db_state
    Db_info tmp_db_info;
    db_state->db_info = (Db_info *)malloc(sizeof(Db_info));
    memcpy(db_state->db_info, &tmp_db_info, sizeof(tmp_db_info));
    
    Compaction_info tmp_c_info;
    db_state->compaction_info = (Compaction_info *)malloc(sizeof(Compaction_info));
    memcpy(db_state->compaction_info, &tmp_c_info, sizeof(tmp_c_info));
    
    Memtable * mem;
    Memtable * imm = NULL;

    //Init memtable
    mem = (Memtable*) malloc (sizeof(Memtable)); // free in memtable dump.
    new_memtable(mem);

    std::list<block_index_node>** block_index_list_level_ 
		= (std::list<block_index_node>**)malloc(MAX_LEVEL * sizeof(std::list<block_index_node>*));
    block_index_list_level_[0] = new std::list<block_index_node>[LEVEL0_SST_NUM];	
    block_index_list_level_[1] = new std::list<block_index_node>[LEVEL1_INDEX_NUM];	
    block_index_list_level_[2] = new std::list<block_index_node>[LEVEL2_INDEX_NUM];	
    block_index_list_level_[3] = new std::list<block_index_node>[LEVEL3_INDEX_NUM];	
    block_index_list_level_[4] = new std::list<block_index_node>[LEVEL4_INDEX_NUM];	
    block_index_list_level_[5] = new std::list<block_index_node>[LEVEL5_INDEX_NUM];	
    block_index_list_level_[6] = new std::list<block_index_node>[LEVEL6_INDEX_NUM];	

    //init struct Db_info
    pthread_mutex_init(&db_state->db_info->mem_mutex, NULL);
    pthread_cond_init( &db_state->db_info->mem_condv, NULL);
    pthread_mutex_init(&db_state->db_info->index_mutex, NULL);
    db_state->db_info->ptr_memtable = mem;
    db_state->db_info->ptr_immutable = imm;
    db_state->db_info->list_of_block_index_node = block_index_list_level_;
    db_state->db_info->list_of_sst_index_node = new std::list<sst_index_node> [MAX_LEVEL];
    db_state->db_info->level0_info.cursor_start = 0;
    db_state->db_info->level0_info.cursor_end  = 0;
    db_state->db_info->sequence = 0;
    pthread_mutex_init(&db_state->db_info->sequence_mutex, NULL);

    //init struct Compaction_info
    db_state->compaction_info->level = 0; //the upper level to compact, level + 1 is the lower level to compact 
    db_state->compaction_info->bg_compaction_scheduled = false;
    db_state->compaction_info->compaction_score = 0;                  //the maxize score among all the levels, each score <1, or > 1 
    db_state->compaction_info->compaction_level = -1;                  //the level of sstable to compact
    db_state->compaction_info->sst_to_compact_for_seek = NULL;          //the sstable to compact due to allowed_seeks //GaoLong modified the type to std::list<sst_index_node*>
    db_state->compaction_info->sst_to_compact_for_seek_level = -1;    //the level of the sstable 
    for(uint32_t i = 0; i < MAX_LEVEL; ++i) {
        db_state->compaction_info->compact_pointer_[i] = (char *) malloc ( MAX_USERKEY_SIZE_BYTE + 8 + 1); //the max internal key of the last compacted key range: length = user_key.size + 8 //free in db_close()
        memset(db_state->compaction_info->compact_pointer_[i], 0x00, MAX_USERKEY_SIZE_BYTE + 8 + 1);
    }
    db_state->compaction_info->consecutive_compaction_errors = 0;

    //init storage
    Storage_info tmp_s_info;
    tmp_s_info.map_l2p = new std::map<uint64_t, Physical_addr>;
    init_storage(&tmp_s_info);
    db_state->storage_info = (Storage_info *) malloc (sizeof(Storage_info));
    memcpy(db_state->storage_info, &tmp_s_info, sizeof(tmp_s_info));
    db_state->storage_info->fd = open(store_fname, O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE , 0644);
    
    //declare and init struct Bg_pthread_quene
    pthread_mutex_init(&cq.mu, NULL);
    pthread_cond_init(&cq.bgsignal, NULL);
    cq.started_bgthread = false;
    
    //-----------------------------------------------------------------------------
    // Init db_state end 
    //-----------------------------------------------------------------------------
    fprintf(stdout, "%s end!\n", __func__);
    return db_state;
}

status put_customize(Db_state * db_state, double dup_rate, const int first_index)
{
    fprintf(stdout, "%s begin!\n", __func__);
    status s = e_Ok;
    const uint32_t num_put = test_number;//10737418;//10737418;  //2^30 / 100 = 10737418.24   // 2^32-1
    const size_t key_len = MAX_USERKEY_SIZE_BYTE;
    const size_t value_len = 68;
  
    RandomGenerator gen;
    
    fp = fopen("log_put_customize.txt", "w+");
    
    Random rand(919);
    for(int i = 0; i < first_index; ++i) rand.Next();
    
    uint32_t dup_num = 0;

    for(int i = 0; i < num_put; i++) {
        slice s_key, s_value;
        s_key.size = key_len;
        s_value.size = value_len;
        s_key.data = (char *) malloc (key_len + 1);
        s_value.data = (char *) malloc (value_len + 1);
        memset(s_key.data, 0x00, key_len + 1);
        memset(s_value.data, 0x00, value_len + 1);

        //重复率大于预设值时，创建一个非重复值
        int k;
        if( ((dup_num + 1) / static_cast<double>(i+1)) > dup_rate) {
            k = rand.Next() % beyond_key_range + key_range;
        } else {
            k = rand.Next() % key_range;
            ++dup_num;
        }
        
        snprintf(s_key.data, key_len + 1, "%0*d", key_len, k);
        s = gen.Generate(value_len, &s_value);

        s = db_put(db_state, s_key, s_value);
        fprintf( fp, "%s\t%s\n", s_key.data, s_value.data);        
        if( s != e_Ok ) {
            fprintf(stderr, "error : %d, %d\n", __LINE__, s);
        }
        fflush(fp);
        free(s_key.data);
        free(s_value.data);
    }
    fclose(fp);
    
    fprintf(stdout, "%s end!\n", __func__);
    return s;
}


status put_random(Db_state * db_state)
{
    fprintf(stdout, "%s begin!\n", __func__);
    status s = e_Ok;
    const uint32_t num_put = test_number;//10737418;//10737418;  //2^30 / 100 = 10737418.24   // 2^32-1
    const size_t key_len = MAX_USERKEY_SIZE_BYTE;
    const size_t value_len = 76;
  
    RandomGenerator gen;
    
    fp = fopen("log_put_random.txt", "w+");
    
    Random rand(919);

    for(int i = 0; i < num_put; i++) {
        slice s_key, s_value;

        s_key.size = key_len;
        s_value.size = value_len;
        s_key.data = (char *) malloc (key_len + 1);
        s_value.data = (char *) malloc (value_len + 1);
        memset(s_key.data, 0x00, key_len + 1);
        memset(s_value.data, 0x00, value_len + 1);
        
        const int k = rand.Next() % key_range;
        snprintf(s_key.data, key_len + 1, "%0*d", key_len, k);
        s = gen.Generate(value_len, &s_value);

        s = db_put(db_state, s_key, s_value);
        fprintf( fp, "%s\t%s\n", s_key.data, s_value.data);        
        if( s != e_Ok ) {
            fprintf(stderr, "error : %d, %d\n", __LINE__, s);
        }
        free(s_key.data);
        free(s_value.data);
    }
    fclose(fp);
    
    fprintf(stdout, "%s end!\n", __func__);
    return s;
}

status put_seq(Db_state * db_state)
{
    fprintf(stdout, "%s begin!\n", __func__);
    status s = e_Ok;
    const uint32_t num_put = test_number;//10737418;//10737418;  //2^30 / 100 = 10737418.24   // 2^32-1
    const size_t key_len = MAX_USERKEY_SIZE_BYTE;
    const size_t value_len = 76;
  
    RandomGenerator gen;
    
    fp = fopen("log_put_seq.txt", "w+");

    for(int i = 0; i < num_put; i++) {
        slice s_key, s_value;

        s_key.size = key_len;
        s_value.size = value_len;
        s_key.data = new char[key_len + 1];
        s_value.data = new char[value_len + 1];
        
        snprintf(s_key.data, key_len + 1, "%0*d", key_len, i);
        s = gen.Generate(value_len, &s_value);

        s = db_put(db_state, s_key, s_value);
        fprintf( fp, "%s\t%s\n", s_key.data, s_value.data);        
        if( s != e_Ok ) {
            fprintf(stderr, "error : %d, %d\n", __LINE__, s);
        }
        delete s_key.data;
        delete s_value.data;
    }
    fclose(fp);
    fprintf(stdout, "%s end!\n", __func__);    
    return s;
}

status get_seq(Db_state * db_state)
{
    fprintf(stdout, "%s begin!\n", __func__);
    status s = e_Ok;
    fp = fopen("log_get_seq.txt", "w");
    uint32_t get_number = test_number;
    const size_t key_len = MAX_USERKEY_SIZE_BYTE;
    //const size_t value_len = 76;
    for ( uint32_t count = 1; count < get_number; ++count ) {
        slice s_key;
        s_key.size = key_len;
        s_key.data = new char[key_len + 1];
        snprintf(s_key.data, key_len + 1, "%0*d", key_len, count);
        slice s_value_get; 
        s = db_get(db_state, s_key, &s_value_get);
        if (s == e_Ok) {
            fprintf(fp, "%s\t%s\n", s_key.data, s_value_get.data );
            free(s_value_get.data);// malloc inside skiplist_get or db_get
        } else {
            fprintf(fp, "%s\t%s\n", s_key.data, msg_status[s]);
        }
        delete s_key.data;
    }    
    fclose(fp);
    fprintf(stdout, "%s end!\n", __func__);    
    return s;
}

void wait_for_compaction(Db_state * db_state)
{
    fprintf(stdout, "%s begin!\n", __func__);
    while(db_state->compaction_info->bg_compaction_scheduled) {
        sleep(5);
    }
    fprintf(stdout, "%s end!\n", __func__);    
    return;
}

status test_put_get(Db_state * db_state)
{
    fprintf(stdout, "%s begin!\n", __func__);
    status s = e_Ok;
    uint32_t put_number = 1048576;
    size_t key_len = 8;
    size_t value_len = 174;
    RandomGenerator gen;
    fp = fopen("log.txt", "w+");
    for ( uint32_t count = 1; count <= put_number; ++count ) {
        slice s_key; 
        s_key.data = (char *) malloc (key_len + 1); //free below
        memset(s_key.data, 0x00, key_len + 1);
        s = gen.Generate(key_len, &s_key);

        slice s_value; 
        s_value.data = (char *) malloc (value_len + 1);  //free below
        memset(s_value.data, 0x00, value_len + 1);
        s = gen.Generate(value_len, &s_value);

        s = db_put(db_state, s_key, s_value);
        if( s != e_Ok ) {
            fprintf(stderr, "error : %d, %d\n", __LINE__, s);
        }
        slice s_value_get;
        db_get(db_state, s_key, &s_value_get);
        fprintf( fp, "%s\n%s\n%s\n", s_key.data, s_value.data, s_value_get.data);
        if( memcmp (s_value.data, s_value_get.data, s_value.size) ) {
            fprintf(stderr, "%s() : %d value not equal!\n", __func__, __LINE__);
            exit(-1);
        }

        free(s_key.data);
        free(s_value.data);
        free(s_value_get.data);
    }	
    fclose(fp);
    fprintf(stdout, "%s end!\n", __func__);    
    return s;
}

int main(int argc, char *argv[])
{
    file_log = fopen(log_fname.c_str(), "w");
    status s = e_Ok;
    Log(file_log, "ZKV begin! -- line %d\n", __LINE__);

    Db_state * db_state;

    db_state = db_init(db_state);	

    if ( s != e_Ok) {
        abort();
    }

    maybe_schedule_compaction(db_state);
    if ( s != e_Ok )
    {
        exit(-1);
    }

    //s = put_seq(db_state);
    s = put_random(db_state);
    //test_put_get(db_state);
    
#if 0 // print level 0 cursor 0
std::list<internal_key_value> kv_prt;
parse_block2kv(db_state, &(db_state->db_info->list_of_block_index_node[0][0].front()), &kv_prt);

for(std::list<internal_key_value>::iterator iter = kv_prt.begin(); 
     iter != kv_prt.end(); ++iter) {
    fprintf(stdout, "%s\n", iter->key.i_key.c_str());
}

#endif
    
    #if 0
    wait_for_compaction(db_state);
    timeval start_time, end_time;
    double time_use;
    gettimeofday(&start_time, NULL);
    s = get_seq(db_state);
    gettimeofday(&end_time, NULL);
    time_use = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    fprintf(stdout, "Db_get use time: %lf u_seconds!\n", time_use);
    
    //LogDBStateSize(file_log, db_state);
    fprintf(stdout, "System down. Waiting for break!\n");
    #endif
    wait_for_compaction(db_state);
    LogDBStateSize(file_log, db_state);
    db_close(db_state);
    return 0;
} 

