/*************************************************
  Copyright (C), 2013,  ict.ac
  File name:     
  Author:       Version:        Date: 
  Description:    // 用于详细说明此程序文件完成的主要功能，与其他模块
                  // 或函数的接口，输出值、取值范围、含义及参数间的控
                  // 制、顺序、独立或依赖等关系
  Others:         // 其它内容的说明
  Function List:  // 主要函数列表，每条记录应包括函数名及功能简要说明
    1. ....
  History:        // 修改历史记录列表，每条修改记录应包括修改日期、修改
                  // 者及修改内容简述
    1. Date:
       Author:
       Modification:
    2. ...
*************************************************/

#ifndef IMPL_H_
#define IMPL_H_
#include "block.h"
#include "table.h"
#include "util.h"
#include "format.h"
#include "storage.h"
#include "memtable.h"

#include <string>
#include <pthread.h>
#include <deque>
#include <list>
#include <vector>
#include <stdint.h>
#pragma comment(lib, "pthreadVC2.lib")
#ifdef DEBUG
	//extern FILE * fp;
#endif

struct Bg_item { void* arg; void (*function)(void*); };
typedef std::deque<struct Bg_item> Bg_queue;

struct Bg_pthread_quene_{
    Bg_queue queue;
    pthread_mutex_t mu;
    pthread_cond_t bgsignal;
    pthread_t bgthread;
    bool started_bgthread;
};
typedef Bg_pthread_quene_ Bg_pthread_quene;

struct Db_info_
{
//below is protected by mem_mutex
    pthread_mutex_t mem_mutex;
    pthread_cond_t mem_condv;
    Memtable * ptr_memtable;
    Memtable * ptr_immutable;
    
    pthread_mutex_t index_mutex;//Needed??
    std::list<block_index_node>** list_of_block_index_node;   //the global block index node list array
    std::list<sst_index_node>* list_of_sst_index_node;     //the global sst index node list array
    
//sequence is protected by sequence_mutex 
    pthread_mutex_t sequence_mutex;
    uint64_t sequence;
    Level0_info level0_info;
};
typedef Db_info_ Db_info;


struct Compaction_info_
{
    int32_t level; //the upper level to compact, level + 1 is the lower level to compact 
    bool bg_compaction_scheduled;
    double compaction_score;                  //the maxize score among all the levels, each score <1, or > 1 
    int32_t compaction_level;                  //the level of sstable to compact
    sst_index_node* sst_to_compact_for_seek;          //the sstable to compact due to allowed_seeks //GaoLong modified the type to std::list<sst_index_node*>
    int32_t sst_to_compact_for_seek_level;    //the level of the sstable 
    char* compact_pointer_[MAX_LEVEL];   //the max internal key of the last compacted key range
    uint32_t consecutive_compaction_errors;
};
typedef Compaction_info_ Compaction_info;

//yueyinliang/2013/11/14: modify several parameters
struct Compaction_
{
    int32_t level; //the upper level to compact, level + 1 is the lower level to compact 
    int32_t level_dump; //the level for dump to
    bool is_dump;
    std::list<sst_index_node*> inputs_[2]; 
    std::list<sst_index_node*> grandparents; //all the sstable index blocks whose key in key range being compacted
    std::list<sst_index_node*>::iterator grandparent_index; //Gaolong add 2013/11/18 : the index of grandparent that has been overlaped
    bool seen_key;   //yueyinliang/2013/11/14 : used for the key compare of multiple sequent keys during the compaction, gaolong[add]/2013/11/18  some output key has been seen
    uint64_t overlapped_bytes; //the ouput of L[i] and L[i+1] overlapped with the L[i+2], should smaller than a predefined bytes, such as 25*2MB
    #if 0
     std::list<block_index_node*>::iterator level_ptrs_[MAX_LEVEL];//GaoLong add 2013/11/18 19:55:42:for use of function is_base_level_for_key()
    #else
     std::list<sst_index_node>::iterator level_ptrs_[MAX_LEVEL];//GaoLong add 2013/11/18 19:55:42:for use of function is_base_level_for_key()
    #endif
    internalkey smallest[2], largest[2];//GaoLong add 2013/11/20
    std::list<block_index_node*> block_node_list_outputs;//GaoLong add 2013/11/20
    std::list<sst_index_node*> sst_node_list_outputs;//GaoLong add 2013/11/20
    bool is_seek_compaction;    //GaoLong added 2013/12/30
};
typedef Compaction_ Compaction;

//yueyinliang/2013/11/14: 
//GaoLong modified names 2013/11/19:list_of_block_index_node,list_of_sst_index_node
struct Db_state_
{
    Db_info * db_info;
    Compaction_info * compaction_info;
    Storage_info * storage_info;
};
typedef Db_state_ Db_state;

extern Bg_pthread_quene cq;

/*******************  Functions here  *******************/
//GaoLong added 2013/11/27
//Phread priority quene
void pthread_log(const char* label, int result);

void bg_schedule(void (*function)(void*), void* arg);

void* bg_thread(void* arg);
//uint64_t g_file_number = 0;

//GaoLong 2013/11/18 19:39:23
//Get the sstable's key_range according to sst_index list
//called by select_other_inputs
void get_range_of_input(
        const std::list<sst_index_node*>& inputs, 
        internalkey* smallest, 
        internalkey* largest);

void get_range_of_two_inputs(
        std::list<sst_index_node*> &inputs1,
        std::list<sst_index_node*> &inputs2,
        internalkey* smallest,
        internalkey* largest);

//to judge whether the system needs compaction? 
bool needs_compaction(Compaction_info * c_info);

//GaoLong 2013/11/11
//to judge :iff we should schedule compaction
void maybe_schedule_compaction(Db_state * db_state);

void background_call(Db_state * db_state);

status background_compaction(Db_state * db_state);

//GaoLong added 2013/11/26
//called by do_compaction_work() / next_valid_input()
//To judge whether the kv should be drop?
bool should_drop(bool &is_first_of_input_list, 
    std::list<internal_key_value>::iterator &iter,
    slice &current_user_key,
    const Db_state * db_state,
    Compaction * c);

//do compaction of level1-N after get inputs_[0,1]
status do_compaction_work(Db_state * db_state, Compaction * c);

//Gaolong add 2013/11/18
//to judge iff we should stop generating this sstable and flush the block of the sstable right now.
bool should_stop_before(slice user_key, Compaction * c);


//Gaolong add 2013/11/19
// called by should_drop()
// Returns true if the information we have available guarantees that
// the compaction is producing data in "level+1" for which no data exists
// in levels greater than "level+1".
bool is_base_level_for_key(const slice user_key, const Db_state * db_state, Compaction * c);


/* make a merging list through c->inputs_[0,1]: 
 * read the block according to the key range of c->inputs_[0,1]
 * sorted by:1. user_key, 2.tag(64bits)
 *
input: input[0,1]: sstable_index_node
step1: get block index node from sstable_index_node, and build a list <block_index_node>
step2: parse_block2kv(), and call list::push_back() put the key/value pairs to a list <kv>
step3: sort
 */
std::list<internal_key_value> merge_input_to_kvlist(Db_state * db_state, Compaction * c);

//dump the immutable memtable to level0
status compact_memtable(Db_state * db_state, Compaction * c);

//GaoLong modified the argumemt
status block_index_update(Db_info * db_info, Compaction * c); // the args

//GaoLong modified the argumemt
status sst_index_update(Db_info * db_info, Compaction * c);

bool after_file(const slice smallest_user_key, const sst_index_node* p_sst_node);

bool before_file(const slice largest_user_key, const sst_index_node* p_sst_node);

bool some_file_overlaps_range(bool disjoint_sorted_files, const std::list<sst_index_node> sst_file_list, const slice smallest_user_key, const slice largest_user_key);

bool overlap_in_level(Db_state* state, uint32_t level, const slice smallest_user_key, const slice largest_user_key);

uint32_t pick_level_for_memtable_output(Db_state* state, internalkey smallest_internalkey, internalkey largest_internalkey);

status write_level0_sst(Db_state* state, Memtable * memtable, Compaction * c);

bool is_trivial_move(Compaction * c);

status pick_compaction(Db_state * db_state, Compaction * c);

//to expand sstables
void select_other_inputs(Db_state * db_state, Compaction * c);

//Gaolong add 2013/11/18 19:24:01
//Store in "*inputs" all files in "level" that overlap [begin, end]
void get_overlapping_inputs(
    const int32_t level,
    const internalkey *begin,
    const internalkey *end,
    std::list<sst_index_node*> *inputs,
    const Db_info * db_info);


//GaoLong 2013/11/20
//The interface of storage
//s1.1: Generate a block_index_node, put necessary members into the block_index_node, and push_back to the block_node_list_outputs
//s1.2: if the first block of the sstable then generate a new sst_index_node, put necessary members into the sst_index_node, and push_back to the block_node_list_outputs
//s2: Finish flushing the block, deal with the exception
//s3: Loging "new levelN block to logical_addr ..."
status finish_compaction_output_block(
        Db_state * db_state,
        Compaction* c,                              //this argument is for print log
        const std::string &block_buf,
        const std::vector<slice>& keys_,        
        const internalkey min_key,
        const internalkey max_key,
        bool &is_first_block);

//GaoLong finished 2013/12/2
//Function:
//Update the compaction_score and compaction_level:
//Alg: 
//  Calculate the max score and corresponding level;
//  then assign these to compaction_score and compaction_level.
//Called in the last stage of compaction, after updating the sstable and block list : 
//called by 1. compact_memtable(), 2. background_compaction().
status level_score_update(Db_state * db_state);

//GaoLong finished 2013/11/29
//Five conditions(Priority decrease) as follows:
// (1). Level0 sstable number >= LO_COMPACTION_TRIGGER; then delay for some seconds
// (2). memtable size + cur_key_value <= MAX_MEMTABLE_SIZE_BYTE ; then break;
// (3). immutable != NULL && memtable size + cur_key_value > MAX_MEMTABLE_SIZE_BYTE, then wait for immutable dump
// (4). Level0 sstable number >= L0_STOP_WRITES_TRIGGER; then wait for compaction
// (5). else switch to a new memtable and trigger compaction of old
status make_room_for_put(Db_state* db_state, size_t kv_size);

bool update_allowed_seeks(Compaction_info * compaction_info, 
                                            sst_index_node *seek_sst, 
                                            const int32_t seek_level);

//GaoLong removed 2013/11/20 
//s1:Loging "Compacted number@level + number@level+1 files => total bytes"
//s2:
#if 0
status install_compaction_results(db_state* state);
#endif



status db_put(Db_state * db_state, const slice key, const slice value);

status db_delete(Db_state  * db_state, const slice key);

status db_get( Db_state * db_state, const slice key, slice* value);

status db_scan(void);

#endif //IMPL_H_
