/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: impl.cpp
  Author:        Version :          Date:
  Description: for 
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/
#include "impl.h"
#include "format.h"
#include "util.h"
#include "storage.h"
#include "kv_iter.h"
#include "memtable.h"
#include "skiplist.h"
#include "table.h"
#include "block.h"
#include "log.h"
#include "bloomfilter.h"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string>
#include <list>
#include <vector>
#include <deque>
#include <assert.h>
#include <iterator>
#include <errno.h>
#include <stdint.h>
#include <windows.h>

#ifdef DEBUG
	extern FILE * fp;
#endif

#define BLOOM_FILTER 1
Bg_pthread_quene cq;

//GaoLong added 2013/11/27
void pthread_log(const char* label, int result)
{
    if (result != 0 && result != EBUSY) {
        fprintf(strerror_s, "pthread %s: %s\n", label, strerror(result));
        abort();
    } else if ( result == EBUSY) {
		//fprintf(stdout, "pthread %s: %s\n", label, strerror(result));
	}
}

void bg_schedule(void (*function)(void*), void* arg)
{
    pthread_log("lock", pthread_mutex_lock(&cq.mu) );

    // Start background thread if necessary
    if (!cq.started_bgthread) {
        cq.started_bgthread = true;
        pthread_log(
            "create thread",
            pthread_create(&cq.bgthread, NULL,  &bg_thread, NULL));
    }

    // If the queue is currently empty, the background thread may currently be
    // waiting.
    if (cq.queue.empty()) {
        pthread_log("signal", pthread_cond_signal(&(cq.bgsignal)) );
    }

    // Add to priority queue
    //Bg_item new_bg_item;
    cq.queue.push_back(Bg_item());
    cq.queue.back().function = function;
    cq.queue.back().arg = arg;

    pthread_log("unlock", pthread_mutex_unlock(&(cq.mu)) );
}

void* bg_thread(void* varg)
{
    while (true) {
        // Wait until there is an item that is ready to run
        pthread_log( "lock", pthread_mutex_lock(&cq.mu) );
        while (cq.queue.empty()) {
            pthread_log("wait", pthread_cond_wait(&(cq.bgsignal), &(cq.mu)));
        }

        void (*function)(void*) = cq.queue.front().function;
        void* arg = cq.queue.front().arg;
        cq.queue.pop_front();

        pthread_log( "unlock", pthread_mutex_unlock(&(cq.mu)) );
        (*function)(arg);
    }
}

//GaoLong finished 2013/11/19
//get the key range of L0, and then used for selecting sstbles in Li+1 as inputs[1]
void get_range_of_input(
        const std::list<sst_index_node*>& inputs, 
        internalkey* smallest, 
        internalkey* largest)
{
    assert(!inputs.empty());
    smallest->i_key.clear();
    largest->i_key.clear();
    for (std::list<sst_index_node*>::const_iterator iter = inputs.begin(); iter != inputs.end(); ++iter) {
        sst_index_node* sst = *iter;
        if ( iter == inputs.begin() ) {
            *smallest = sst->min_key;
            *largest = sst->max_key;
        } else {
            if (internalkey_compare(sst->min_key, *smallest) < 0) {
                *smallest = sst->min_key;
            }
            if (internalkey_compare(sst->max_key, *largest) > 0) {
                *largest = sst->max_key;
            }
        }
    }

}

//GaoLong removed 2013/11/18 20:40:05
//GaoLong finished 2013/11/27
void get_range_of_two_inputs(
        std::list<sst_index_node*> &inputs1,
        std::list<sst_index_node*> &inputs2,
        internalkey* smallest,
        internalkey* largest)
{
    std::list<sst_index_node*> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    get_range_of_input(all, smallest, largest);
}

//GaoLong finished 2013/11/11
bool needs_compaction(Compaction_info * c_info)
{
     return (c_info->compaction_score >= 1)
                    || ( NULL != c_info->sst_to_compact_for_seek );
}

//GaoLong finished 2013/11/11
void maybe_schedule_compaction(Db_state * db_state)
{
    //Assert mem_mutex is held.
    if (db_state->compaction_info->bg_compaction_scheduled) {
        //do nothing
    } else if (db_state->db_info->ptr_immutable == NULL  && !needs_compaction(db_state->compaction_info)) {
        //do nothing
    } else {
        db_state->compaction_info->bg_compaction_scheduled = true;
        bg_schedule((void (*) (void*))&background_call, (void*)db_state);   //Add a new thread to execute function background_call.
    }
}

//GaoLong finished 2013/11/11
//Background thread to execute compaction
void background_call(Db_state * db_state)
{
//Log("background_call(), %d", __LINE__);
    pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
    status s = background_compaction(db_state);
    if (s == e_Ok){//this compact has finished.
        //Ok
        db_state->compaction_info->consecutive_compaction_errors = 0;
    } else {
        pthread_log("broadcast", pthread_cond_broadcast(&db_state->db_info->mem_condv) );  // In case a waiter can proceed despite the error
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
        ++db_state->compaction_info->consecutive_compaction_errors;
        int seconds_to_sleep = 1;
        //At most sleep for 2^3 (= 8) seconds. 
        for (uint32_t i = 0; i < 3 && i < db_state->compaction_info->consecutive_compaction_errors - 1; ++i) {
            seconds_to_sleep *= 2;
        }
        Sleep(seconds_to_sleep * 1000);    //Done: the strategy of seconds_to_sleep
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
    }

    db_state->compaction_info->bg_compaction_scheduled = false;
    //pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );

    maybe_schedule_compaction(db_state);
    pthread_log("broadcast", pthread_cond_broadcast(&db_state->db_info->mem_condv) );
    pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
}

//GaoLong finished 2013/11/11
status background_compaction(Db_state * db_state)
{
//Log("background_compaction(), %d", __LINE__);
    //Assert mem_mutex is held.
    status s = e_Ok;
    Compaction c;
    c.level = -1;
    c.level_dump = -1;
    c.grandparent_index = c.grandparents.begin();//We haven't assigned grandparent, can we set grandparent_index?
    c.seen_key = false;
    c.overlapped_bytes = 0;
    c.is_seek_compaction = false;
    
    if (db_state->db_info->ptr_immutable != NULL) {
        c.is_dump = true;
        s = compact_memtable(db_state, &c);
    } else {
        c.is_dump = false;    
        for(int i = 0; i < MAX_LEVEL; i++) {
            c.level_ptrs_[i] = db_state->db_info->list_of_sst_index_node[i].begin();
        }
        s = pick_compaction(db_state, &c); //return the selected sstables recoreded in input[0,1]
        assert(s == e_Ok);

LogDBState(file_log, db_state, &c, e_LogCompBegin);
#ifdef DETAIL_LOG
int idx0 = 0;
for(std::list<sst_index_node*>::iterator iter = c.inputs_[0].begin(); iter != c.inputs_[0].end(); ++iter, ++idx0) {
    Log(file_log, "pick: L%d sst_id:%u min_key0#%d %s  max_key0#%d %s", 
        c.level, (*iter)->sst_id,
        idx0, (*iter)->min_key.i_key.c_str(), 
        idx0,  (*iter)->max_key.i_key.c_str());
}

int idx1 = 0;
for(std::list<sst_index_node*>::iterator iter = c.inputs_[1].begin(); iter != c.inputs_[1].end(); ++iter, ++idx1) {
    Log(file_log, "pick: L%d sst_id:%u min_key1#%d %s  max_key1#%d %s", 
    c.level + 1, (*iter)->sst_id,
    idx1, (*iter)->min_key.i_key.c_str(), 
    idx1, (*iter)->max_key.i_key.c_str());
}
#endif

#if 1 
//if( c.inputs_[0].front()->sst_id == 95617024) {//95617024
//    printf(" sleep waiting for debug \n");
//    sleep(100);
//}

Log(file_log, "compact range : min_key0 %s  max_key0 %s",  
        c.inputs_[0].front()->min_key.i_key.c_str(), 
        c.inputs_[0].back()->max_key.i_key.c_str());
if(!c.inputs_[1].empty()) {
Log(file_log, "compact range : min_key1 %s  max_key1 %s",  
        c.inputs_[1].front()->min_key.i_key.c_str(), 
        c.inputs_[1].back()->max_key.i_key.c_str());
}
#endif


#ifdef DETAIL_LOG

size_t comp_size_bytes = 0;
for(std::list<sst_index_node*>::iterator iter = c.inputs_[0].begin();
        iter != c.inputs_[0].end();
        ++iter) {
        comp_size_bytes += (*iter)->size;
    }

    for(std::list<sst_index_node*>::iterator iter = c.inputs_[1].begin();
            iter != c.inputs_[1].end();
            ++iter) {
        comp_size_bytes += (*iter)->size;
    }
Log(file_log, "compacting sstable total size: %d -- line %d", comp_size_bytes, __LINE__);	
#endif

        if(is_trivial_move(&c)) {
#if 1
Log(file_log, "Trivial Move! From L%d to L%d: sst_id = %d", c.level, c.level + 1, c.inputs_[0].front()->sst_id);
#endif
        // Move file to next level
            if (c.level == 0) {
                s = find_l0_block_index_node_list( db_state->db_info->list_of_block_index_node, 
                                                                    db_state->db_info->level0_info, 
                                                                    c.inputs_[0].front()->sst_id, 
                                                                    &c.block_node_list_outputs);
            } else {
                s = find_block_index_node_list( db_state->db_info->list_of_block_index_node, 
                                                                db_state->db_info->level0_info, 
                                                                c.inputs_[0].front()->min_key, 
                                                                c.inputs_[0].front()->max_key, 
                                                                c.level, 
                                                                &c.block_node_list_outputs);
#if 0
for(std::list<block_index_node*>::iterator iter = c.block_node_list_outputs.begin();
iter != c.block_node_list_outputs.end(); iter++) {
Log(file_log, "min_key = %s -- line %d", (*iter)->min_key.i_key.c_str(), __LINE__);
}
#endif
            }
            c.sst_node_list_outputs = c.inputs_[0];
        } else {
            s = do_compaction_work(db_state, &c);
        }
        if(c.is_seek_compaction ) {
            assert(db_state->compaction_info->sst_to_compact_for_seek != NULL);
            delete db_state->compaction_info->sst_to_compact_for_seek;
            db_state->compaction_info->sst_to_compact_for_seek = NULL;
        }

        //Atomically update index data.
        if( s == e_Ok) {
            //Update block index node list
            s = block_index_update(db_state->db_info, &c);
        } else {
            abort();
        }
#if 0
int idx3 = 0;
for(std::list<sst_index_node*>::iterator iter = c.sst_node_list_outputs.begin(); iter != c.sst_node_list_outputs.end(); ++iter, ++idx3) {
    Log(file_log, "outputs: L%d sst_id:%u min_key1#%d %s  max_key1#%d %s", 
    c.level + 1, (*iter)->sst_id,
    idx3, (*iter)->min_key.i_key.c_str(), 
    idx3, (*iter)->max_key.i_key.c_str());
}
#endif        

        if( s == e_Ok) {//Update block index node list
            s = sst_index_update(db_state->db_info, &c);
        } else {
            abort();
        }
        
        if( s == e_Ok) {
            s = level_score_update(db_state);
        } else {
            abort();
        }
LogDBState(file_log, db_state, &c, e_LogCompEnd);
    }
    return s;
}


//GaoLong added 2013/11/26
bool should_drop(bool &is_first_of_input_list,
        std::list<internal_key_value>::iterator &iter,
        slice &current_user_key,
        const Db_state * db_state,
        Compaction * c)
{

    bool should_drop = false;
    parsed_internalkey pkey;
    if( !parse_internalkey(iter->key, &pkey) ) {//先不考虑
        //current_user_key.data = NULL;
        //current_user_key.size = 0;
        is_first_of_input_list = true;
    }
    
#if 0
if ( memcmp(pkey.user_key.data , "0000000000000133", pkey.user_key.size) == 0 ) {
    std::list<internal_key_value>::iterator tmp_iter = iter;
Log(file_log, "Meet 0000000000000133[%c] compact to L%d -- line %d", iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
Log(file_log, "next is %s[%c] compact to L%d -- line %d", (++tmp_iter)->key.i_key.c_str(), tmp_iter->value[0], c->level + 1, __LINE__);
}
#endif
#if 0
if ( memcmp(pkey.user_key.data , "0000000000001681", pkey.user_key.size) == 0 ) {
Log(file_log, "Meet 0000000000001681[%c] compact to L%d -- line %d", iter->value[0], c->level + 1, __LINE__);
}
if ( memcmp(pkey.user_key.data , "0000000000045719", pkey.user_key.size) == 0 ) {
Log(file_log, "Meet 0000000000045719[%c] compact to L%d -- line %d", iter->value[0], c->level + 1, __LINE__);
}
#endif
    
    if (is_first_of_input_list ||
      (slice_userkey_compare(pkey.user_key, current_user_key) != 0)) {
        // First occurrence of this user key
        if (!is_first_of_input_list) {
            free(current_user_key.data);
        }
        current_user_key.size = pkey.user_key.size;
        current_user_key.data= (char *) malloc (current_user_key.size);
        memset(current_user_key.data, 0x00, current_user_key.size);
        memcpy(current_user_key.data, pkey.user_key.data, current_user_key.size);
        is_first_of_input_list = false;
    } else {
        //(A) The following kv other than the first must be dropped
        assert(is_first_of_input_list == false);
        should_drop = true;
    }
    if (pkey.type == e_TypeDeletion &&
             is_base_level_for_key(pkey.user_key, db_state, c)) {
    // For this user key:
    // (1) there is no data in higher levels
    // (2) data in lower levels will have larger sequence numbers
    // (3) data in the two levels that are being compacted here
    //     which follow the first occurrence will be dropped in the next
    //     few iterations of this loop (by rule (A) above).
    // Therefore this deletion marker is obsolete and can be dropped.
        should_drop = true;
    }
    free(pkey.user_key.data);
    return should_drop;
}

//yueyinliang2013/11/14: have got input[0,1]
//GaoLong 2013/11/19: finished
//GaoLong 2013/11/20: modified
//GaoLong 2013/12/2  : modified
status do_compaction_work(Db_state * db_state, Compaction * c)
{
//Log("do_compaction_work() begin, %d", __LINE__);
    pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
    std::list<internal_key_value> input_list = merge_input_to_kvlist(db_state, c);
#ifdef DETAIL_LOG
Log(file_log, "input_list's kv number = %d -- line %d", input_list.size(), __LINE__);	
#endif
    std::list<internal_key_value>::iterator input = input_list.begin();//auto : list<internal_key_value>::iterator

    //parsed_internalkey pkey;
    status s = e_Ok;
    bool judged = false;//If should_drop() be called, then we will not repeat the should_drop() function unnecessarily.
    slice current_user_key;
    bool is_first_of_input_list = true; //Whether the input is the first kv of input_list
    std::string block_buf;
    std::vector<slice> tmp_keys_;
    
    uint32_t num_entries_in_block = 0; //the number of KV entries in block_buf
    uint32_t total_num_entries = 0; //the number of KV entries in block_buf
    internalkey smallest, largest;
    bool is_first_block= true;
    
    for(; input != input_list.end(); ) {
        
        #if 1 //TODO ,compact_memtable will change  *c, so we put it down at first.
        //compact memtable
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
        if (db_state->db_info->ptr_immutable != NULL) {
Log(file_log, "Dump when compaction!");
            Compaction c_dump;
            c_dump.level = -1;
            c_dump.level_dump = -1;
            c_dump.grandparent_index = c_dump.grandparents.begin();//We haven't assigned grandparent, can we set grandparent_index?
            c_dump.seen_key = false;
            c_dump.overlapped_bytes = 0;
            c_dump.is_dump = true;
            s = compact_memtable(db_state, &c_dump);
            if(s != e_Ok) {
            //err_log
                abort();
                //return s;
            }
            pthread_cond_signal(&db_state->db_info->mem_condv);  // Wakeup MakeRoomForWrite() if necessary
        }
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
        #endif
        
        parsed_internalkey pkey;
        if( !parse_internalkey(input->key, &pkey) ) {//先不考虑
            //current_user_key.data = NULL;
            //current_user_key.size = 0;
            is_first_of_input_list = true;
            abort();
        }

        // 1.  if we should stop adding the key to the block; then stop and finish_block_output
        if(should_stop_before(pkey.user_key, c) ) {
Log(file_log, "should_stop_before(%s) == true -- line %d", pkey.user_key.data, __LINE__);
            if(!block_buf.empty()) {
                s = finish_compaction_output_block (db_state, c, block_buf, tmp_keys_,
                                                                    smallest, largest,is_first_block);
                if(s != e_Ok) {
                    abort();
                }
                block_buf.clear();
                for(int i = 0; i < tmp_keys_.size(); i++) {
                    free(tmp_keys_[i].data);
                }
                tmp_keys_.clear();
            }
            num_entries_in_block = 0;
            is_first_block = true;//finish generating a sstable
        }
        free(pkey.user_key.data);
        if (judged ||
            !should_drop(is_first_of_input_list, input, current_user_key, db_state, c)) {
            if  ( ++input == input_list.end() ) {//the last input of the merging_list,we should output the last block behind for-loop
                --input;
                kv_append_to_buf(&(*input), &block_buf);
                ikey_append_to_vector(input->key, &tmp_keys_);
                ++num_entries_in_block;
                ++total_num_entries;
                break;
            } else {
                --input;
            }
            if (num_entries_in_block == 0) {
                smallest = input->key;
                largest = input->key;
            }
            largest = input->key;
            kv_append_to_buf(&(*input), &block_buf);
            ikey_append_to_vector(input->key, &tmp_keys_);
            ++num_entries_in_block;
            ++total_num_entries;

            //get the next valid input internal_key_value
            std::list<internal_key_value>::iterator next_input = input;
            while (++next_input != input_list.end() &&
                    should_drop(is_first_of_input_list, next_input, current_user_key, db_state, c) ) 
                ;//Empty loop body
                
            if ( next_input == input_list.end() ) {
                break;
            }
            size_t next_input_size = next_input->key.i_key.size() + next_input->value.size();

            //iff the block cannot contain the next input KV, we output a new block
            if (block_buf.size() + next_input_size + 2 * sizeof(uint32_t) > MAX_BLOCK_SIZE_BYTE) {
                s = finish_compaction_output_block(db_state, c, block_buf, tmp_keys_,
                                                                        smallest, largest, is_first_block);
                block_buf.clear();
                for(int i = 0; i < tmp_keys_.size(); i++) {
                    free(tmp_keys_[i].data);
                }
                tmp_keys_.clear();
                num_entries_in_block = 0;
                if ( e_Ok != s) {
                    break;
                }
            }
            input = next_input;//skip to the next valid input
            judged = true;
        } else { //If the situation be happen? Answer:yes, the first input kv of the input_list is deletion and base level
            ++input;//next valid input KV
            judged = false;
        }
    }//end for-loop

    //Output the last block
    s = finish_compaction_output_block(db_state, c, block_buf, tmp_keys_,
                                                            smallest, largest, is_first_block);
    free(current_user_key.data);
    pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
Log(file_log, "compacted %d key-values, -- line %d -- status %d", total_num_entries, __LINE__, s);
    return s;
}

//GaoLong add 2013/11/18
//GaoLong modified 2013/11/26: fixed the key compare
bool should_stop_before(slice user_key, Compaction * c)
{
//Log(" should_stop_before -- line %d", __LINE__);	
//Log(" (*c->grandparent_index)->max_key = %s -- line %d", (*c->grandparent_index)->max_key.i_key.c_str(), __LINE__);
	if(c->grandparent_index != c->grandparents.end()) {
            slice s_max_key;
            s_max_key.data = const_cast<char *>((*c->grandparent_index)->max_key.i_key.c_str());
            s_max_key.size = (*c->grandparent_index)->max_key.i_key.size() - 8;

		// Scan to find earliest grandparent file that contains key.
		while (c->grandparent_index != c->grandparents.end() &&
		 slice_userkey_compare(user_key, s_max_key) > 0) {
			if (c->seen_key) {
			  c->overlapped_bytes += (*c->grandparent_index)->size;
			}
			++(c->grandparent_index);
//Log("++(c->grandparent_index");
		}
		c->seen_key = true;
	}
    if(c->overlapped_bytes > MAX_GRANDPARENT_OVERLAP_BYTES) {
        // Too much overlap for current output; start new output
        c->overlapped_bytes = 0;
        return true;
    } else {
        return false;
    }
}

//GaoLong finished 2013/11/19
//Should be changed
//propose1: connect all the list of each level , the hash just provide the index to search quickly
//propose2: add a function in block.cpp --to fulfil find whether the user_key is in range of blocks instead of find the user_key
//or judge the range according to the sst_index_node list
bool is_base_level_for_key(const slice user_key, const Db_state * db_state, Compaction * c)
{
    for (int lvl = c->level + 2; lvl < MAX_LEVEL; lvl++) {
#if 0
        const std::list<block_index_node>* p_block_list = state->list_of_block_index_node[lvl];
        for (; state->compaction->level_ptrs_[lvl] != p_block_list.end(); ) {
            block_index_node* p_block_node = *(state->compaction->level_ptrs_[lvl]);
            parsed_internalkey parsed_key_max;
            parsed_internalkey parsed_key_min;
            parse_internalkey(p_block_node.max_key, &parsed_key_max);
            parse_internalkey(p_block_node.min_key, &parsed_key_min);
            if (slice_userkey_compare(user_key, parsed_key_max.user_key) <= 0) {
                // We've advanced far enough
                if (slice_userkey_compare(user_key, parsed_key_min.user_key) >= 0) {
                      // Key falls in this file's range, so definitely not base level
                      return false;
                  }
                  break;
              }
              ++(state->compaction->level_ptrs_[lvl]);
              if( *(state->compaction->level_ptrs_[lvl]) )//not finish, a little complex

            }
#else
        const std::list<sst_index_node> sst_list = db_state->db_info->list_of_sst_index_node[lvl];
        for (;c->level_ptrs_[lvl] != sst_list.end(); ) {
            sst_index_node sst_node = *(c->level_ptrs_[lvl]);

            slice s_min_key;
            s_min_key.data = const_cast<char *>(sst_node.min_key.i_key.c_str());
            s_min_key.size = sst_node.min_key.i_key.size() - 8;
            slice s_max_key;
            s_max_key.data = const_cast<char *>(sst_node.max_key.i_key.c_str());
            s_max_key.size = sst_node.max_key.i_key.size() - 8;
            
            if (slice_userkey_compare(user_key, s_max_key) <= 0) {
                // We've advanced far enough
                if (slice_userkey_compare(user_key, s_min_key) >= 0) {
                      // Key falls in this file's range, so definitely not base level
                      return false;
                }
                break;
            }
              ++(c->level_ptrs_[lvl]);
          }
#endif
        }
        return true;
}

/* make a merging list through c->inputs_[0,1]:
 * read the block according to the key range of c->inputs_[0,1]
 * sorted by:1. user_key, 2.tag(64bits)
 *
input: input[0,1]: sstable_index_node
step1: get block index node from [minkey, maxkey] of sstable_index_node, and build a list <block_index_node>
step2: parse_block2kv(), and call list::push_back() put the key/value pairs to a list <kv>
step3: sort
 */
//GaoLong finished 2013/11/21 15:16:35
std::list<internal_key_value> merge_input_to_kvlist(Db_state * db_state, Compaction * c)
{
    std::list<internal_key_value> i_kv_list;
    std::list<block_index_node*> block_list;

    //s1:get block index node from sstable_index_node, and build a list <block_index_node>

    //s1.1: deal with level
    for(std::list<sst_index_node*>::iterator iter = c->inputs_[0].begin(); 
            iter != c->inputs_[0].end(); ++iter) {
        if(c->level == 0) {
            if( find_l0_block_index_node_list(db_state->db_info->list_of_block_index_node,
                                                            db_state->db_info->level0_info,
                                                            (*iter)->sst_id,
                                                            &block_list)  != e_Ok ) { 
                    //error_log
                    abort();
                }
            } else {
                if (find_block_index_node_list(db_state->db_info->list_of_block_index_node, 
                                                            db_state->db_info->level0_info, 
                                                            (*iter)->min_key, 
                                                            (*iter)->max_key, 
                                                            c->level, 
                                                            &block_list)  != e_Ok ) {
                    //error_log
                    abort();
                }
            }
        }
    //s1.2: deal with level+1
    for(std::list<sst_index_node*>::iterator iter = c->inputs_[1].begin(); 
            iter != c->inputs_[1].end(); ++iter) {
        if( find_block_index_node_list(db_state->db_info->list_of_block_index_node, 
                                                        db_state->db_info->level0_info, 
                                                        (*iter)->min_key, 
                                                        (*iter)->max_key, 
                                                        c->level + 1, 
                                                        &block_list) != e_Ok) { //Provide a interface to find block_nodes according to the sst_id(no) or key_range(ok)?
            //error_log
            abort();
        }
    }

    //s2:parse_block2kv(), and call list::push_back() put the key/value pairs to a list <kv>
    for(std::list<block_index_node*>::iterator iter = block_list.begin(); 
            iter != block_list.end(); ++iter) {
        if( parse_block2kv(db_state, *iter, &i_kv_list ) != e_Ok ) {
            //error_log
            abort();
        }
    }
    //sort the i_kv_list
    //using function: ikv_compare
    //approximately NlogN where N is the i_kv_list number.
    i_kv_list.sort(ikv_compare);
    return i_kv_list;
}

//GaoLong modified 2013/11/21
status compact_memtable(Db_state * db_state, Compaction * c)
{
    //Assert the mem_mutex is held.
LogDBState(file_log, db_state, c, e_LogDumpBegin);
    status s = e_Ok;
    assert(db_state->db_info->ptr_immutable != NULL);
    s = write_level0_sst(db_state, db_state->db_info->ptr_immutable, c);//GL:Make sure c->largest[0].i_key is updated.
//Log("%d, below write_level0_sst -- block_node_list_outputs.size() = %d\n", __LINE__, c->block_node_list_outputs.size());
    pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
    if (s == e_Ok) {
        //Update compact_pointer[c->level] ?? Need dump to update the compact_pointer?
        //db_state->compaction_info->compact_pointer_[c->level] = c->largest[0].i_key;

        c->level_dump = pick_level_for_memtable_output(db_state, 
                                                            c->smallest[0], c->largest[0]);
//Log("pick_level_for_memtable_output = %d \n", c->level_dump);
        //Update block index node list
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->index_mutex) );
        s = block_index_update(db_state->db_info, c);
    } else {
        abort();
    }
    if ( s == e_Ok ) {
        //Update sstable index node list
        s = sst_index_update(db_state->db_info, c);
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->index_mutex) );
    } else {
        abort();
    }
        
    if ( s== e_Ok) {
        s = level_score_update(db_state);
    } else {
        abort();
    }
        
    if ( s == e_Ok) {
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
        db_state->db_info->ptr_immutable = NULL;
    } else {
        abort();
    }

LogDBState(file_log, db_state, c, e_LogDumpEnd);
    return s;
}

//GaoLong finished 2013/11/20
status block_index_update(Db_info * db_info, Compaction * c) // the args
{
//Log("block_index_update() begin, %d", __LINE__);
    //Assert index_mutex is held!
    status s = e_Corruption;
    uint32_t level_to;
    std::list<block_index_node*> outputs = c->block_node_list_outputs;
#if 0
	fprintf(stdout, "block outputs number: %d \n", c->block_node_list_outputs.size());
#endif
    
    if( c->is_dump ) {//Iff dump
        level_to = c->level_dump;
    } else {
        level_to = c->level + 1;
    }

     if ( !c->is_dump && !is_trivial_move(c) && !c->inputs_[1].empty()) {
        assert(level_to > 0);
        s = delete_block_index_node(db_info->list_of_block_index_node, 
                                                    level_to, c->smallest[1], c->largest[1]);
        if ( s != e_Ok) {
            abort();
        }
    }
     
    for(std::list<block_index_node*>::iterator iter = outputs.begin(); iter != outputs.end(); ++iter) {
    //insert the new block index node to global list for compaction output
        assert(level_to >= 0);
        s = insert_block_index_node(db_info->list_of_block_index_node, 
                                                    &(db_info->level0_info), 
                                                    level_to, *iter);
        if(!is_trivial_move(c)) {
            free(*iter);
        }
        if( s != e_Ok ) {
            //TODO:err_log
            abort();
        }
    }

    if( !c->is_dump ) {
        if (c->level == 0) {
            s = delete_l0_block_index_node(db_info->list_of_block_index_node, 
                                                        &db_info->level0_info, 
                                                        c->inputs_[0]);
            if ( s != e_Ok) {
                abort();
            }
        } else {
            s = delete_block_index_node(db_info->list_of_block_index_node, 
                                                        c->level, c->smallest[0], c->largest[0]);
            if ( s != e_Ok) {
                abort();
            }            
        }
    }
    return s;
}

//GaoLong finished 2013/11/20
status sst_index_update(Db_info * db_info, Compaction * c)
{
//Log("sst_index_update() begin, %d", __LINE__);
    //Assert index_mutex is held!
    status s = e_Ok;
    int32_t level_to;
    std::list<sst_index_node*> outputs = c->sst_node_list_outputs;

    if( c->is_dump ) {//Iff dump.
        level_to = c->level_dump;
    } else { //Iff compact other level except for level0.
        level_to = c->level+1;
    }

    if( !c->is_dump && !is_trivial_move(c) && !c->inputs_[1].empty()) {
        s= delete_sst_index_node_list(db_info->list_of_sst_index_node, 
                                        c->level+1, c->smallest[1], c->largest[1]);
        if ( s != e_Ok) {
            abort();
        }        
    }

    for( std::list<sst_index_node*>::iterator iter = outputs.begin(); iter != outputs.end(); ++iter) {
    //insert the new sst index node to global list for compaction output
        assert(level_to >= 0);
        s = insert_sst_index_node_list(db_info->list_of_sst_index_node, 
                                                level_to, *iter);
        if(!is_trivial_move(c)) {
            free(*iter);
        }
        if(s != e_Ok) {
            abort();
        }
    }
    
    if( !c->is_dump ) {
        if(c->level == 0) {
            s = delete_l0_sst_index_node_list(db_info->list_of_sst_index_node, c->inputs_[0]);
            if ( s != e_Ok) {
                abort();
            }            
        } else {
            s = delete_sst_index_node_list(db_info->list_of_sst_index_node, 
                                                    c->level, c->smallest[0], c->largest[0]);
            if ( s != e_Ok) {
                abort();
            }            
        }
    }
    return s;
}

//@zxx 2013/11/29
//true if the smallest_user_key is larger than the largest key in sst_node
//this means the range is completely after the sstable
bool after_file(const slice smallest_user_key, const sst_index_node* p_sst_node) {
    //assert(smallest_user_key != NULL);??

    slice s_max_key;
    s_max_key.data = const_cast<char *>(p_sst_node->max_key.i_key.c_str());
    s_max_key.size = p_sst_node->max_key.i_key.size() - 8;
    
    //const slice node_max_user_key = parsed_node_maxkey.user_key;
    
    if(slice_userkey_compare(smallest_user_key, s_max_key) > 0) {
        return true;
    } else {
        return false;
    }
}

//@@zxx 2013/11/29
//true if the largest_user_key is smaller than the smallest key in sst_node
//this means the range is completely before the sstable
bool before_file(const slice largest_user_key, const sst_index_node* p_sst_node) {
    //assert(largest_user_key != NULL);??
    slice s_min_key;
    s_min_key.data = const_cast<char *>(p_sst_node->min_key.i_key.c_str());
    s_min_key.size = p_sst_node->min_key.i_key.size() - 8;
    //const slice node_min_user_key = parsed_node_minkey.user_key;
    
    if(slice_userkey_compare(largest_user_key, s_min_key) < 0) {
        return true;
    } else {
        return false;
    }
}




//@zxx 2013/11/29
//@return value: true: has overlap;  false: no overlap
bool some_file_overlaps_range(bool disjoint_sorted_files, const std::list<sst_index_node> sst_file_list, const slice smallest_user_key, const slice largest_user_key) {
        
        //since it is a list, can not use binary search for level1-N, so use the same way with level0
        
        //need to check against all files
        std::list<sst_index_node>::const_iterator iter = sst_file_list.begin();
        for(; iter != sst_file_list.end(); iter++) {
            const sst_index_node* p_sst_node = &(*iter);
            if(after_file(smallest_user_key, p_sst_node) || before_file(largest_user_key, p_sst_node)) {
                //no overlap
            }
            else {
                return true;
            }
        }
        return false;
}

//@zxx 2013/11/29
bool overlap_in_level(Db_state* state, int32_t level, const slice smallest_user_key, const slice largest_user_key) {
    return some_file_overlaps_range((level > 0), state->db_info->list_of_sst_index_node[level], smallest_user_key, largest_user_key);
}

//@zxx 2013/11/30
uint32_t pick_level_for_memtable_output(Db_state* state, internalkey smallest_internalkey, internalkey largest_internalkey) {
    int32_t level = 0;
    
    slice s_smallest_key;
    s_smallest_key.data = const_cast<char *>(smallest_internalkey.i_key.c_str());
    s_smallest_key.size = smallest_internalkey.i_key.size() - 8;
    slice s_largest_key;
    s_largest_key.data = const_cast<char *>(largest_internalkey.i_key.c_str());
    s_largest_key.size = largest_internalkey.i_key.size() - 8;
    //space of data is in parsed_smallest_internalkey&parsed_largest_internalkey.
    //if true, no overlap in level 0, then check the overlap in level 1 and level 2 
    if(!overlap_in_level(state, 0, s_smallest_key, s_largest_key)) {
        internalkey start, limit;
        userkey_to_internalkey(s_smallest_key, e_TypeValue, ~0ull, &start );//1 << 64, &start);
        userkey_to_internalkey(s_largest_key, e_TypeValue, ~0ull, &limit);//1 << 64, &limit);
        std::list<sst_index_node*> overlaps;

        while(level < KMAXCOMPACTLEVEL) {
            if(overlap_in_level(state, level+1, s_smallest_key, s_largest_key)) {
                break;
            }
            if(level+2 < MAX_LEVEL) {
                // check that file does not overlap too many grandparent bytes.
                get_overlapping_inputs(level + 2, &start, &limit, &overlaps, state->db_info);
                uint64_t sum = total_sst_size(overlaps);
                if(sum > MAX_GRANDPARENT_OVERLAP_BYTES) {
                    break;
                }
            }
            level++;
        }   
    }
    return level;
}

/*
write_key_value_to_block： 
a)  kv_append_to_buf(String *buf, &internal_kv)
b)  logical_write(blockid, buf.data(), buf.size());
*/

status
write_level0_sst (Db_state* db_state, Memtable * memtable, Compaction * c) 
{
//Log("write_level0_sst() begin, %d", __LINE__);
    //0. assert the mutex is held NO-DONE
    status s = e_Ok;

    //block_buf: key_size |value_size | user_key-sequence:type | value
    std::string block_buf;
    std::vector<slice> tmp_keys_;
    bool is_first_block= true;
    uint32_t num_entries_in_block = 0;  //the number of KV entries in block_buf
    uint32_t num_entries = 0;               // all dumped entries
    internalkey min_key, max_key;
    
//Log("Level-0 table: started : ");    
    
    // 1. get kv from skiplist and insert into block_buf
    ptr_skiplist_node node = memtable->skiplist->head->forward[0];
    slice_to_internalkey(node->key, &c->smallest[0]); 
    for(; node != memtable->skiplist->nil; node = node->forward[0]) {
        char a_size[4];
        memset(a_size, 0x00, sizeof(a_size));
        encode_fixed32(a_size, node->key.size);
        block_buf.append(a_size, sizeof(a_size));
        memset(a_size, 0x00, sizeof(a_size));
        encode_fixed32(a_size, node->value.size);
        block_buf.append(a_size, sizeof(a_size));

        char* a_key_value = (char *) malloc (node->key.size + node->value.size + 1);//free below
        memset (a_key_value, 0x00, sizeof(a_key_value));
        memcpy(a_key_value, node->key.data, node->key.size);
        memcpy(a_key_value + node->key.size, node->value.data, node->value.size);

        block_buf.append(a_key_value, node->key.size + node->value.size);
        free(a_key_value);

        //bush_back user_key to vector!
        slice s_user_key;
        s_user_key.size = node->key.size - 8; // internalkey's data size - sequence_type's size
        s_user_key.data = (char*) malloc (s_user_key.size);//Free below:when vector is useless
        memset(s_user_key.data, 0x00, s_user_key.size);
        memcpy(s_user_key.data, node->key.data, s_user_key.size);
        tmp_keys_.push_back(s_user_key);
#if 1
if( memcmp(s_user_key.data, "0000000000001681", s_user_key.size) == 0 ) {
Log(file_log, "0000000000001681[%c] dump to level %d -- line %d", node->value.data[0], c->level_dump, __LINE__);
}

if( memcmp(s_user_key.data, "0000000000045719", s_user_key.size) == 0 ) {
Log(file_log, "0000000000045719[%c] dump to level %d -- line %d", node->value.data[0], c->level_dump, __LINE__);
}
#endif

        if(num_entries_in_block == 0) {
            slice_to_internalkey(node->key, &min_key);
            slice_to_internalkey(node->key, &max_key);//For case: only one key in memtable!
        }
        slice_to_internalkey(node->key, &max_key);
        ++num_entries_in_block;
        ++num_entries;

        if(node->forward[0] != memtable->skiplist->nil) { //Not the last kv
            uint32_t next_kv_size = node->forward[0]->key.size + node->forward[0]->value.size; 
            if( block_buf.size() + next_kv_size + 2*sizeof(uint32_t) > MAX_BLOCK_SIZE_BYTE) {
                s = finish_compaction_output_block(db_state, c, block_buf, tmp_keys_,
                                                            min_key, max_key, is_first_block);
//Log("Level-0 block : %lld enteries %lld bytes - %d", (uint64_t) num_entries_in_block, block_buf.size(), s);
                block_buf.clear();
                for(int i = 0; i < tmp_keys_.size(); i++) {
                    free(tmp_keys_[i].data);
                }
                tmp_keys_.clear();
                num_entries_in_block = 0;
            }
        } else { //The next KV will be the nil..
            s = finish_compaction_output_block(db_state, c, block_buf, tmp_keys_,
                                                            min_key, max_key, is_first_block);
            if (s == e_Ok) {
//Log("Level-0 block : %lld enteries %lld bytes - %d", (uint64_t) num_entries_in_block, block_buf.size(), s);
                block_buf.clear();                
                for(int i = 0; i < tmp_keys_.size(); i++) {
                    free(tmp_keys_[i].data);
                }
                tmp_keys_.clear();
                slice_to_internalkey(node->key, &c->largest[0]);
            }
            break;
        }
    }

//Log("%d, c->block_node_list_outputs.size() = %d\n", __LINE__, c->block_node_list_outputs.size());
#ifdef DETAIL_LOG
Log(file_log, "dumped %d key-values -- line:%d -- status %d", num_entries, __LINE__, s);
#endif
    return s;
}

bool is_trivial_move(Compaction * c) 
{
    return (c->inputs_[0].size() == 1 && 
                c->inputs_[1].size() == 0 &&
                total_sst_size(c->grandparents) < MAX_GRANDPARENT_OVERLAP_BYTES);
}


//GaoLong modified 2013/11/21
//pick the sstable and insert the sst_index_node to inputs_[0,1]
status pick_compaction(Db_state * db_state, Compaction * c)
{
//Log("pick_compaction() begin, %d", __LINE__);
    int level;
    status s = e_Ok;
    //Compaction * c = (Compaction *)malloc( sizeof(Compaction) ); //free outside : when compaction is end. 
    const bool score_compaction = (db_state->compaction_info->compaction_score >= 1);
    const bool seek_compaction = (db_state->compaction_info->sst_to_compact_for_seek != NULL); //TODO: GAOLONG
    if (score_compaction) {
        level = db_state->compaction_info->compaction_level;
        c->level = level;
        assert(level >= 0);
        assert(level + 1 < MAX_LEVEL);
        
        for( std::list<sst_index_node>::iterator iter = db_state->db_info->list_of_sst_index_node[level].begin();
                iter != db_state->db_info->list_of_sst_index_node[level].end();
                ++iter) {
            sst_index_node* sst = &(*iter);
            if ( strlen(db_state->compaction_info->compact_pointer_[level]) == 0 ||                 //is correct ? for the first byte possible be zero?
                    sst->max_key.i_key.compare(0, sst->max_key.i_key.size(),
                                                                db_state->compaction_info->compact_pointer_[level],
                                                                MAX_USERKEY_SIZE_BYTE + 8) > 0 ) {//Be care of the size of max_key.i_key and compact_pointer_[lvl]
                assert(sst->max_key.i_key.size() == MAX_USERKEY_SIZE_BYTE + 8);
                c->inputs_[0].push_back(sst);
                break;
            }
        }
        if (c->inputs_[0].empty()) {
            c->inputs_[0].push_back( &(db_state->db_info->list_of_sst_index_node[level].front()) );
        }
        //assert inputs_[0]: front() == back()
        assert(c->inputs_[0].front()->sst_id == c->inputs_[0].back()->sst_id);
        c->smallest[0] = c->inputs_[0].front()->min_key;
        c->largest[0] = c->inputs_[0].front()->max_key;
    }
    else if (seek_compaction) {
        level = db_state->compaction_info->sst_to_compact_for_seek_level;
        c->level = level;
        c->inputs_[0].push_back( db_state->compaction_info->sst_to_compact_for_seek );
        c->smallest[0] = c->inputs_[0].front()->min_key;
        c->largest[0] = c->inputs_[0].front()->max_key;
        c->is_seek_compaction = true;
    } else {
        return e_NotSupported;
    }

    // pick up all overlapping sstables in level0 according to the inputs_[0]
    if (level == 0) {
        internalkey smallest, largest;
        get_range_of_input(c->inputs_[0], &smallest, &largest);
        // Note that the next call will discard the file we placed in
        // c->inputs_[0] earlier and replace it with an overlapping set
        // which will include the picked file.
        get_overlapping_inputs(0, &smallest, &largest, &c->inputs_[0], db_state->db_info);
        c->smallest[0] = c->inputs_[0].front()->min_key;
        c->largest[0] = c->inputs_[0].back()->max_key;
        assert(!c->inputs_[0].empty());
    }
    select_other_inputs(db_state, c);
//Log("pick_compaction() end , %d -- status %d", __LINE__, s);
    return s;
}

//GaoLong 2013/11/18: finished
//Functions:
// 1 .Select other sstable for compaction inputs:
// 2. Setup smallest[0,1] and largest[0,1] internalkey
// 3. Setup grandparents and grandparent_index
// 4. Setup compact_pointer_[level] to largest[0]
//TODO: where to setup the smallest[1] and largest[1] --two choice below
// 1:do_compaction_work() 
// 2:here in get_overlapping_inputs() -- add two args to get the range [tick]
void select_other_inputs(Db_state * db_state, Compaction * c)
{
//Log("select_other_inputs() begin, %d", __LINE__);
    const int level = c->level;
    internalkey smallest, largest;
    get_range_of_input(c->inputs_[0], &smallest, &largest);

    get_overlapping_inputs(level + 1, &smallest, &largest, &c->inputs_[1], db_state->db_info);

    // Get entire range of inputs_[0,1] that covered by compaction
    internalkey all_start, all_limit;
    get_range_of_two_inputs(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);    //Gaolong 2013/11/26 added

    //Expend the inputs_[0] that overlap the largest
    if (!c->inputs_[1].empty()) {
        get_range_of_input(c->inputs_[1], &(c->smallest[1]), &(c->largest[1]));//Setup smallest[1], largest[1] of struct Compaction.
        std::list<sst_index_node*> expanded0;
        get_overlapping_inputs(level, &all_start, &all_limit, &expanded0, db_state->db_info);
        //const uint64_t inputs0_size = total_sst_size(c->inputs_[0]);
        const uint64_t inputs1_size = total_sst_size(c->inputs_[1]);
        const uint64_t expanded0_size = total_sst_size(expanded0);
        if (expanded0.size() > c->inputs_[0].size() &&                                                         //if we expanded at least one sstable to list inputs_[0]
            inputs1_size + expanded0_size < EXPANDED_COMPACTION_BYTE_SIZE_LIMIT) { //and didn't exceed the limit in size.
            internalkey new_start, new_limit;//for testing if the [new_start, new_limit] will overlaping new sstable in layer level+1
            get_range_of_input(expanded0, &new_start, &new_limit);
            std::list<sst_index_node*> expanded1;
            get_overlapping_inputs(level+1, &new_start, &new_limit, &expanded1, db_state->db_info);
            if (expanded1.size() == c->inputs_[1].size()) {
                //[GL]如果进过expanded0扩展后，不会添加与level+1层有overlap的sstalbe，则执行扩展。
                //log if necessary
                c->smallest[0] = new_start;
                c->largest[0] = new_limit;
                c->inputs_[0] = expanded0;
                c->inputs_[1] = expanded1;
//Log("c->smallest[0]  = %s, c->largest[0] = %s", c->smallest[0].i_key.c_str(), c->largest[0].i_key.c_str());
                get_range_of_two_inputs(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
            }
        }
    }
    if ( level + 2 <  MAX_LEVEL ) {
        get_overlapping_inputs(level + 2,  &all_start, &all_limit, &c->grandparents, db_state->db_info);
        c->grandparent_index = c->grandparents.begin();
//Log("get_overlapping_inputs() end, c->grandparents' number = %d . line %d", c->grandparents.size(), __LINE__);
//Log("get_overlapping_inputs() end,  %d . line %d", c->grandparents.size(), __LINE__);
    }
    // Set comact_pointer_ :
    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
#ifdef DETAIL_LOG
Log(file_log, "update compact_pointer_ = %s , -- line %d", db_state->compaction_info->compact_pointer_[level], __LINE__);
#endif
    memcpy(db_state->compaction_info->compact_pointer_[level], const_cast<char *> (c->largest[0].i_key.data()) , MAX_USERKEY_SIZE_BYTE + 8);
//Log("select_other_inputs() end, %d", __LINE__);
    return;
}

void get_overlapping_inputs(
    const int32_t level,
    const internalkey *begin,
    const internalkey *end,
    std::list<sst_index_node*> *inputs,
    const Db_info * db_info)
{
    inputs->clear();
    //slice slice_user_begin, slice_user_end;
    parsed_internalkey parsed_begin;
    parsed_internalkey parsed_end;

    if (begin != NULL) {
        parse_internalkey(*begin, &parsed_begin);
        //slice_user_begin = parsed_begin.user_key;
    }
    if (end != NULL) {
        parse_internalkey(*end, &parsed_end);
        //slice_user_end = parsed_end.user_key;
    }
//Log("db_info->list_of_sst_index_node[%d].size() = %d -- line: %d", level,  db_info->list_of_sst_index_node[level].size(), __LINE__);   
    for (std::list<sst_index_node>::iterator iter = db_info->list_of_sst_index_node[level].begin();
          iter != db_info->list_of_sst_index_node[level].end(); ) {
        sst_index_node* psst= &*iter++;
        //parse_internalkey(internal_sst_min_key, &parsed_sst_min_key);
        //parse_internalkey(internal_sst_max_key, &parsed_sst_max_key);
        slice slice_sst_min_key, slice_sst_max_key;
        slice_sst_min_key.data = const_cast<char *>(psst->min_key.i_key.c_str());
        slice_sst_min_key.size = psst->min_key.i_key.size() - 8;
        slice_sst_max_key.data = const_cast<char *>(psst->max_key.i_key.c_str());
        slice_sst_max_key.size = psst->max_key.i_key.size() - 8;
        if (begin != NULL && slice_userkey_compare(slice_sst_max_key, parsed_begin.user_key) < 0) {
            // "f" is completely before specified range; skip it
        } else if (end != NULL && slice_userkey_compare(slice_sst_min_key, parsed_end.user_key) > 0) {
            // "f" is completely after specified range; skip it
        } else {//[GL] find the overlaping file , then insert into list
            inputs->push_back(psst);
            if (level == 0) {
            // Level-0 files may overlap each other.  So check if the newly
            // added file has expanded the range.  If so, restart search.
                if (begin != NULL && slice_userkey_compare(slice_sst_min_key, parsed_begin.user_key) < 0) {
                    free(parsed_begin.user_key.data);
                    parsed_begin.user_key.size = slice_sst_min_key.size;
                    parsed_begin.user_key.data = (char *) malloc (slice_sst_min_key.size);
                    memcpy(parsed_begin.user_key.data, slice_sst_min_key.data, parsed_begin.user_key.size);
                    //slice_user_begin = slice_sst_min_key;
                    inputs->clear();
                    iter = db_info->list_of_sst_index_node[level].begin();
                } else if (end != NULL && slice_userkey_compare(slice_sst_max_key, parsed_end.user_key) > 0) {
                    free(parsed_end.user_key.data);
                    parsed_end.user_key.size = slice_sst_max_key.size;
                    parsed_end.user_key.data = (char *) malloc (parsed_end.user_key.size);
                    memcpy(parsed_end.user_key.data, slice_sst_max_key.data, parsed_end.user_key.size);
                    //slice_user_end = slice_sst_max_key;
                    inputs->clear();
                    iter = db_info->list_of_sst_index_node[level].begin();
                }//TODO: gaolong
            }
        }
    }
    free(parsed_begin.user_key.data);
    free(parsed_end.user_key.data);
    return;
}

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
        bool &is_first_block)
{

//Log("finish_compaction_output_block() begin, %d", __LINE__);
    status s = e_Ok;
    uint64_t block_id;
    //Physical_addr* phy_addr;
    //We should make sure first :Whether the remaining sstable size can hold the current block size.
    if( c->sst_node_list_outputs.empty() || 
        (c->sst_node_list_outputs.back()->size + block_buf.size() > MAX_SST_SIZE_BYTE) || 
        (is_first_block == true) ) {
    //it is the last block of the sstable
        is_first_block = true;
    } else {
        is_first_block = false;
    }
    if(is_first_block) {
    //iff: the first block in the sstable, we should allocate a new logical space to store the new sstable
        uint64_t new_sst_id;
        sst_index_node new_sst_node;
        block_index_node new_block_node;

        sst_index_node* p_new_sst_node = (sst_index_node*) malloc (sizeof(new_sst_node));
        memcpy(p_new_sst_node, &new_sst_node, (sizeof(new_sst_node)) );

        block_index_node* p_new_block_node = (block_index_node*) malloc (sizeof(new_block_node));
        memcpy(p_new_block_node, &new_block_node, (sizeof(new_block_node)) );

        if( (s = alloc_sst(db_state->storage_info, &new_sst_id)) == e_Ok ) {
            p_new_sst_node->sst_id = new_sst_id; //sstable's offset in disk
            p_new_sst_node->size = 0;
            p_new_sst_node->min_key = min_key;
            p_new_sst_node->max_key = max_key;
            //extended parts
            p_new_sst_node->allowed_seeks = MAX_ALLOWED_SEEKS;     //for needs_compaction
        } else {
            //err_log
            fprintf(stdout, "%d alloc_block error : %d\n", __LINE__ , s);
            abort();
        }

        //get block_id according to new_sst_id
        block_id = get_blockid(new_sst_id, p_new_sst_node->size);
        if( ( s = alloc_block(db_state->storage_info, block_id, block_buf.size()) ) == e_Ok ) {
            p_new_block_node->block_id = block_id;
            p_new_block_node->size = block_buf.size();
            p_new_block_node->max_key = max_key;
            p_new_block_node->min_key = min_key;
            p_new_block_node->bloom_filter.clear();
            p_new_block_node->kv_amount= keys_.size();
            generate_filter(keys_, keys_.size(), BITS_PER_KEY, &p_new_block_node->bloom_filter);
        } else {
            //err_log
            fprintf(stdout, "%d alloc_block error : %d\n", __LINE__ , s);
            abort();
        }
        p_new_sst_node->size += MAX_BLOCK_SIZE_BYTE;
        c->sst_node_list_outputs.push_back(p_new_sst_node);
        c->block_node_list_outputs.push_back(p_new_block_node);
        is_first_block = false;
    } else {
    //iff not the first block in the sstable
        uint64_t last_sst_id = c->sst_node_list_outputs.back()->sst_id;
        block_index_node new_block_node;
        block_index_node* p_new_block_node = (block_index_node*) malloc (sizeof(new_block_node));
        memcpy(p_new_block_node, &new_block_node, (sizeof(new_block_node)) );
        //Get block_id according the last_sst_id
        block_id = get_blockid(last_sst_id, c->sst_node_list_outputs.back()->size);
        if( (s = alloc_block(db_state->storage_info, block_id, block_buf.size()) ) == e_Ok ) {
            p_new_block_node->block_id = block_id;
            p_new_block_node->size = block_buf.size();
            p_new_block_node->max_key = max_key;
            p_new_block_node->min_key = min_key;
            p_new_block_node->bloom_filter.clear();
            p_new_block_node->kv_amount= keys_.size();
            generate_filter(keys_, keys_.size(), BITS_PER_KEY, &p_new_block_node->bloom_filter);
            c->block_node_list_outputs.push_back(p_new_block_node);
            c->sst_node_list_outputs.back()->size += MAX_BLOCK_SIZE_BYTE;
            c->sst_node_list_outputs.back()->max_key.i_key = max_key.i_key;
        } else {
			//err_log
			fprintf(stdout, "%d alloc_block error \n", __LINE__, s);
			abort();
		}
    }
    //s2: flush :  block_buf to ssd..[phy_addr]
    #if 1 //TODO: a hole in a file are filled with '\0'
    char tmp_buf[MAX_BLOCK_SIZE_BYTE + 1];
    memset(tmp_buf, 0x00, sizeof(tmp_buf));
    memcpy(tmp_buf, block_buf.data(), block_buf.size());
    assert(block_buf.size() != 0);
    assert( block_buf[0] != '\0' );
    assert( block_id == c->block_node_list_outputs.back()->block_id );
    s = logical_write(db_state->storage_info, block_id, tmp_buf, MAX_BLOCK_SIZE_BYTE);
//Log("logical_write: sst_id[%d]  block_id[%d]", c->sst_node_list_outputs.back()->sst_id, c->block_node_list_outputs.back()->block_id);    
//Log("offset = %d  -- line %d", db_state->storage_info->map_l2p->find(block_id)->second.offset, __LINE__);    
    #else
    
    s = logical_write(db_state->storage_info, block_id, block_buf.data(), block_buf.size());
    #endif
    if ( s != e_Ok ) {
        abort();
    }
#if 1
char tmp[MAX_BLOCK_SIZE_BYTE + 1];
memset(tmp, 0x00, sizeof(tmp) );
s = logical_read(db_state->storage_info, block_id, tmp, MAX_BLOCK_SIZE_BYTE);
if ( s != e_Ok ) {
    abort();
}
if( memcmp(block_buf.data(), tmp, block_buf.size()) != 0) {
    fprintf(stdout, "offset = %d \n", db_state->storage_info->map_l2p->find(block_id)->second.offset);
}
#endif    

    //s3: TODO
    //uint32_t level = c->level;
    //printf the Log massage.

    //iff everything is ok, then return the status
//Log("finish_compaction_output_block() end, %d -- status %d", __LINE__, s);
    return s;
}

//GaoLong finished 2013/12/2
//Function:
//Update the compaction_score and compaction_level:
//Alg: 
//  Calculate the max score and corresponding level;
//  then assign these to compaction_score and compaction_level.
//Called in the last stage of compaction, after updating the sstable and block list : 
//called by 1. compact_memtable(), 2. background_compaction().
status level_score_update(Db_state * db_state)
{
      // Precomputed best level for next compaction
    int best_level = -1;
    double best_score = -1;

    for (int level = 0; level < MAX_LEVEL; level++) {
        double score;
        if (level == 0) {
          // We treat level-0 specially by bounding the number of files
          // instead of number of bytes for two reasons:
          //
          // (1) With larger write-buffer sizes, it is nice not to do too
          // many level-0 compactions.
          //
          // (2) The files in level-0 are merged on every read and
          // therefore we wish to avoid too many files when the individual
          // file size is small (perhaps because of a small write-buffer
          // setting, or very high compression ratios, or lots of
          // overwrites/deletions).
          score = db_state->db_info->list_of_sst_index_node[level].size() /
              static_cast<double>(LO_COMPACTION_TRIGGER);
        } else {
          // Compute the ratio of current size to size limit.
          const uint64_t level_bytes = total_sst_size2(db_state->db_info->list_of_sst_index_node[level]);
          score = static_cast<double>(level_bytes) / max_bytes_for_level(level);
        }
    
        if (score > best_score) {
            best_level = level;
            best_score = score;
        }
    }
    
    db_state->compaction_info->compaction_level = best_level;
    db_state->compaction_info->compaction_score = best_score;
    return e_Ok;
}



//GaoLong finished 2013/11/29
//Five conditions(Priority decrease) as follows:
// (1). Level0 sstable number >= LO_COMPACTION_TRIGGER; then delay for some seconds
// (2). memtable size + cur_key_value <= MAX_MEMTABLE_SIZE_BYTE ; then break;
// (3). immutable != NULL && memtable size + cur_key_value > MAX_MEMTABLE_SIZE_BYTE, then wait for immutable dump
// (4). Level0 sstable number >= L0_STOP_WRITES_TRIGGER; then wait for compaction
// (5). else switch to a new memtable and trigger compaction of old
status make_room_for_put(Db_state* db_state, size_t kv_size)
{
//Log("make_room_for_put() begin, %d", __LINE__);
    //Assert mem_mutex is held.
    status s = e_Ok;//No used?
    bool allow_delay = true;    //Whether we should allow delay when level0 sstable number exceed LO_COMPACTION_TRIGGER.
    bool force = false;     //Whether we should force the memtable switch to an immutable and then compact the immatable.
    while( true ) {
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->index_mutex) );
        uint32_t level0_sst_num = db_state->db_info->list_of_sst_index_node[0].size();
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->index_mutex) );        
        if(allow_delay && level0_sst_num >= LO_COMPACTION_TRIGGER) {
            pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
            usleep(1000);//sleep 1000 microseconds 
            allow_delay = false;
            pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
        } else if (!force && 
            get_memtable_usage(db_state->db_info->ptr_memtable) + kv_size <= MAX_MEMTABLE_SIZE_BYTE) {
            // There is room in current memtable for putting current kv in; e_Ok!
            break;
        } else if (db_state->db_info->ptr_immutable != NULL) {
            //We have filled up the current memtable, but the immutable dump hasn't finished
            pthread_log("wait", pthread_cond_wait(&db_state->db_info->mem_condv, &db_state->db_info->mem_mutex) );
        } else if (level0_sst_num >= L0_STOP_WRITES_TRIGGER ) {
            // There are too many level-0 files.We must stop.
            pthread_log("wait", pthread_cond_wait(&db_state->db_info->mem_condv, &db_state->db_info->mem_mutex) );
        } else {
            // Attempt to switch to a new memtable and trigger compaction of old
            db_state->db_info->ptr_immutable= db_state->db_info->ptr_memtable;
			db_state->db_info->ptr_immutable->is_imm = true;
			db_state->db_info->ptr_memtable = (Memtable *) malloc(sizeof(Memtable));
            new_memtable(db_state->db_info->ptr_memtable);
            force = false;   // Do not force another compaction if have room
            maybe_schedule_compaction(db_state);
        }
    }
    //Log("make_room_for_put() end %d -- status %d", __LINE__, s);
    return s;
}


//GaoLong added 2013/12/2
bool update_allowed_seeks(Compaction_info * compaction_info, 
                                            sst_index_node *seek_sst, 
                                            const int32_t seek_level)
{
    if (seek_sst != NULL) {
        seek_sst->allowed_seeks--;
        if (seek_sst->allowed_seeks <= 0 && compaction_info->sst_to_compact_for_seek == NULL) {
        //compaction_info->sst_to_compact_for_seek = seek_sst;
        compaction_info->sst_to_compact_for_seek = new sst_index_node;
        *compaction_info->sst_to_compact_for_seek = *seek_sst;
        compaction_info->sst_to_compact_for_seek_level = seek_level;
        return true;
        }
    }
    return false;
}

//GaoLong added 2013/12/1
status db_put(Db_state * db_state, const slice key, const slice value)
{
    status s  = e_Ok;
    size_t cur_kv_size = key.size + value.size + 8;

    pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
    // May temporarily unlock and wait when call make_room_for_put.
    s = make_room_for_put(db_state, cur_kv_size);   
    pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) ); 
    
    if(s == e_Ok) {//critical region protected by mem_mutex
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
        s = memtable_put(db_state->db_info->ptr_memtable, key, value, e_TypeValue, db_state->db_info->sequence + 1);
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
    }
    
    if(s == e_Ok) { 
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->sequence_mutex) );
        ++(db_state->db_info->sequence);   //Is any other ++last_sequence operator except for db_delete? 
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->sequence_mutex) );
    }
    
    return s;
}

//GaoLong added 2013/12/1
status db_delete(Db_state * db_state, const slice key)
{
    status s = e_Ok;
    slice value = {"", 0};
    size_t cur_kv_size = key.size + 8;
    
    pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
    // May temporarily unlock and wait.
    s = make_room_for_put(db_state,  cur_kv_size);
    pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );    
    
    if(s == e_Ok){//critical region protected by mem_mutex
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
        s = memtable_put(db_state->db_info->ptr_memtable, key, value, e_TypeDeletion, db_state->db_info->sequence + 1);
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
    }

    if(s == e_Ok) {
        pthread_log("lock", pthread_mutex_lock(&db_state->db_info->sequence_mutex) );
        ++(db_state->db_info->sequence);   //Is any other ++last_sequence operator except for db_put? 
        pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->sequence_mutex) );
    }
    
    return s;
}


/*
    1.call block's iterator to find the key
        1> memtable
        2> level 0
        3> level 1-N
    2. do judge the validity
  */
status db_get( Db_state * db_state, const slice key, slice* value)
{
    status s = e_NotFound;
    sst_index_node seek_sst;
    sst_index_node last_sst;
    int32_t seek_level = -1;
    int32_t last_level = -1;
    bool has_sst_seeked = false;
    pthread_log("lock", pthread_mutex_lock(&db_state->db_info->mem_mutex) );
    if(memtable_get(db_state->db_info->ptr_memtable, key, *value) == e_Ok) {//Get from memtable
        //Done
        s = e_Ok;
    } else if (db_state->db_info->ptr_immutable != NULL 
                && memtable_get(db_state->db_info->ptr_immutable, key, *value) == e_Ok ) {//Get from immutable
        //Done
        s = e_Ok;
    } else {//Get from sstable level 0 ~ MAX-1
        block_index_node * bnode = new block_index_node;
        for(int32_t level = 0; level <MAX_LEVEL; level++ ) {//Search each level according to block's key-ranges
            //If sstable number of current level is zero, then skip to the next level.
            size_t num_files = db_state->db_info->list_of_sst_index_node[level].size();
            if (num_files == 0) continue;
            if ( find_block_index_node(db_state->db_info->list_of_block_index_node, 
                                                        db_state->db_info->level0_info, 
                                                        key, level, bnode) == e_Ok) {//If we found that the key is in a block's key-range,then we check the block.
                if(last_level != -1 && seek_level == -1) {//Remember the 1st seeked sst.
                    seek_sst = last_sst;
                    seek_level = last_level;
                }
                uint64_t sst_id = get_sstid(bnode->block_id);
                if( find_sst_index_node(db_state->db_info->list_of_sst_index_node, level, sst_id, &last_sst) == e_Ok) {
                    last_level = level;
                } else { //error!sst_index are not match the block_index!
                    abort();
                }
                #if BLOOM_FILTER
                if ( !key_may_match(key, &bnode->bloom_filter) ) {
                    //fprintf(stdout, "key_not_match!\n");
                    continue;
                }
                #endif
                std::list<internal_key_value> list_internal_key_value;
                parsed_internalkey pkey;
                if(parse_block2kv(db_state, bnode, &list_internal_key_value) == e_Ok) {//TODO:opt vector then binary sort.
                    std::list<internal_key_value>::iterator iter;
                    for(iter = list_internal_key_value.begin(); 
                            iter != list_internal_key_value.end(); ++iter) {
                        parse_internalkey(iter->key, &pkey);
                        if(slice_userkey_compare(pkey.user_key, key) == 0) {
                            value->size = iter->value.size();
                            value->data = (char *) malloc (value->size + 1); // free outside
                            memset(value->data, 0x00, value->size + 1);
                            memcpy(value->data, iter->value.data(), value->size);
                            free(pkey.user_key.data);
                            break;           //We had found the newest KV, stop search.
                        }
                        free(pkey.user_key.data);
                    }
                    if( iter == list_internal_key_value.end()) {//We have to search for the next level.
                        s = e_NotFound;
                        continue;
                    } else if(pkey.type == e_TypeDeletion) {
                        s = e_NotFound;
                        break;//break out of the outer for-loop
                    } else if(pkey.type == e_TypeValue){
                        s = e_Ok;
                        break;//break out of the outer for-loop
                    } else {//holyshit!
                        s = e_Corruption;
                        break;//break out of the outer for-loop
                    }
                }else {
                    abort();
                }
            }
        }
        has_sst_seeked = true;
        delete bnode;
    }
    if(has_sst_seeked && update_allowed_seeks(db_state->compaction_info, &seek_sst, seek_level)) {
        maybe_schedule_compaction(db_state);
    }
    pthread_log("unlock", pthread_mutex_unlock(&db_state->db_info->mem_mutex) );
    return s;
}


//Using no accuracy scan.
status db_scan(Db_state * db_state)
{
    status s = e_Corruption;
    

    return s;
}

