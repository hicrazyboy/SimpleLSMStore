/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: block.cpp
  Author:        Version :          Date:
  Description: for 
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/

#include "block.h"
//#include "port.h"
#include "util.h"
#include "format.h"
#include "log.h"

#include <list>
#include <iterator>
#include <assert.h>
#include <stdint.h>


//block_id format
uint64_t get_blockid(uint64_t sst_id, const uint32_t curr_sst_size)
{
    uint64_t block_id = curr_sst_size | sst_id;
    return block_id;
}

//GaoLong added 2013/12/2
uint64_t get_sstid(const uint64_t block_id)
{
    uint8_t exponent = 0;
    for(uint32_t number = MAX_SST_SIZE_BYTE; number > 1; number = number >> 1, exponent++)  ;//Calcualte the exponent of base 2.
    uint64_t mask = (~0ULL) << exponent; // mask = 11111111...1 00000...0 -- We has exponent zero
    return (block_id & mask);
}

//locate the bucket index , according to user_key, not internal_key
uint32_t bucket_index_of_user_key(const slice user_key, const uint32_t level) {
    assert (level > 0);
    assert (level < MAX_LEVEL);
    uint32_t level_index_num;
    switch (level) 
    {
        case 1:
            level_index_num = LEVEL1_INDEX_NUM;
            break;
        case 2:
            level_index_num = LEVEL2_INDEX_NUM;
            break;
        case 3:
            level_index_num = LEVEL3_INDEX_NUM;
            break;
        case 4:
            level_index_num = LEVEL4_INDEX_NUM;
            break;
        case 5:
            level_index_num = LEVEL5_INDEX_NUM;
            break;
        case 6:
            level_index_num = LEVEL6_INDEX_NUM;
            break;
    }
    int32_t position = hash(user_key.data, user_key.size, level_index_num);

    assert(position >= 0);
    assert(position < level_index_num);
    return position;
}


bool internal_key_is_in_range(const internalkey key, const internalkey min_key, const internalkey max_key) {
    if( internalkey_compare(key, min_key) >= 0 &&
        internalkey_compare(key, max_key) <= 0) {
        return true;
    }
    else {
        return false;
    }
}

//@key: user_key  NOT internal_key
//return true if key is in the given range.
bool user_key_is_in_range(const slice user_key, const internalkey min_key, const internalkey max_key)
{
    slice s_min_key, s_max_key;
    s_min_key.data = const_cast<char *>(min_key.i_key.c_str());
    s_min_key.size = min_key.i_key.size() - 8;
    s_max_key.data = const_cast<char *>(max_key.i_key.c_str());
    s_max_key.size = max_key.i_key.size() - 8;
    if( slice_userkey_compare(user_key, s_min_key) >= 0 &&
        slice_userkey_compare(user_key, s_max_key) <= 0) {
        return true;
    } else {      
        return false;
    }
}


//GL added
bool user_key_is_before_block(const slice user_key, const block_index_node &block)
{
    slice s_min_key;
    s_min_key.data = const_cast<char *>(block.min_key.i_key.c_str());
    s_min_key.size = block.min_key.i_key.size() - 8;
    if( slice_userkey_compare(user_key, s_min_key) < 0) {
        return true;
    } else {
        return false;
    }
}

bool user_key_is_after_block(const slice user_key, const block_index_node &block)
{
    slice s_max_key;
    s_max_key.data = const_cast<char *>(block.max_key.i_key.c_str());
    s_max_key.size = block.max_key.i_key.size() - 8;
    if( slice_userkey_compare(user_key, s_max_key) > 0) {
        return true;
    } else {
        return false;
    }
}


//yueyinliang: given min_key, return the corresponding block_index_node
//GaoLong modified : min_key type from slice to internalkey
//@min_key : is an user_key
//TODO: 
status find_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                        const Level0_info level0_info, 
                                                        const slice min_key, 
                                                        const uint32_t level, 
                                                        block_index_node* p_block_index_node)
{
    assert(level < MAX_LEVEL);
    //level 0 is different from other levels.
    if(level == 0) {
        //deal with level 0
        //design principle:
        //start: refer to where current sstable begins. when compact is done, need to change start.
        //end:  refer to where last sstable ends
        //search from end to start.
        if(level0_info.cursor_end == level0_info.cursor_start) {
            std::list<block_index_node>* l1 = &(block_index_list_level[level][level0_info.cursor_end]);
            if(l1->empty()) {
                return e_NotFound;
            } else {
                for(std::list<block_index_node>::iterator iter=l1->begin(); iter != l1->end(); iter++) {
                    if (user_key_is_in_range(min_key, iter->min_key, iter->max_key ) ) {
                        *p_block_index_node = *iter;
                        return e_Ok;
                    }
                }
                return e_NotFound;
            }
        }
        //enter here when level0_info.cursor_end != level0_info.cursor_start
        //for(int i = level0_info.cursor_end; i != level0_info.cursor_start; i--) {
        //changed by zxx @2013/12/10
        for(int i = level0_info.cursor_end; ; i--) {
            std::list<block_index_node>* l2 = &(block_index_list_level[level][i]);
            if( !l2->empty() ) {
                std::list<block_index_node>::iterator iter = l2->begin();
                for(; iter != l2->end(); iter++) {
                    if (user_key_is_in_range(min_key, iter->min_key, iter->max_key ) ) {
                        *p_block_index_node = *iter;
                        return e_Ok;
                    }
                }
            }
            //below 'if' is added by zxx @2013/12/10
            if(i == level0_info.cursor_start) {
                break;  //jump out of the for loop
            }
            //special action when i == 0
            if(i == 0) {
                    i = L0_STOP_WRITES_TRIGGER;
            }
        }//end for
        return e_NotFound;
    } else{
        int pos = bucket_index_of_user_key(min_key, level);
        //TODO: to check whether it is in the last node in [pos-1 ], note that [pos -1] may by invalied value 
        //NOTE: if pos is already 0, no necessary to find in the pos-1 level.
        // 1.0 search level-pos-1, may exist in the last node 
        if(pos > 0) {
            std::list<block_index_node>* l1 = &(block_index_list_level[level][pos-1]);
            if(!l1->empty()) {
                    std::list<block_index_node>::iterator iter1 = l1->end(); // [GL]
                    --iter1;// [GL]
                    if( user_key_is_in_range(min_key, iter1->min_key, iter1->max_key) ) {
                            *p_block_index_node = *iter1;
                            return e_Ok;
                    }
            }
        }
        // 2.0 search below level-pos.
        std::list<block_index_node>* l2 = &(block_index_list_level[level][pos]);
        if(!l2->empty()) {
            std::list<block_index_node>::iterator iter2 = l2->begin();
            for(; iter2 != l2->end(); iter2++) {
                if (user_key_is_before_block(min_key, *iter2) ) { //[GL]
                    break;
                } else if (!user_key_is_after_block(min_key, *iter2 ) ) {
                    *p_block_index_node = *iter2;
                    return e_Ok;
                }
            }
        }
        else{
            return e_NotFound;
        }
    }
    return e_NotFound;
}

//yueyinliang: insert block_index_node_i to the designated level

status insert_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                            Level0_info * level0_info, 
                                                            const uint32_t level, 
                                                            block_index_node* p_block_index_node){
    status s = e_Ok;
    if(level == 0) {
        //Log("insert_block_index_node begin level = 0 branch");
        //for(int i = 0; i < LEVEL0_SST_NUM; ++i) {
        //    Log("block_index_list_level[0][%d].size() = %d", i, block_index_list_level[0][i].size());
        //}
            //deal with level 0
            //when compact memtables, [start, end] must be reset. 
            //decide insert into end or end+1 level.
            if(block_index_list_level[0][level0_info->cursor_end].empty()){//Iff block_index_list_level[0] is empty ( block_index_list_level[0][level0_info.cursor_end] is empty only if block_index_list_level[0] is empty! ) 
                block_index_list_level[0][level0_info->cursor_end].push_back(*p_block_index_node);
                //free(p_block_index_node);
                s = e_Ok;
            } else {
                if( sstid_compare(p_block_index_node->block_id, block_index_list_level[0][level0_info->cursor_end].begin()->block_id) == 0 ) {//GL:sstid_compare -->??
                    //insert into end level
                    block_index_list_level[0][level0_info->cursor_end].push_back(*p_block_index_node);
                    //free(p_block_index_node);
                    s = e_Ok;
                }
                else {
                    //insert into end+1 level //GL modified
                    //Assert cursor won't reaches the limit+1
                    assert(++level0_info->cursor_end < LEVEL0_SST_NUM );
                    assert(block_index_list_level[0][level0_info->cursor_end].empty());  //when dump to  higher level,  should clear the corresponding info in block_index_list.
                    block_index_list_level[0][level0_info->cursor_end].push_back(*p_block_index_node);
                    //free(p_block_index_node);
                    s = e_Ok;
                }
            }
    //Log("insert_block_index_node end level = 0 branch");
    //for(int i = 0; i < LEVEL0_SST_NUM; ++i) {
    //    Log("block_index_list_level[0][%d].size() = %d", i, block_index_list_level[0][i].size());
    //}
            return s;
        } else {
            //get bucket index
            slice s_min_key;
            s_min_key.data = const_cast<char *>(p_block_index_node->min_key.i_key.c_str());
            s_min_key.size = p_block_index_node->min_key.i_key.size() - 8;
            int bucket_index = bucket_index_of_user_key(s_min_key, level);
            //insert corresponding list
            std::list<block_index_node>* l = &(block_index_list_level[level][bucket_index]);
            if(l->empty()) {  //if the list is empty
                l->push_back(*p_block_index_node);
                //free(p_block_index_node);
                return e_Ok;
            } else {
                // 1. locate the place to insert
                std::list<block_index_node>::iterator iter = l->begin();
                
                for(; iter != l->end(); iter++){
                    if (internalkey_compare(p_block_index_node->min_key, (*iter).min_key) > 0) {
                        //do nothing, just let iter++
                    }
                    else {
                        // 2. insert
                        //insert the [*pNode] before iterator.
                        l->insert(iter, *p_block_index_node);
                        //free(p_block_index_node);
                        return e_Ok;
                    }
                }//end for
                //the node to insert larger than all keys given in list, so add it to the last.
                if(iter == l->end()) {
                    l->push_back(*p_block_index_node);
                    //free(p_block_index_node);
                    return e_Ok;
                }
            }
        }
        return e_IOError;
}


//GL added    //deal with level 0
status delete_l0_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                                Level0_info * level0_info,
                                                                std::list<sst_index_node*> inputs_lvl0)
{
    status s = e_Ok;
    if(level0_info->cursor_end == level0_info->cursor_start) {
        if(block_index_list_level[0][level0_info->cursor_end].empty()) {
            s = e_NotFound;
        } else {
            uint64_t target_sst_id = get_sstid(block_index_list_level[0][level0_info->cursor_end].front().block_id);
            for(std::list<sst_index_node*>::iterator iter = inputs_lvl0.begin();
                iter != inputs_lvl0.end();
                ++iter) {
                if( (*iter)->sst_id == get_sstid(block_index_list_level[0][level0_info->cursor_end].front().block_id) ) {
                    block_index_list_level[0][level0_info->cursor_end].clear();
                }
            }
        }
        s = e_Ok;
    } else {
        int cursor = static_cast<int>(level0_info->cursor_end);
        for( ; cursor >= 0; cursor--) {
            assert( !block_index_list_level[0][cursor].empty() );
            uint64_t target_sst_id = get_sstid(block_index_list_level[0][cursor].front().block_id);
            for(std::list<sst_index_node*>::iterator iter = inputs_lvl0.begin();
                iter != inputs_lvl0.end();
                ++iter) {
                if( (*iter)->sst_id == target_sst_id ) {
                    block_index_list_level[0][cursor].clear();
                    
                    //push together
                    for(int i = cursor;i < level0_info->cursor_end; ++i) {
                        block_index_list_level[0][i] = block_index_list_level[0][i + 1];
                    }
                    block_index_list_level[0][level0_info->cursor_end] = std::list<block_index_node>();
                    
                    if(level0_info->cursor_end != level0_info->cursor_start) {
                        --level0_info->cursor_end;
                    }
                    break;
                }
            }
        }
        s = e_Ok;
    } //end for
    return s;
}

//yueyinliang: delete the corresponding block_index_node in the designated level with the given min_key 
status delete_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                            const uint32_t level, 
                                                            const internalkey min_key, 
                                                            const internalkey max_key){
#if 0
    if(level == 0) {
        //deal with level 0
        if(level0_info.cursor_end == level0_info.cursor_start) {
            std::list<block_index_node>* l1 = &(block_index_list_level[level][level0_info.cursor_end]);
            if(l1->empty()) {
                return e_NotFound;
            }
            else {
                for(std::list<block_index_node>::iterator iter=l1->begin(); iter != l1->end(); iter++) {
                    if( internal_key_is_in_range(iter->min_key, min_key, max_key) &&
                        internal_key_is_in_range(iter->max_key, min_key, max_key) ) {
                        iter = l1->erase(iter);
                        --iter;//GL
                    }
                }
            }
            return e_Ok;
        }
        //delete node matches from cursor_end to cursor_start.
        //enter here when level0_info.cursor_end != level0_info.cursor_start
        //for(int i = level0_info.cursor_end; i != level0_info.cursor_start; i--) {
        //changed by zxx @2013/12/11
        for(int i = level0_info.cursor_end; ; i--) {
            std::list<block_index_node>* l2 = &(block_index_list_level[level][i]);
            if( !l2->empty() ) {
                std::list<block_index_node>::iterator iter = l2->begin();
                for(; iter != l2->end(); iter++) {
                    if( internal_key_is_in_range(iter->min_key, min_key, max_key) &&
                        internal_key_is_in_range(iter->max_key, min_key, max_key) ) {
                        iter = l2->erase(iter);
                        --iter;//GL
                    }
                }
            }
            if(i == level0_info.cursor_start) {
                break; //jump out of the for loop
            }
            //special action when i == 0
            if(i == 0) {
                    i = LEVEL0_SST_NUM;
            }
        } //end for
        return e_Ok;
    }
#endif
    assert(level != 0);
    // 1. get position range by min_key & max_key, we only need to search this range block index list.
    slice s_min_key;
    s_min_key.data = const_cast<char *>(min_key.i_key.c_str());
    s_min_key.size = min_key.i_key.size() - 8;
    slice s_max_key;
    s_max_key.data = const_cast<char *>(max_key.i_key.c_str());
    s_max_key.size = max_key.i_key.size() - 8;

    int low_bucket_index = bucket_index_of_user_key(s_min_key, level);//(slice user_key,uint32_t level)
    int high_bucket_index = bucket_index_of_user_key(s_max_key, level);//(slice user_key,uint32_t level)
    assert(low_bucket_index <= high_bucket_index);

    for(int i = low_bucket_index; i <= high_bucket_index; i++){
    std::list<block_index_node>* l = &(block_index_list_level[level][i]);
    for (std::list<block_index_node>::iterator iter = l->begin(); iter != l->end(); iter++) {
//Log("%s size = %d, -- line : %d", iter->min_key.i_key.c_str(), iter->min_key.i_key.size(), __LINE__ );
//Log("%s size = %d, -- line : %d", iter->max_key.i_key.c_str(), iter->max_key.i_key.size(), __LINE__ );
        if(internal_key_is_in_range(iter->min_key, min_key, max_key) &&
            internal_key_is_in_range(iter->max_key, min_key, max_key ) ) {
            // node matches the range, erase it.
            iter = l->erase(iter);
            --iter;//GL
        }
    }
}
        // 2. we use the simplest way at first. 
        // check every node in list to see matches the range. delete when hit the range.
//RESERVED. we use simple at first. see above 2.
//complicate way!   // 2. for each list[level][position], if the first node bigger than max_key, no need to search the rest in list
    
    return e_Ok;
}

//GL added 20131223
status find_l0_block_index_node_list(std::list<block_index_node> **  block_index_list_level,
                                                                    const Level0_info level0_info, 
                                                                    uint64_t sst_id,
                                                                    std::list<block_index_node*>* block_list)
{
    status s = e_Ok;
    if(level0_info.cursor_end == level0_info.cursor_start) { // only one sstable in Level-0
        assert(level0_info.cursor_end == 0);
        std::list<block_index_node>* l1 = &(block_index_list_level[0][level0_info.cursor_end]);
        if(l1->empty() ) {
            s = e_NotFound;
        } else if (get_sstid(l1->front().block_id) != sst_id) {
            s = e_NotFound;
        } else {//match the sst_id
            for(std::list<block_index_node>::iterator iter=l1->begin(); iter != l1->end(); iter++) {
                block_list->push_back(&(*iter));
            }
            s = e_Ok;
        }
    } else {
        for(int cursor = level0_info.cursor_end; cursor >= static_cast<int>(level0_info.cursor_start); cursor--) {
            if( !block_index_list_level[0][cursor].empty() && 
                get_sstid(block_index_list_level[0][cursor].front().block_id) == sst_id) {//match the sst_id
                for(std::list<block_index_node>::iterator iter = block_index_list_level[0][cursor].begin(); 
                        iter != block_index_list_level[0][cursor].end(); 
                        iter++) {
                    block_list->push_back(&(*iter));
                }
                s = e_Ok;
            }
        }
    }
    return s;
}


//this funciton only returns the nodes which are totally in the range of [min_key, max_key]
//min_key max_key is internalkey here.
//get the block index list by [minkey, maxkey] of one SSTable
status find_block_index_node_list(std::list<block_index_node> ** block_index_list_level, 
                                                                const Level0_info level0_info, 
                                                                const internalkey min_key, 
                                                                const internalkey max_key, 
                                                                const uint32_t level, 
                                                                std::list<block_index_node*>* block_list) {
    assert(level != 0);
    status s = e_NotFound;
    if(level == 0) {
        //deal with level 0
        //design principle:
        //start: refer to where current sstable begins. when compact is done, need to change start.
        //end:  refer to where last sstable ends
        //search from end to start.
        if(level0_info.cursor_end == level0_info.cursor_start) {
            std::list<block_index_node>* l1 = &(block_index_list_level[level][level0_info.cursor_end]);
            if(!l1->empty()) {
                for(std::list<block_index_node>::iterator iter=l1->begin(); iter != l1->end(); iter++) {
                    if( internal_key_is_in_range(iter->min_key, min_key, max_key) &&
                        internal_key_is_in_range(iter->max_key, min_key, max_key) ) {
                        block_list->push_back(&(*iter));
                        s = e_Ok;
                    }
                }
            } else {
                s = e_NotFound;
            }
        } else {
            //enter here when level0_info.cursor_end != level0_info.cursor_start
            //for(int i = level0_info.cursor_end; i != level0_info.cursor_start; i--) {
            //changed by zxx @2013/12/20
            for(int i = level0_info.cursor_end; ; i--) {
                std::list<block_index_node>* l2 = &(block_index_list_level[level][i]);
                if( !l2->empty() ) {
                    std::list<block_index_node>::iterator iter = l2->begin();
                    for(; iter != l2->end(); iter++) {
                        if( internal_key_is_in_range(iter->min_key, min_key, max_key) &&
                            internal_key_is_in_range(iter->max_key, min_key, max_key) ) {
                            block_list->push_back(&(*iter));
                            s = e_Ok;
                        }
                    }
                }
                if(i == level0_info.cursor_start) {
                    break; //jump out of the for loop
                }
            }//end for
        }
    } else {
            slice s_min_key;
            s_min_key.data = const_cast<char *>(min_key.i_key.c_str());
            s_min_key.size = min_key.i_key.size() - 8;
            slice s_max_key;
            s_max_key.data = const_cast<char *>(max_key.i_key.c_str());
            s_max_key.size = max_key.i_key.size() - 8;

            int low_bucket_index = bucket_index_of_user_key(s_min_key, level);
            int high_bucket_index = bucket_index_of_user_key(s_max_key, level);
            // check every node in list to see matches the range
            for(int i = low_bucket_index; i <= high_bucket_index; i++) {//[GL] TODO optimize
                std::list<block_index_node>* l = &(block_index_list_level[level][i]);
                for (std::list<block_index_node>::iterator iter = l->begin(); iter != l->end(); iter++) {
                    if(internal_key_is_in_range(iter->min_key, min_key, max_key) &&
                   internal_key_is_in_range(iter->max_key, min_key, max_key)) {
                    // node matches the range, add it to list.
                    block_list->push_back(&(*iter));
                    s = e_Ok;
                }
            }
        }
    }
    return s;
}

