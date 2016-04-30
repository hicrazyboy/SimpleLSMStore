/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: table.cpp
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
//#include "port.h"
#include "table.h"
#include "util.h"
#include "block.h"

    
#include <list>
#include <iterator>
#include <stdint.h>

//GaoLong added 2013/12/2
status find_sst_index_node(const std::list<sst_index_node>* const sst_index_list_level, 
                                                            const size_t level, 
                                                            const uint64_t sst_id, 
                                                            sst_index_node * sst_node)
{
    status s = e_NotFound;
    for(std::list<sst_index_node>::const_iterator iter = sst_index_list_level[level].begin(); 
            iter != sst_index_list_level[level].end(); iter++) {
        if( iter->sst_id == sst_id) {
            *sst_node = *iter;
            s = e_Ok;
            break;
        }
    }
    return s;
}


//NOTE: use pointer or node itself as elem in list???
status find_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, 
                                                            const size_t level, 
                                                            const internalkey min_key, 
                                                            const internalkey max_key, 
                                                            std::list<sst_index_node*>* p_list_sst_index_node) {
	std::list<sst_index_node> *l = &(sst_index_list_level[level]);
	std::list<sst_index_node>::iterator iter = l->begin();
	bool is_found = false;
	for(; iter != l->end(); iter++) {
		if(    internalkey_compare(iter->min_key, min_key) >= 0 
			&& internalkey_compare(iter->max_key, max_key) <= 0 ) {// max_key compare to maxkey??? or min_key compare to maxkey???
			p_list_sst_index_node->push_back(&(*iter));
			is_found = true;
		}
	}
	if(!is_found) {
		return e_NotFound;
	}
	return e_Ok;
}

//GL added 20131223
status delete_l0_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, std::list<sst_index_node*> inputs_lvl0)
{
    status s = e_Ok;
    for(std::list<sst_index_node>::iterator iter = sst_index_list_level[0].begin();
        iter != sst_index_list_level[0].end();
        ++iter) {
            
        //Check if iter is in inputs_[0]
        for(std::list<sst_index_node*>::iterator check_cursor = inputs_lvl0.begin();
                check_cursor != inputs_lvl0.end();
                ++check_cursor) {
            if( (*check_cursor)->sst_id == iter->sst_id ) { //Do in!
                iter = sst_index_list_level[0].erase(iter);
                --iter;
                break;
            }
        }
            
    }
    return s;
}

status delete_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, 
                                                                const size_t level, 
                                                                const internalkey min_key, 
                                                                const internalkey max_key) {
	std::list<sst_index_node> *l = &(sst_index_list_level[level]);
	std::list<sst_index_node>::iterator iter = l->begin();
	for (; iter != l->end(); iter++ ) {
		if( internal_key_is_in_range( iter->min_key, min_key, max_key) &&
			internal_key_is_in_range( iter->max_key, min_key, max_key ) ) {
			// node matches the range, erase it.
			iter = l->erase(iter);
			--iter;
		}
	}
	return e_Ok;
}

status insert_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, 
                                                                const size_t level, 
                                                                sst_index_node* p_sst_index_node) {
	std::list<sst_index_node> *l = &(sst_index_list_level[level]);
	std::list<sst_index_node>::iterator iter = l->begin();

	for(; iter != l->end(); iter++) {
		if ( internalkey_compare(p_sst_index_node->min_key, iter->min_key) > 0) {
				//do nothing, just let iter++
		}
		else {
				// 2. insert
				//insert the [*pNode] before iterator.
				l->insert(iter, *p_sst_index_node);
				return e_Ok;
		}
	}
	//the node to insert larger than all keys given in list, so add it to the last.
	if(iter == l->end()) {
		l->push_back(*p_sst_index_node);
		return e_Ok;
	}
	//ASSUME IT IS AN IOERROR???
	return e_IOError;
}
