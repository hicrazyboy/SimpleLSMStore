/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: memtable.cpp
  Author:        Version :          Date:
  Description:         
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/
    
#include "skiplist.h"
#include "format.h"
#include "memtable.h"
//#include "error.h"

#include <string.h>
#include <stdint.h>

using namespace std;

void new_memtable(Memtable* memtable) {
//	memtable = (Memtable*) malloc (sizeof(Memtable));
	memtable->is_imm  = false; // NOT immutable
	memtable->curr_memtable_size = 0;

	//add by zxx @ 2013/12/07
//	memtable->skiplist = (Skiplist*)malloc(sizeof(Memtable));
	memtable->skiplist = (Skiplist*)malloc(sizeof(Skiplist)); // free when we need to free memtable TODO : GL
	init_skiplist(memtable->skiplist);
//	pthread_mutex_init(&(memtable->mutex), NULL);//GaoLong removed. Mmutex should be under the upper controlling.
    return;
}


//@key: suppose to be user_key
//user_key + sequence + type ==> internal_key
//in skiplist, we use the internal_key
status memtable_put(Memtable* memtable, 
                                        slice user_key, 
                                        slice value, 
                                        ValueType type, 
                                        uint64_t sequence){
    internalkey internal_key;
    slice slice_internal_key;

    userkey_to_internalkey(user_key, type, sequence, &internal_key);
    slice_internal_key.data = (char *) malloc (internal_key.i_key.size() + 1);
    slice_internal_key.size = internal_key.i_key.size();
    memcpy(slice_internal_key.data, internal_key.i_key.c_str(), slice_internal_key.size);
    //internalkey_to_slice(internal_key, &slice_internal_key);
    
    skiplist_put(memtable->skiplist, slice_internal_key, value);
    
    memtable->curr_memtable_size += (slice_internal_key.size + value.size);
    free(slice_internal_key.data);
    return e_Ok;
}

//get the user_key part out of memtable
//@key: user_key
status memtable_get(Memtable* memtable, slice user_key, slice& value){
    slice internal_key;
    internal_key.size = user_key.size + 8;
    internal_key.data = (char*) malloc (user_key.size); //free below

    memset(internal_key.data, 0xFF, user_key.size + 8);  
    memcpy(internal_key.data, user_key.data, user_key.size);
    status s = skiplist_get(memtable->skiplist, internal_key, value);
 
    free(internal_key.data);
    internal_key.data = NULL;
    return s;
}

//@key: user_key 
status memtable_delete(Memtable* memtable, slice key, slice value, uint64_t &sequence) {
	ValueType type = e_TypeDeletion;
	return memtable_put(memtable, key, value, type, sequence);
}


size_t get_memtable_usage(Memtable* memtable) {
	return memtable->curr_memtable_size;
}

