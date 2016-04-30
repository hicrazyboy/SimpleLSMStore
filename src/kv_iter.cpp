/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: kv_iter.cpp
  Author:        Version :          Date:
  Description: for 
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/
    
#include "kv_iter.h"
#include "block.h"
#include "storage.h"
#include "skiplist.h"

#include <list>
#include <string.h>
#include <iterator>
#include <string>
#include <assert.h>
#include <stdint.h>


status parse_memtable2kv(Memtable *memtable, std::list<internal_key_value>* p_list_internal_key_value) {
	ptr_skiplist_node node = memtable->skiplist->head->forward[0];
	while(node != memtable->skiplist->nil) {
		internal_key_value ikv;
		slice_to_internalkey(node->key, &(ikv.key));
		ikv.value.assign(node->value.data, node->value.size);
		p_list_internal_key_value->push_back(ikv);

		node = node->forward[0];
	}
	return e_Ok;
}


// 1.0 get block_id from node, then parse block_id to get sst_id and offset 
// 2.0 read disk through sst_id & offset in block_id
// 3.0 parse 2.0, get key and value  then push_back into list
//block format: key_size(32bits) | value_size(32bits) | key | value
status parse_block2kv(Db_state * db_state, block_index_node* node, std::list<internal_key_value>* p_list_internal_key_value) {
    //1.0
    uint64_t block_id = node->block_id;
    size_t block_size = node->size;

    uint32_t bits = log2_int(MAX_SST_SIZE_BYTE);
    uint64_t full1bits = (~0x0ull);
    uint64_t sst_id = block_id & (full1bits << bits);
    uint32_t offset = (uint32_t)(block_id & (~(full1bits << bits)));

    //	uint64_t sst_id = block_id >> 18;
    //	uint32_t offset = (uint32_t)(block_id & ((1 << 18) - 1));
    //2.0
    char buf[MAX_BLOCK_SIZE_BYTE + 1];
    if( logical_read(db_state->storage_info, block_id, buf, MAX_BLOCK_SIZE_BYTE) == e_IOError ) {
        return e_IOError;
    }
    //3.0 
    internal_key_value ikv;
    uint32_t key_size;
    uint32_t value_size;
    internalkey ik;
    uint32_t kv_amount = 0;
    std::string value; //TODO: 如果此处的value类型为slice或char*, 则此处data的内容需要用malloc分配内存么???
    for(int i = 0; i < block_size; ) {
        memcpy(&key_size, buf+i, sizeof(uint32_t));
        memcpy(&value_size, buf+i+4, sizeof(uint32_t));
        ik.i_key.resize(0); //clear string
        ik.i_key.assign(buf+i+8, key_size);
        value.resize(0);
        value.assign(buf+i+8+key_size, value_size);

        ikv.key = ik;
        ikv.value = value;
        if( ikv.key.i_key.size() == 0) { //GL
            abort();
        }
        p_list_internal_key_value->push_back(ikv);
        i += (4 + 4 + key_size + value_size);
        ++kv_amount;
    }
    assert(kv_amount == node->kv_amount);
    return e_Ok;
}



