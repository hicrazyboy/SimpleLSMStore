/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: skiplist.cpp
  Author:        Version :          Date:
  Description: for 
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/
#include "skiplist.h"
#include "format.h"
#include "util.h"
//#include "port.h"

#include <assert.h>
#include <string.h>
#include <stdint.h>

//@key: internal_key
ptr_skiplist_node new_skiplist_node(slice key, slice value, uint32_t height){
  //  ptr_skiplist_node ptr_new_node = (ptr_skiplist_node)malloc(sizeof(Skiplist_node) + (height - 1) * sizeof(ptr_skiplist_node));
//deleted by zxx@2013/12/08
    ptr_skiplist_node ptr_new_node = (ptr_skiplist_node)malloc(sizeof(Skiplist_node) + (height) * sizeof(ptr_skiplist_node)); //GL TODO: free() outside when skiplist to destory
	ptr_new_node->key.data = (char*)malloc(key.size + 1);//GL TODO free()
	//memset(ptr_new_node->key.data, 0x00, key.size + 1);
	ptr_new_node->key.size = key.size;

	ptr_new_node->value.data = (char*)malloc(value.size + 1);//GL TODO free()
	//memset(ptr_new_node->value.data, 0x00, value.size + 1);
	ptr_new_node->value.size = value.size;

	memcpy(ptr_new_node->key.data, key.data, key.size);
	memcpy(ptr_new_node->value.data, value.data, value.size);
	
	return ptr_new_node;
}


void init_skiplist(Skiplist* skiplist){
    int i = 0 ;
//	assert(skiplist != NULL);
//	skiplist->nil = (Skiplist_node* )malloc(sizeof(Skiplist_node) + (MAX_SKIPLIST_HEIGHT - 1)*sizeof(ptr_skiplist_node));
	skiplist->nil = (Skiplist_node* )malloc(sizeof(Skiplist_node) + (MAX_SKIPLIST_HEIGHT)*sizeof(ptr_skiplist_node));//GL TODO: free() when the skiplist is to be destory
    //add by zxx @2013/12/08
//	skiplist->nil->forward = (ptr_skiplist_node*)malloc(MAX_SKIPLIST_HEIGHT * sizeof(ptr_skiplist_node));

	for(i = 0; i < MAX_SKIPLIST_HEIGHT; i++){
        skiplist->nil->forward[i] = NULL;
    }
	skiplist->nil->key.data = NULL;
	skiplist->nil->value.data = NULL;
//changed by zxx @2013/12/08
//	skiplist->head = (Skiplist_node* )malloc(sizeof(Skiplist_node) + (MAX_SKIPLIST_HEIGHT - 1)*sizeof(ptr_skiplist_node));

	skiplist->head = (Skiplist_node* )malloc(sizeof(Skiplist_node) + (MAX_SKIPLIST_HEIGHT)*sizeof(ptr_skiplist_node));//GL TODO: free() when the skiplist is to be destory
	for(i = 0; i < MAX_SKIPLIST_HEIGHT; i++){
        skiplist->head->forward[i] = skiplist->nil;
	}

//	skiplist->curr_max_height = 0;  //deleted by zxx @2013/12/06
	skiplist->curr_max_height = 1;
	return;
}


//[GL] cmp: skiplist put :slice_userkey_compare
//skiplist_get :slice_internalkey_compare
bool key_is_after_node(slice key, Skiplist_node* next, comparator cmp)
{
	#if 0
    internalkey next_internalkey, curr_key; 

   // if(NULL == next->forward[0]){//this node is nil //deleted by zxx @ 2013/12/07
	//  if(NULL == next){
	if(NULL == next->forward[0]) {
        return false;
    }else{
	    slice_to_internalkey(key, &curr_key);
		slice_to_internalkey(next->key, &next_internalkey);

		if(internalkey_compare(next_internalkey, curr_key) > 0 )
            return false;
		else
			return true;
	}
    #endif
    #if 1
    return ( (next->forward[0] != NULL) && cmp(next->key, key) < 0);
    #endif
}


//@key: internal_key
Skiplist_node* find_greater_or_equal(Skiplist* skiplist, slice key, Skiplist_node** prev, comparator cmp){
	Skiplist_node* x = skiplist->head;
	//search the levels <= max_height
	int level = skiplist->curr_max_height - 1;

    Skiplist_node* next = NULL;
	while(true) {
		next = x->forward[level];
		if(key_is_after_node(key, next, cmp)) {
			x = next;
		} else { //key is in the previous nodes. we should go back then find the wanted key.
			if(prev != NULL)//record the search path
				prev[level] = x;
			if(level == 0) {
				return next;
			}
			else {
				level--;
			}			
		}
	}//end while
}


uint32_t random_next(uint32_t* seed){
	static const uint32_t M = 2147483647L;   // 2^31-1
	static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0

	// We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = (*seed) * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    (*seed) = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if ( (*seed) > M ) {
      (*seed) -= M;
    }
    return 0;
}


//generate the height used in skiplist, start from 1, to MAX_SKIPLIST_HEIGHT
uint32_t random_height(){
	static const unsigned int kBranching = 4;
	int height = 1;
	uint32_t s = 0xdeadbeef;
	uint32_t seed_ = s & 0x7fffffffu;
	while (height < MAX_SKIPLIST_HEIGHT&& (seed_ % kBranching) == 0) {
		random_next(&seed_);
		height++;
	}
	assert(height > 0);
	assert(height <= MAX_SKIPLIST_HEIGHT);
	return height;
}


//@key: internal_key
uint32_t skiplist_put(Skiplist* skiplist, slice key, slice value){
    Skiplist_node* prev[MAX_SKIPLIST_HEIGHT];
    comparator cmp = slice_userkey_compare;//[GL]
    Skiplist_node* x = find_greater_or_equal(skiplist, key, prev, cmp);

    int height = random_height();
	if(height > skiplist->curr_max_height) {
		//set the top parts pointed by skiplist head. add to node below
		for(int i = skiplist->curr_max_height; i < height; i++) {
			prev[i] = skiplist->head;
		}//end for
		//reset current max_height
		skiplist->curr_max_height = height;
	}//end if

	//BELOW PART NEED TO RECONSIDER.
	x = new_skiplist_node(key, value, height);
	for(int i = 0; i < height; i++) {
		x->forward[i] = prev[i]->forward[i];
		prev[i]->forward[i] = x;
	}
	return 0;
}


//@user_key: here is internal_key: format:  memtable's userkey + maxsequence + max_type{actually all set to 1}
//NOTE: user_key here is the slice form of an internalkey
//@return value: return internal_key if found
status skiplist_get(Skiplist* skiplist, slice user_key, slice& value){
    parsed_internalkey parsed_x_key;
    internalkey x_key;
    parsed_internalkey parsed_userkey; //to transfer the slice form user_key(actually it is a internalkey NOT user_key!) to internalkey
    internalkey ik_userkey;
    comparator cmp = slice_internalkey_compare;//[GL]
    Skiplist_node* x = find_greater_or_equal(skiplist, user_key, NULL, cmp);
    //compare user_key with the actual key in x->key.
    //if(x != NULL && (slice_compare(user_key, x->key) == 0)){  //deleted by zxx@ 2013/12/07
    if(x->key.data != NULL && x->key.size != 0) { //GL
        slice_to_internalkey(user_key, &ik_userkey);
        parse_internalkey(ik_userkey, &parsed_userkey);
        slice_to_internalkey(x->key, &x_key);
        parse_internalkey(x_key, &parsed_x_key);

        if( slice_userkey_compare(parsed_userkey.user_key, parsed_x_key.user_key) != 0 ) {
            free(parsed_x_key.user_key.data);//GL
            free(parsed_userkey.user_key.data);
            return e_NotFound;
        }

        free(parsed_x_key.user_key.data);// GL
        free(parsed_userkey.user_key.data);

        switch(parsed_x_key.type){
            case e_TypeDeletion:
                return e_NotFound;
                break;
            case e_TypeValue:
                value.data = (char *)malloc(x->value.size + 1);	//Free outside here!
                memset(value.data, 0x00, x->value.size + 1);
                assert(value.data != NULL);
                memcpy(value.data, x->value.data, x->value.size);
                value.size = x->value.size;
                return e_Ok;
                break;
        }
    }else{
        return e_NotFound;
    }

}
