// =====================================================================================
//
//       Filename:  ts.cpp
//
//    Description:  no
//
//        Version:  1.0
//        Created:  12/10/2013 04:47:32 AM
//       Revision:  none
//       Compiler:  gcc
//
//         Author:  GaoLong (GL), crazyboy.long@gmail.com
//   Organization:  ustc
//
// =====================================================================================



 //use binary search
uint32_t position_of( const std::vector<slice> &v, const slice key) {
    if( v.size() == 0) return -1;
    uint32_t low = 0;
    uint32_t high = v.size() -1;
    uint32_t mid = (low + high) / 2;
    while(low <= high) {
        if(slice_userkey_compare(key, v[mid]) >0) {
            low = mid + 1;
        } else if(slice_userkey_compare(key, v[mid]) < 0){
            high = mid - 1;
        } else {
            return mid;
        }
        mid = (low + high) / 2;
    }
    return mid;
}

bool get_overlaping_from_v0(const std::vector<slice> &v0, 
                                                const slice min_userkey, 
                                                const slice max_userkey, 
                                                std::vector<slice>* vc) {
    uint32_t low_pos = position_of(v0, min_userkey);
    uint32_t high_pos = position_of(v0, max_userkey);
    if(slice_userkey_compare(max_userkey ,v0[low_pos]) < 0 || 
        slice_userkey_compare(min_userkey, v0[high_pos]) > 0) {
        return false;  //no overlap
    }
    for(uint32_t i = low_pos; i <= high_pos; i++) {
        vc->push_back(v0[i]);
    }
    assert(!vc->empty());
    return true;
}

//Get the filter matched slices from VC, and store the set into VS. 
bool match_bloomfilter( const std::vector<slice> &vc, const std::string* filter, std::vector<slice>* vs) {
    for(std::vector<slice>::iterator iter = vc.begin(); iter != vc.end(); iter++) {
        if (key_may_match(*iter, filter)) {
            vs->push_back(*iter);
        }
    }
    if (vs->empty()) {
        return false; //no key match. no need to calcuate rate P, just do SCAN
    }
    return true;
}

//assert(vc_size != 0);
//assert(sst_size != 0);
//assert(vs_size != 0);
//assert(F <= 1.0);
double accuracy_if_drop(size_t vc_size, size_t sst_size, size_t vs_size, double false_rate)
{
	return  vc_size / (vc_size + sst_size - vs_size * (1 - false_rate) );
}


//assert(level > 0);
//assert(level < 7);
uint32_t level_index_num(uint32_t level) 
{
	switch(level)
	{
		case 1: 
			return LEVEL1_INDEX_NUM;
		case 2:
			return LEVEL2_INDEX_NUM;
		case 3:
			return LEVEL3_INDEX_NUM;
		case 4:
			return LEVEL4_INDEX_NUM;
		case 5:
			return LEVEL5_INDEX_NUM;
		case 6:
			return LEVEL6_INDEX_NUM;
	}
	return 0; //if enter here, check for error.
}

void get_level_block_index_node(std::list<block_index_node> ** block_index_list_level,
                                                                const uint32_t level,
                                                                std::list<block_index_node>* level_block_index_node) {
    uint32_t bucket_num = level_index_num(level);
    for(uint32_t i = 0; i < bucket_num; i++) {
        std::list<block_index_node>::iterator position = level_block_index_node->end();
        std::list<block_index_node>::iterator begin = block_index_list_level[level][i].begin();
        std::list<block_index_node>::iterator end = block_index_list_level[level][i].end();
        level_block_index_node->insert(position, begin, end);
    }
    return;
}

bool is_need_to_scan(const block_index_node* block_node, 
						 std::vector<slice>* v0, 
						 std::vector<slice>* vc, 
						 std::vector<slice>* vs) {
    //get min and max userkey in block node
    slice min_userkey;
    slice max_userkey;

    min_userkey.size = block_node.min_key.i_key.size() - 8;
    min_userkey.data = (char *) malloc (min_userkey.size + 1);
    memset(min_userkey.data, 0x00, min_userkey.size + 1);
    memcpy(min_userkey.data, block_node.min_key.i_key.c_str(), min_userkey.size);

    //get Vc
    get_overlaping_from_v0(*v0, min_userkey, max_userkey, slice_userkey_compare, vc);

    //get vs
    match_bloomfilter(vc, &block_node->bloom_filter, vs);

    //calculate p
    size_t vc_size = vc->size();
    size_t block_size = block_node->kv_amount;
    size_t vs_size = vs->size();
    if( (vc_size == 0) || (vs_size == 0) )
        return true;

    double accuracy_if_droped = accuracy_if_drop(vc_size, block_size, vs_size, FALSE_RATE);

    free(min_userkey.data);
    free(max_userkey.data);

    //judge whether to scan or not
    if(accuracy_if_droped >= ACCURACY) {
    //no need to scan. return  false
        return false;
    } else {
    //need ot scan.
        return true;
    //scan_block(storageinfo, block_node->block_id, v0);
    }
}

void deduplicate(std::vector<slice>* v0)
{
    assert(v0.size() != 0);
    
    
    //duplicate
    std::vector<slice>::iterator iter = v0->begin();
    std::vector<slice>::iterator next_iter = v0->begin();
    ++next_iter;
    
    for(uint32_t i_count = 0; iter != v0->end() && next_iter != v0->end(); i_count++) {
        if( slice_userkey_compare(*iter, *next_iter) == 0 ) {
            //delete the elem which next_iter points
            next_iter = v0->erase(next_iter);
        } else {
            ++iter;
            ++next_iter;
        }
    }    
}

//v0 will be changed, vs block_buf will not.
void update_v0(std::vector<slice>* v0_update_set, std::string* vs_bloomfilter, std::list<internal_key_value>* p_list_internal_key_value) {
	std::list<internal_key_value>::iterator list_iter = p_list_internal_key_value->begin();
	
	for(; list_iter != p_list_internal_key_value->end(); list_iter++) {
		//get userkey from internal_key_value
		parsed_internalkey pkey;
		parse_internalkey(list_iter->key, &pkey);
		if( key_may_match(pkey.user_key, vs_bloomfilter) == false ) {
			v0_update_set->push_back(pkey.user_key);
			// TODO: when to release the memory which pkey.user_key holds?
		}
	}

}

std::vector<slice> setup_v0(Db_state* db_state)
{
    std::vector<slice> v0; 

    //get immutable keys
    if(db_state->db_info->ptr_immutable != NULL)
    {
        for(ptr_skiplist_node node = db_state->db_info->ptr_immutable->skiplist->head->forward[0]; 
                node != db_state->db_info->ptr_immutable->skiplist->nil; 
                node = node->forward[0]) {
            //get keys
            v0.push_back(node->key);
            //get values
            //TODO: get all the immutable values here.

        } 
    }
    
    //get memtable keys
    if(db_state->db_info->ptr_memtable != NULL)
    {
        for(ptr_skiplist_node node = db_state->db_info->ptr_memtable->skiplist->head->forward[0]; 
                node != db_state->db_info->ptr_memtable->skiplist->nil; 
                node = node->forward[0]) {
            //get keys
            v0.push_back(node->key);
            //get values
            //TODO: get all the immutable values here.
        }
    }

    //get Level-0 keys 
    std::list<block_index_node> **  block_index_list_level = db_state->db_info->list_of_block_index_node;
    for(int cursor = db_state->db_info->level0_info.cursor_start; cursor <= db_state->db_info->level0_info.cursor_end; cursor++)
    {
        if(block_index_list_level[0][cursor].empty())
            continue;
        for(std::list<block_index_node>::iterator iter = block_index_list_level[0][cursor].begin();
                        iter != block_index_list_level[0][cursor].end();
                        iter++) {
            std::list<internal_key_value> list_internal_key_value;
            parse_block2kv(db_state, &(*iter), &list_internal_key_value);
            //add keys to v0 set.
            for(std::list<internal_key_value>::iterator iter = list_internal_key_value.begin();
                    iter != list_internal_key_value.end(); 
                    iter++) {
                slice key;
                internalkey ikey = iter->key;
                std::string value = iter->value; //TODO: need to handle values.
                key.data = (char*)malloc(ikey.i_key.size() - 8 + 1);
                key.size = ikey.i_key.size() - 8;
                memcpy(key.data, ikey.i_key.c_str(), key.size);
                //push into v0 set. //Tobe continued.!!!!!
                v0->push_back(key);
            }
        }
    }  
    
    //sort
    std::sort(v0.begin(), v0.end(), sort_slice_compare);
        
    //dedup
    deduplicate(&v0);
    return v0;
}   

//called by mulit-thread 
//int process_block_index_node(Db_state* db_state, block_index_node* block_node, std::vector<slice>* v0, std::vector<slice>* v0_update_set) {
int process_block_index_node(void* args_) {

    thread_handle_args* args = (thread_handle_args*)(args_);
    Db_state* db_state = args->db_state;
    block_index_node* block_node = args->block_node;
    std::vector<slice>* v0 = args->v0;
    std::vector<slice>* v0_update_set = args->v0_update_set;

	std::vector<slice> vc;
	std::vector<slice> vs;

	bool ret = is_need_to_scan(block_node, v0, &vc, &vs);

	if(!ret) 
		return -1;
	
	char* block_buf = (char*)malloc(block_node->size + 1); //free in caller function.
    std::list<internal_key_value> p_list_internal_key_value;
	parse_block2kv(db_state, block_node, &p_list_internal_key_value);

	// TODO:  scan data can be processed here. data is in p_list_internal_key_value
	printf("block node is going to scan. id is: %llu ==============\n", block_node->block_id);
	
	//update v0. For futher scan operation
	//generate an bloomfilter for vc, accerelate speed to update v0.
	// TODO: need to reconsider the value=> bits_per_key
	size_t vs_bits_per_key = 10;
	std::string vs_bloomfilter;
	generate_filter(vs, vs.size(), vs_bits_per_key, &vs_bloomfilter);
	
	update_v0(v0_update_set, &vs_bloomfilter, &p_list_internal_key_value);

	free(block_buf);
	block_buf = NULL;

	return 0;
	
}

