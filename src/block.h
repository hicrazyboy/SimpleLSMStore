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

#ifndef BLOCK_H_
#define BLOCK_H_

#include "format.h"
//#include "port.h"
#include "storage.h"
#include "table.h"
//#include "error.h"

#include <list>
#include <stdint.h>
 

struct block_index_node_
{
    //basic parts
    uint64_t block_id;        //[sst_id] | [offset in sstable]  //yueyinliang/2013/11/14: change from logical_addr to block_id, to keep consistence with sst_id.
    //uint64_t logical_addr;     
    size_t size;                //block size: around 4kB
    size_t kv_amount;
    internalkey min_key; 
    internalkey max_key;
    std::string bloom_filter;

    //extended part
    uint32_t frequency;
};
typedef struct block_index_node_ block_index_node;

struct Level0_info_
{
	uint32_t cursor_start;
	uint32_t cursor_end;
};
typedef struct Level0_info_ Level0_info;

uint64_t get_blockid(uint64_t sst_id, const uint32_t curr_sst_size);

uint64_t get_sstid(const uint64_t block_id);

bool internal_key_is_in_range(const internalkey key, const internalkey min_key, const internalkey max_key);

bool user_key_is_in_range(const slice user_key, const internalkey min_key, const internalkey max_key);

bool user_key_is_before_block(const slice user_key, const block_index_node &block);

bool user_key_is_after_block(const slice user_key, const block_index_node &block);

status find_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                        const Level0_info level0_info, 
                                                        const slice min_key, 
                                                        const uint32_t level, 
                                                        block_index_node* p_block_index_node);

//NOTE: Level0_info should be reference since the elem will change in function
status insert_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                            Level0_info* level0_info, 
                                                            const uint32_t level, 
                                                            block_index_node* p_block_index_node);

status delete_l0_block_index_node(std::list<block_index_node> ** block_index_list_level, 
                                                                Level0_info * level0_info,
                                                                std::list<sst_index_node*> inputs_lvl0);

status delete_block_index_node(std::list<block_index_node> ** block_index_list_level, 
									const uint32_t level, 
									const internalkey min_key, 
									const internalkey max_key);

status find_l0_block_index_node_list(std::list<block_index_node> **  block_index_list_level,
                                                                    const Level0_info level0_info, 
                                                                    uint64_t sst_id,
                                                                    std::list<block_index_node*>* block_list);


status find_block_index_node_list(std::list<block_index_node> ** block_index_list_level, 
                                                                const Level0_info level0_info, 
                                                                const internalkey min_key, 
                                                                const internalkey max_key, 
                                                                const uint32_t level, 
                                                                std::list<block_index_node*>* block_list);


#endif //BLOCK_H_
