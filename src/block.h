/*************************************************
  Copyright (C), 2013,  ict.ac
  File name:     
  Author:       Version:        Date: 
  Description:    // ������ϸ˵���˳����ļ���ɵ���Ҫ���ܣ�������ģ��
                  // �����Ľӿڣ����ֵ��ȡֵ��Χ�����弰������Ŀ�
                  // �ơ�˳�򡢶����������ȹ�ϵ
  Others:         // �������ݵ�˵��
  Function List:  // ��Ҫ�����б�ÿ����¼Ӧ���������������ܼ�Ҫ˵��
    1. ....
  History:        // �޸���ʷ��¼�б�ÿ���޸ļ�¼Ӧ�����޸����ڡ��޸�
                  // �߼��޸����ݼ���
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
