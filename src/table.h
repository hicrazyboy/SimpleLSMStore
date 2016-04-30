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

#ifndef TABLE_H_
#define TABLE_H_

#include "format.h"
//#include "port.h"

#include <list>
#include <stdint.h>

//yueyinliang/2013/11/14: each sstable only has a sst_id, which represented by the sstable logical offset
struct sst_index_node_
{
    //basic parts
    uint64_t sst_id;            //sstable's offset in disk
	size_t size;               //GaoLong change the name from sst_size to size
	internalkey min_key;
	internalkey max_key;
	
	//extended parts
	uint32_t allowed_seeks;     //for needs_compaction
};
typedef struct sst_index_node_ sst_index_node;

status find_sst_index_node(const std::list<sst_index_node>* const sst_index_list_level, 
                                                            const size_t level, 
                                                            const uint64_t sst_id, 
                                                            sst_index_node * sst_node);

status find_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, 
                                                            const size_t level, 
                                                            const internalkey min_key, 
                                                            const internalkey max_key, 
                                                            std::list<sst_index_node*>* p_list_sst_index_node) ;

status delete_l0_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, std::list<sst_index_node*> inputs_lvl0);

status delete_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, 
                                                                const size_t level, 
                                                                const internalkey min_key, 
                                                                const internalkey max_key);


status insert_sst_index_node_list(std::list<sst_index_node>* sst_index_list_level, 
                                                                const size_t level, 
                                                                sst_index_node* p_sst_index_node);

#endif //TABLE_H_
