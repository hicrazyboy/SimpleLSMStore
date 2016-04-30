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

#ifndef KV_ITER_H_
#define KV_ITER_H_

#include "impl.h"

#include <list>
#include <stdint.h>

//zxx: para define as below.
status parse_memtable2kv(Memtable *memtable, std::list<internal_key_value>* p_list_internal_key_value);

//yueyinliang/2013/11/14: parse single block into kv in list
//GaoLong /2013/11/21: change the args1 to block_index_node
//push_back to the kv_list instead of replace kv_list
status parse_block2kv(Db_state * state, block_index_node* node, std::list<internal_key_value>* p_list_internal_key_value);
#endif //KV_ITER_H_
