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

#ifndef SCANUTIL_H_
#define SCANUTIL_H_
#include "block.h"
#include "table.h"
#include "util.h"
#include "format.h"
#include "storage.h"
#include "memtable.h"

#include <string>
#include <pthread.h>
#include <deque>
#include <list>
#include <vector>
#include <stdint.h>

#define ACCURACY 0.9
#define FALSE_RATE 0.01


struct thread_handle_args_
{
    Db_state *db_state;
    block_index_node* block_node;
    std::vector<slice>* v0; 
    std::vector<slice>* v0_update_set;
};
typedef thread_handle_args_ thread_handle_args;

uint32_t position_of( const std::vector<slice> &v, const slice key);



#endif //SCANUTIL_H_
