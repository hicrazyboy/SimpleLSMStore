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
#ifndef MEMTABLE_H_
#define MEMTABLE_H_

#include "skiplist.h"
#include "format.h"
//#include "port.h"
#include <stdint.h>



struct memtable_
{
	bool is_imm;        //标记是否为immutable或memtable
	Skiplist *skiplist;	
	size_t curr_memtable_size;
//    pthread_mutex_t mutex; //pthread_mutex_lock(mutex)  pthread_mutex_unlock
};
typedef struct memtable_ Memtable;

void new_memtable(Memtable* memtable);

status memtable_put(Memtable* memtable, slice key, slice value, ValueType type, uint64_t sequence);

status memtable_delete(Memtable* memtable, slice key, slice value, uint64_t &sequence);

status memtable_get(Memtable* memtable, slice key, slice& value);

size_t get_memtable_usage(Memtable* memtable);
#endif //MEMTABLE_H_
