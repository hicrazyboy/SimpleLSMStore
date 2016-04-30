
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

#ifndef SKIPLIST_H_
#define SKIPLIST_H_

#include "format.h"
//#include "port.h"
#include "util.h"
#include <stdint.h>


#define MAX_SKIPLIST_HEIGHT 12

#if 0
struct key_in_skiplist_
{
	uint32_t key_size;
	uint32_t value_size;
	//...more if needed
	//just for convinence of get the size
}
typedef struct key_in_skiplist_ key_in_skiplist;
#endif

struct skiplist_node_ 
{
    slice key;  //internal_key
	slice value;
	struct skiplist_node_* forward[1];
};
typedef struct skiplist_node_ Skiplist_node, *ptr_skiplist_node;

struct skiplist_
{
	int curr_max_height; //max height in current skiplist
	ptr_skiplist_node head;
	ptr_skiplist_node nil;
};
typedef struct skiplist_ Skiplist;

ptr_skiplist_node new_skiplist_node(slice key, slice value, uint32_t height);

bool key_is_after_node(slice key, Skiplist_node* next, comparator cmp);

Skiplist_node* find_greater_or_equal(Skiplist* skiplist, slice key, Skiplist_node** prev, comparator cmp);

uint32_t random_next(uint32_t* seed);

uint32_t random_height();

void init_skiplist(Skiplist* skiplist);

uint32_t skiplist_put(Skiplist* skiplist, slice key, slice value);

// If found, put the value in std::string* value, set s e_Ok, else return s set e_NotFound
status skiplist_get(Skiplist* skiplist, slice user_key, slice& value);


#endif //SKIPLIST_H_

