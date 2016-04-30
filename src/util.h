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

#ifndef UTIL_H_
#define UTIL_H_

#include "format.h"
//#include "error.h"
#include "table.h"

#include <string.h>
#include <string>
#include <list>
#include <vector>
#include <stdint.h>

#define MAX_OF_2INT(a, b) ((a) > (b) ? (a) : (b))

void ikey_append_to_vector(const internalkey ikey, std::vector<slice>* tmp_keys_);

//GaoLong added[2013/11/18 16:54:54]: to append one kv by specified format [key_size|value_size|key|value] to buf
void kv_append_to_buf(const internal_key_value *kv, std::string * buf);

void get_reverse_string(const char * data, size_t size, char * rst);

uint32_t hash(const char* data, size_t n, uint32_t level_index_num);

void encode_fixed32(char* buf, uint32_t value);

uint32_t decode_fixed32(const char* ptr);

void encode_fixed64(char* buf, uint64_t value);

uint64_t decode_fixed64(const char* ptr);

int sstid_compare(uint64_t blockid1, uint64_t blockid2);

int32_t slice_internalkey_compare(const slice s_a, const slice s_b);

//TODO:GaoLong modified 2013/11/18 20:08:20
int32_t slice_userkey_compare(const slice s_a, const slice s_b);

//GaoLong added 2013/11/21 15:12:48
//used for i_kv_list.sort in impl.cpp:merge_input_to_kvlist()
//TODO: how to improve the performance of sort
bool ikv_compare(const internal_key_value ikv_a, const internal_key_value ikv_b);

/* GaoLong modified 2013/11/18 20:07:54
 * >0:   Either the value of the first character that does not match is lower in the compared string, or all compared characters match but the compared string is shorter.
 * <0:   Either the value of the first character that does not match is greater in the compared string, or all compared characters match but the compared string is longer.
 * 0:    a=b
 */
int32_t internalkey_compare(const internalkey i_a, const internalkey i_b);

//GaoLong added[2013/11/25]
uint64_t total_sst_size(const std::list<sst_index_node*>& ssts);

uint64_t total_sst_size2(const std::list<sst_index_node>& ssts);

//2013/12/4
uint32_t log2_int(const uint64_t target_num);
//End added

#endif //UTIL_H_
