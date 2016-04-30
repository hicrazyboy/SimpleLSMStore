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
