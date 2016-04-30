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
