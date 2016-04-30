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
