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
#ifndef MEMTABLE_H_
#define MEMTABLE_H_

#include "skiplist.h"
#include "format.h"
//#include "port.h"
#include <stdint.h>



struct memtable_
{
	bool is_imm;        //����Ƿ�Ϊimmutable��memtable
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
