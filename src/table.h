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
