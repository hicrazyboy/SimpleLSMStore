/*************************************************
  Copyright (C), 2013,  ict.ac
  File name:     
  Author:       Version:        Date: 
  Description:    // 真真真真真真真真真真真真
                  // 真真真真真真真真真真真真
                  // 真真真真真真?  Others:         // 真真真?  Function List:  // 真真真真真真真真真真真真
    1. ....
  History:        // 真真真真真真真真真真真真?                  // 真真真真
    1. Date:
       Author:
       Modification:
    2. ...
*************************************************/
#ifndef STORAGE_H_
#define STORAGE_H_

#include "format.h"
//#include "error.h"
//#include "port.h"
#include <map>
#include <stdint.h>

/*
 *  yueyinliang/2013/11/14: design notes
 *  mapping sstable to logical address space, and mapping blocks to physical address space, 
 *  note that the logical address of blocks are determined by the [sstable logical addrss] and [offset of block in sstable]
 *  however, the physical addrss of blocks can be changed according to the "block movement" schemes
 *  blocks can be organized into RAID0/1/5/6/10 or other innovative manner

 *  we only record the mapping of blocks from logical to physical,

 *  two layer mapping: sstable to logical  blocks (only located in block index), logical block to physical block
 */

//define physical_addr
struct physical_addr_
{
    uint32_t dev_id;//disk id[support multi disk.]
    uint64_t offset;//offset of sstable in device //2013/12/1 zxx: offset of block in disk.
    uint32_t length;//length of block.
};
typedef physical_addr_ Physical_addr;

//yueyinliang/2013/11/14: mapping blocks to physical space
struct log2phy_
{
    uint64_t block_id; //block logical address in ZKV, including [sstable logical address] and [offset of block in sstable]
    Physical_addr phy_addr; //one logical block, one physical block
};
typedef struct log2phy_  Log2phy;

//defined at 2013/12/1. suggest to use as an member of Db_state.
//@logical_addr_bitmap
//each bit in bitmap marks the status of logical address allocation
// '1' means  used
// '0' means unused
//size should be [logical_bitmap_size + 1],  init in function [init_logical_addr_bitmap()]
//@physical_addr_bitmap
//each bit in bitmap marks the status of physical address allocation
// '1' means  used
// '0' means unused
//size should be [physical_bitmap_size + 1],  init in function [init_physical_addr_bitmap()]
struct storage_info_ {
	uint32_t *logical_addr_bitmap;
	uint32_t *physical_addr_bitmap;
	//record the last allocated position bit in bitmap
	uint64_t last_pos_of_logical_addr_bitmap;
	uint64_t last_pos_of_physical_addr_bitmap;

	uint64_t logical_bitmap_size;
	uint64_t physical_bitmap_size;
	//mapping blockid(logical, ==> sstid | offset) to physical address.
	std::map<uint64_t, Physical_addr>* map_l2p;

	//pthread_mutex_t storage_mutex;
	
	int fd; //need to init when system starts.
	bool is_first_time_allocate_logical;
	bool is_first_time_allocate_physical;
};
typedef struct storage_info_ Storage_info;

bool test_bit(uint32_t* bitmap_array, uint64_t i);

bool set_bit(uint32_t* bitmap_array, uint64_t i);

bool clr_bit(uint32_t* bitmap_array, uint64_t i);

//initialize storageinfo
status init_storage(Storage_info* storageinfo);

/* 
 * yueyinliang/2013/11/14: 
 * allocate an sstable storage space, return the sst_id [sstable logical offset]
 * we remove the  <flag: seq>, because I  think the 2MB/4MB sstable storage space must be sequential. 
 * we do not map the logical block to physical block in this stage, we alloc block only when a block is used. 
 * because we do not exactly know the length of each block until the block is filled up by kv. 
*/
//changed by zxx @2013/12/1
//interface for outside function call to allocate an sstable
status alloc_sst(Storage_info* storageinfo, uint64_t* sst_id);


//yueyinliang/2013/11/14: alloc physical storage space for a block, in this stage, we should know the exact length of this block.
//used when you try to flush the block to disks 
//changed by zxx @2013/12/1
status alloc_block(Storage_info* storageinfo, uint64_t block_id, size_t block_len);

//yueyinliang/2013/11/14: building the mapping from block id to block physical address
//question: must build another table to record this mapping information. 
//changed by zxx @2013/12/1

status get_logical_addr_from_bitmap(Storage_info* storageinfo, uint64_t* sst_id);

status get_physical_addr_from_bitmap(Storage_info* storageinfo, Physical_addr* p_phy_addr);

status free_sstable(Storage_info* storageinfo, uint64_t sst_id);

status logical_read(const Storage_info* storageinfo, const uint64_t block_id, char* buf, const size_t size);

status logical_write(const Storage_info* storageinfo, const uint64_t block_id, const char* buf, const size_t size);

status mapping_block_l2p(Storage_info* storageinfo, uint64_t block_id, Physical_addr phy_addr);

status unmapping_block_l2p(Storage_info* storageinfo, uint64_t block_id);
#endif //STORAGE_H_

