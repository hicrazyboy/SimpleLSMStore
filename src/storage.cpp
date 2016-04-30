/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: block.cpp
  Author:        Version :          Date:
  Description: for 
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/

#define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

//#include "port.h"
#include "storage.h"
#include "format.h"
#include "util.h"
#include "log.h"

#include <iterator>
#include <map>
#include <string.h>
#include <math.h>
//#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h>

//return 1 if bit is 1; return 0 if bit is 0
bool test_bit(uint32_t* bitmap_array, uint64_t i) {
	return (bitmap_array[i >> 5] & (1 << (i & 0x1F))) != 0? true:false;
}

//set 0 to 1
bool set_bit(uint32_t* bitmap_array, uint64_t i) {
	bitmap_array[i >> 5] |= (1 << (i & 0x1F));
	return true;
}

//set 1 to 0
bool clr_bit(uint32_t* bitmap_array, uint64_t i) {
	bitmap_array[i >> 5] &= ~(1 << (i & 0x1F));
	return true;
}

//initialize
status init_storage(Storage_info* storageinfo) {
	//calculate logical_bitmap_size
	assert((MAX_SST_SIZE_BYTE & (MAX_SST_SIZE_BYTE-1)) == 0);
	uint32_t sst_bits = log2_int(MAX_SST_SIZE_BYTE);//log(MAX_BLOCK_SIZE_BYTE) / log(2);
	storageinfo->logical_bitmap_size = (LOGICAL_ADDR_UPPER_LIMIT >> (sst_bits+5)) + 1;//LOGICAL_ADDR_UPPER_LIMIT / (MAX_SST_SIZE_BYTE*32);
	if( LOGICAL_ADDR_UPPER_LIMIT % (MAX_SST_SIZE_BYTE * 32) == 0 ) {
		storageinfo->logical_bitmap_size--;
	}
	
	//calcalate physical bitmap size
	assert((MAX_BLOCK_SIZE_BYTE & (MAX_BLOCK_SIZE_BYTE-1)) == 0);
	uint32_t block_bits = log2_int(MAX_BLOCK_SIZE_BYTE);//log(MAX_BLOCK_SIZE_BYTE) / log(2);
	storageinfo->physical_bitmap_size = (PHYSICAL_ADDR_UPPER_LIMIT >> (block_bits+5)) + 1;//physical_addr_upper_limit / (block_size*32);
	if(PHYSICAL_ADDR_UPPER_LIMIT % (MAX_BLOCK_SIZE_BYTE * 32) == 0) {
		storageinfo->physical_bitmap_size--;
	}

	//init last_pos_of_logical_addr_bitmap and last_pos_of_physical_addr_bitmap
	storageinfo->last_pos_of_logical_addr_bitmap = 0;
	storageinfo->last_pos_of_physical_addr_bitmap = 0;

	storageinfo->is_first_time_allocate_logical = true;
	storageinfo->is_first_time_allocate_physical = true;

	//init mutex
	//pthread_mutex_init(&(storageinfo->storage_mutex), NULL);

	//init logical_addr_bitmap
	storageinfo->logical_addr_bitmap = (uint32_t*) malloc (sizeof(uint32_t) * (storageinfo->logical_bitmap_size));
	if(storageinfo->logical_addr_bitmap == NULL) {
		return e_Corruption;
	}  
	memset(storageinfo->logical_addr_bitmap, 0x00, sizeof(uint32_t) * (storageinfo->logical_bitmap_size));

	//init physical_addr_bitmap
	storageinfo->physical_addr_bitmap = (uint32_t*) malloc (sizeof(uint32_t) * (storageinfo->physical_bitmap_size));
	if(storageinfo->physical_addr_bitmap == NULL) {
		return e_Corruption;
	}  
	memset(storageinfo->physical_addr_bitmap, 0x00, sizeof(uint32_t) * (storageinfo->physical_bitmap_size));
	
	return e_Ok;
}


//interface for outside function call to allocate an sstable
status alloc_sst(Storage_info* storageinfo, uint64_t* sst_id) {
	return get_logical_addr_from_bitmap(storageinfo, sst_id);
}

// block_id ==> sst_id | offset_in_sstable
//old definiton: bool alloc_block(uint64_t block_id, size_t block_len, physical_addr* p_phy_addr) {
//new definition:  by zxx@ 2013/12/1
status alloc_block(Storage_info* storageinfo, uint64_t block_id, size_t block_len) {
	Physical_addr phy_addr;
	get_physical_addr_from_bitmap(storageinfo, &phy_addr);
	//map logical to physical
	mapping_block_l2p(storageinfo, block_id, phy_addr);
	return e_Ok;
}


//find appropriate sst_id. and return if it is ok.
status get_logical_addr_from_bitmap(Storage_info* storageinfo, uint64_t* sst_id) {
//	storageinfo->storage_mutex.lock();
	//!!!search strategy:!!!
	// 1.0 search from the end of latest search, use the first sst physical addr which is unused.
	uint32_t sst_bits = log2_int(MAX_SST_SIZE_BYTE);
	uint64_t i_num = (LOGICAL_ADDR_UPPER_LIMIT >> sst_bits);
	if(LOGICAL_ADDR_UPPER_LIMIT % MAX_SST_SIZE_BYTE != 0) {
		i_num++;
	}
	uint64_t i;
	if(storageinfo->is_first_time_allocate_logical) {
		storageinfo->is_first_time_allocate_logical = false;
		i = 0;
	}
	else{
		i = (storageinfo->last_pos_of_logical_addr_bitmap + 1) % (i_num);
	}
//	print_bitmap(storageinfo->logical_addr_bitmap, i_num);
	for(; i < i_num; i++) {
		if(test_bit(storageinfo->logical_addr_bitmap, i) == false) {
			storageinfo->last_pos_of_logical_addr_bitmap = i;
			// 2.0 set the corresponding bit to '1' 
			set_bit(storageinfo->logical_addr_bitmap, i);
			*sst_id = MAX_SST_SIZE_BYTE * i;
			//storageinfo->storage_mutex.unlock();
			return e_Ok;
		}
	}
//	storageinfo->storage_mutex.unlock();
	//if enter here, no space for bitmap is unused.
	return e_NotFound;
}


status get_physical_addr_from_bitmap(Storage_info* storageinfo, Physical_addr* p_phy_addr) {
	
	//!!!search strategy:!!!
	// 1.0 search from the end of latest search, use the first block physical addr which is unused.
	uint32_t block_bits = log2_int(MAX_BLOCK_SIZE_BYTE);
	uint64_t i_num = (PHYSICAL_ADDR_UPPER_LIMIT >> block_bits);
	if(PHYSICAL_ADDR_UPPER_LIMIT % MAX_BLOCK_SIZE_BYTE != 0) {
		i_num++;
	}
	uint64_t i;
	if(storageinfo->is_first_time_allocate_physical) {
		storageinfo->is_first_time_allocate_physical = false;
		i = 0;
	}
	else{
		i = (storageinfo->last_pos_of_physical_addr_bitmap + 1) % (i_num);
	}
//	print_bitmap(storageinfo->physical_addr_bitmap, i_num);
	for(; i < i_num; i++) {
		if(test_bit(storageinfo->physical_addr_bitmap, i) == false) {
		    storageinfo->last_pos_of_physical_addr_bitmap = i;
//			cout << "last_pos_of_physical_addr_bitmap is : " << storageinfo->last_pos_of_physical_addr_bitmap << endl;
			// 2.0 set the corresponding bit to '1' 
			set_bit(storageinfo->physical_addr_bitmap, i);
			p_phy_addr->dev_id = 0; //TODO: temp value. if extends disk, reconsider. 2013/12/1
			p_phy_addr->offset = MAX_BLOCK_SIZE_BYTE * i;
			p_phy_addr->length = MAX_BLOCK_SIZE_BYTE;
			return e_Ok;
		}
	}
	//if enter here, no space for bitmap is unused.
	return e_NotFound;
}


//1.0 clr logical_bitmap
//2.0 clr physical bitmap [note: need to calculate all the block_id related with sst_id] 
//3.0 unmap block_l2p for each block_id
status free_sstable(Storage_info* storageinfo, uint64_t sst_id) {
	//1.0 clr logical_bitmap
	uint64_t pos = sst_id / MAX_SST_SIZE_BYTE;
	clr_bit(storageinfo->logical_addr_bitmap, pos);
	
	uint32_t offset = MAX_SST_SIZE_BYTE / MAX_BLOCK_SIZE_BYTE;
	for(int i = 0; i < offset; i++) {
		//get all blockid related to sst_id
		uint64_t tmp_blockid = sst_id | (i*MAX_BLOCK_SIZE_BYTE);
		std::map<uint64_t, Physical_addr>::iterator iter = storageinfo->map_l2p->find(tmp_blockid);
		//2.0 clr physical bitmap
		//clear the related bits in physical_addr_map if found.
		if(iter != storageinfo->map_l2p->end()) {
			uint64_t pos = (iter->second.offset) / MAX_BLOCK_SIZE_BYTE;
			clr_bit(storageinfo->physical_addr_bitmap, pos);
			//3.0 unmap block_l2p
			unmapping_block_l2p(storageinfo, tmp_blockid);
		}
		else {
			//just not found, it does not matter.
		}
	}
	return e_Ok;
}

//[GL] modified
status logical_read(const Storage_info* storageinfo, const uint64_t block_id, char* buf, const size_t size) {
    status s = e_NotFound;
    std::map<uint64_t, Physical_addr>::iterator iter = storageinfo->map_l2p->find(block_id);
    if( iter != storageinfo->map_l2p->end() ) {
        //[GL] modified
        #if 1
        lseek(storageinfo->fd, iter->second.offset, SEEK_SET);
        int ret = read(storageinfo->fd, buf, size);
        #else
        ssize_t ret = pread(storageinfo->fd, buf, size, iter->second.offset);
        #endif
#ifdef VIEW_DISK
Log(file_log, " %d  %lld  %d %d", iter->second.dev_id, iter->second.offset/4096, 1, 1);
#endif
        //	cout << "ret: " << ret <<endl;
        if(ret == -1) {
            s = e_IOError;            
Log(file_log, "iter->second.offset = %d, ret = %d error: %s --line %d\n", iter->second.offset, ret, strerror(errno), __LINE__);
        } else if(ret < size){
            s = e_Corruption;
Log(file_log, "iter->second.offset = %d, ret = %d --line %d\n", iter->second.offset, ret, __LINE__);
        } else {
            s = e_Ok;
        }
    }
    return s;
}

//[GL] modified
status logical_write(const Storage_info* storageinfo, const uint64_t block_id, const char* buf, const size_t size) {
    status s = e_NotFound;
    std::map<uint64_t, Physical_addr>::iterator iter = storageinfo->map_l2p->find(block_id);
    if( iter != storageinfo->map_l2p->end() ) {
    #if 0
    lseek(storageinfo->fd, iter->second.offset, SEEK_SET);
    int ret = write(storageinfo->fd, buf, size);
    fdatasync(storageinfo->fd);
    #else
    ssize_t ret = pwrite(storageinfo->fd, buf, size, iter->second.offset);
    fdatasync(storageinfo->fd);
    #endif
#ifdef VIEW_DISK
//¡¾42663398.775600¡¿ ¡¾device_id¡¿0 ¡¾524367 8¡¿ 0£¨1£ºread 0£ºwrite£©
Log(file_log, " %d  %lld  %d %d", iter->second.dev_id, iter->second.offset/4096, 1, 0);
//Log("[write] %dbytes ok ;  device id:%d ; offset: %lld ; size: %dbypes .", ret, iter->second.dev_id, iter->second.offset, size);
#endif
        if(ret == -1) {
Log(file_log, "iter->second.offset = %d, ret = %d error: %s --line %d\n", iter->second.offset, ret, strerror(errno), __LINE__);
            s = e_IOError;
        } else if(ret < size){
Log(file_log, "iter->second.offset = %d, ret = %d --line %d\n", iter->second.offset, ret, __LINE__);
            s = e_Corruption;
        } else {
            s = e_Ok;
        }
    }
    return s;
}
#if 0
//@zxx deleted at 2013/12/1
//NOTE: no need for this function, since if do like this, how to 
//deal with the entire sstable which other blocks are still used.
status free_block(uint64_t block_id) {
	// 1.0 get map (global or by argument)
	map_12p
	// 2.0 erase map item if found
	std::map<uint64_t, Physical_addr>::iterator iter = map_l2p->find(block_id);
	if(iter != map_12p.end()) {
		map_12p->erase(iter);
		return e_Ok;
	}
	else {
		return e_NotFound;
	}
}
#endif

status mapping_block_l2p(Storage_info* storageinfo, uint64_t block_id, Physical_addr phy_addr) {
	//storageinfo->storage_mutex.lock();
	std::map<uint64_t, Physical_addr>::iterator iter = storageinfo->map_l2p->find(block_id);
	//clear old value if exists.
	if(iter != storageinfo->map_l2p->end()) {
		storageinfo->map_l2p->erase(iter);
	}
	storageinfo->map_l2p->insert(std::pair<uint64_t, Physical_addr>(block_id, phy_addr));
	//storageinfo->storage_mutex.unlock();
	return e_Ok;
}


status unmapping_block_l2p(Storage_info* storageinfo, uint64_t block_id) {
	//storageinfo->storage_mutex.lock();
	std::map<uint64_t, Physical_addr>::iterator iter = storageinfo->map_l2p->find(block_id);
	if(iter != storageinfo->map_l2p->end()) {
		storageinfo->map_l2p->erase(iter);
	//	storageinfo->storage_mutex.unlock();
		return e_Ok;
	}
	else {
	//	storageinfo->storage_mutex.unlock();
		return e_NotFound;
	}
}


