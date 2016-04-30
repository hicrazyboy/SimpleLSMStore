/*************************************************
  Copyright (C), 2013,  ict.ac
  File name:     
  Author:       Version:        Date: 
  Description:    // 用于详细说明此程序文件完成的主要功能，与其他模块
定义zkv的存储结构        // 或函数的接口，输出值、取值范围、含义及参数间的控
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

#ifndef FORMAT_H_
#define FORMAT_H_

#include <string.h>
#include <string>
#include <stdint.h>

#define MAX_USERKEY_SIZE_BYTE  8

#define DEVICE_NUM         1
#define MAX_LEVEL           7
#define LEVEL0_SST_NUM      12
#define LEVEL1_INDEX_NUM   8
#define LEVEL2_INDEX_NUM   16
#define LEVEL3_INDEX_NUM   32
#define LEVEL4_INDEX_NUM   64
#define LEVEL5_INDEX_NUM   128
#define LEVEL6_INDEX_NUM   256

//#define LEVEL_INDEX_NUM(level) LEVEL##level##_INDEX_NUM

#define LO_COMPACTION_TRIGGER       4
#define L0_SLOWDOWN_WRITES_TRIGGER  8
#define L0_STOP_WRITES_TRIGGER      12

#define MAX_BLOCK_SIZE_BYTE 16384
#define MAX_SST_SIZE_BYTE                       ( 1ull<<20 )
#define MAX_MEMTABLE_SIZE_BYTE		( 1ull<<21 )//GaoLong added 20131129
#define MAX_GRANDPARENT_OVERLAP_BYTES           ( 10 * MAX_SST_SIZE_BYTE )
#define EXPANDED_COMPACTION_BYTE_SIZE_LIMIT     ( 25 * MAX_SST_SIZE_BYTE )

#define MAX_SEQUENCE_NUMBER ((0x1ull << 56) - 1)
#define MAX_ALLOWED_SEEKS    20

#define KMAXCOMPACTLEVEL 2

//logical bitmap size; bitmap can only support 2^64B  in total.
#define LOGICAL_ADDR_UPPER_LIMIT (DEVICE_NUM * PHYSICAL_ADDR_UPPER_LIMIT)
//maximum disk space limitation; bitmap can only support 2^64B  in total.
#define PHYSICAL_ADDR_UPPER_LIMIT (0x1ull<<40)


static const bool kLittleEndian = true;
static const bool SetBloomFilter = true;


enum ValueType {
    e_TypeDeletion = 0x0,
    e_TypeValue = 0x1
};

enum status{
    e_Ok = 0,
    e_NotFound = 1,
    e_Corruption = 2,
    e_NotSupported = 3,
    e_InvalidArgument = 4,
    e_IOError = 5
};

extern char *msg_status[6];

//data struct in memory
struct slice_
{
    char * data;
    uint32_t size;
};
typedef struct slice_ slice;

struct internalkey_
{
    std::string i_key;  // {user_key} + {sequence<<8 | valuetype(56+8)}
};
typedef struct internalkey_ internalkey;


struct parsed_internalkey_
{
    slice user_key;
    uint64_t seq_num; //sequence number
    ValueType type;
};
typedef struct parsed_internalkey_ parsed_internalkey;

struct internal_key_value_
{
    internalkey key;
    std::string value;
};
typedef struct internal_key_value_ internal_key_value;

typedef int32_t (*comparator) (const slice , const slice);

bool userkey_to_internalkey(slice user_key, ValueType type, uint64_t sequence, internalkey* internal_key);

bool copy_internalkey_to_slice(internalkey internal_key, slice* s_key);

bool slice_to_internalkey(slice sli, internalkey* internal_key);

bool parse_internalkey(const internalkey ikey, parsed_internalkey *pkey);

bool draw_userkey_from_internalkey(const internalkey ikey, slice * user_key);

double max_bytes_for_level(int level);

#endif //FORMAT_H_

