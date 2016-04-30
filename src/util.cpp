/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: util.cpp
  Author:        Version :          Date:
  Description:         
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    13/11/7     1.0     build this moudle
***********************************************************/
    
#include "format.h"
//#include "port.h"
//#include "error.h"
#include "util.h"

#include <string.h>
#include <string>
#include <list>
#include <vector>
#include <assert.h>
#include <math.h>
#include <stdint.h>


void ikey_append_to_vector(const internalkey ikey, std::vector<slice>* tmp_keys_)
{
    slice user_key;
    user_key.size = ikey.i_key.size();
    user_key.data = (char*) malloc (user_key.size);             //Free when the vector is useless!
    draw_userkey_from_internalkey(ikey, &user_key);
    tmp_keys_->push_back(user_key);
    return;
}

//GaoLong finished[2013/11/18 16:54:10] 
//append kv to string buffer:buf
void kv_append_to_buf(const internal_key_value * kv, std::string * buf)
{
    size_t key_size = kv->key.i_key.size();
    size_t value_size = kv->value.size();
    char tmp[4];
    memset(tmp, 0x00, sizeof(tmp));
    encode_fixed32(tmp, key_size);
    buf->append(tmp, 4);
    memset(tmp, 0x00, sizeof(tmp));
    encode_fixed32(tmp, value_size);
    buf->append(tmp, 4);
    const std::string tmp_str = kv->key.i_key + kv->value;
    const char * tmp_c = tmp_str.c_str();
    buf->append(tmp_c, key_size + value_size);
    return;
}


//GaoLong added 
void get_reverse_string(const char * data, size_t size, char * rst) 
{
    for(size_t i = 0; i < size; i++) {
        rst[i] = data[size - i - 1];
    }
    return;
}

// 1.0 get uint64_t form of user_key. (use the first 8B of user_key)
// 2.0 calculate the position
uint32_t hash(const char* data, size_t data_size, uint32_t level_index_num) {
    uint64_t num_data;
    memset(&num_data, 0x00, sizeof(num_data));
    char * rev_data = (char *) malloc (data_size);
    memset(rev_data, 0x00, sizeof(rev_data) );
    get_reverse_string(data, data_size, rev_data); //[GL] added!
    // 1.0
    if (kLittleEndian) {
        if(data_size < 8) {
            memcpy(&num_data, rev_data, data_size);
        } else {
            memcpy(&num_data, rev_data, sizeof(num_data));
        }
    } else {
        if(data_size < 8) {
            char* p_num_data = (char*)(&num_data);
            for(int i = data_size - 1; i >= 0; i--) {
                memcpy(p_num_data, rev_data+i, sizeof(char));
                ++p_num_data;
            }
        } else {
            char* p_num_data = (char*)(&num_data);
            //for(int i = data_size - 1; i >= data_size - 8; i--) {
            for(uint32_t i = data_size - 1; i >= data_size - 8; i--) {
                memcpy(p_num_data, rev_data + i, sizeof(char));
                ++p_num_data;
            }
        }
    }
    // 2.0
    //level_index_num is 2^n
    assert( (level_index_num & (level_index_num - 1)) == 0);
    //uint32_t x = log(level_index_num) / log(2);//GaoLong removed
    uint32_t x = log2_int(level_index_num);
    // position should in [0, level_index_num - 1]
    //uint32_t position = num_data / ((1 << 64) / level_index_num);
    uint64_t position = num_data >> (64 - x);
    return position;
}


void encode_fixed32(char* buf, uint32_t value) {
  if (kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}


uint32_t decode_fixed32(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}


void encode_fixed64(char* buf, uint64_t value) {
  if (kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

uint64_t decode_fixed64(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = decode_fixed32(ptr);
    uint64_t hi = decode_fixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}


int sstid_compare(uint64_t blockid1, uint64_t blockid2) {
    assert( (MAX_SST_SIZE_BYTE & (MAX_SST_SIZE_BYTE - 1)) == 0);
    //uint32_t bits = log(MAX_SST_SIZE_BYTE) / log(2);
    uint32_t bits = log2_int(MAX_SST_SIZE_BYTE);
    if( (blockid1 >> bits) == (blockid2 >> bits) ) {
        return 0;
    }
    else if((blockid1 >> bits) > (blockid2 >> bits)) {
        return 1;
    }
    else {
        return -1;
    }
}

//TODO:GaoLong modified 2013/11/18 20:08:20
#if 0
int32_t slice_userkey_compare(const slice s_a, const slice s_b)
{
    return static_cast<int32_t>( strncmp(s_a.data, s_b.data, MAX_OF_2INT(s_a.size, s_b.size)) );//using <string.h>
}
#endif

int32_t slice_internalkey_compare(const slice s_a, const slice s_b)
{
    slice tmp_a, tmp_b;
    tmp_a.data = s_a.data;
    tmp_a.size = s_a.size - 8;
    
    tmp_b.data = s_b.data;
    tmp_b.size = s_b.size - 8;
    
    int32_t r = slice_userkey_compare(tmp_a, tmp_b);
    if (r == 0) {
        const uint64_t anum = decode_fixed64(s_a.data + s_a.size - 8);
        const uint64_t bnum = decode_fixed64(s_b.data + s_b.size - 8);
        if (anum > bnum) {
            r = -1;
        } else if (anum < bnum) {
            r = +1;
        }
    }
    return r;
}

#if 1
//[GL] modified 2014/1/18
int32_t slice_userkey_compare(const slice s_a, const slice s_b) 
{
    assert(s_a.size == s_b.size);
    const int min_len = (s_a.size < s_b.size) ? s_a.size : s_b.size;
    int r = memcmp(s_a.data, s_b.data, min_len);
    if (r == 0) {
        if (s_a.size < s_b.size) r = -1;
        else if (s_a.size > s_b.size) r = +1;
    }
        return r;
    }
#endif

bool sort_slice_compare(const slice s_a, const slice s_b)
{
    int32_t ret = static_cast<int32_t>( strncmp(s_a.data, s_b.data, MAX_OF_2INT(s_a.size, s_b.size)) );//using <string.h>
    if(ret < 0)
        return true;
    else
        return false;   
}


bool ikv_compare(const internal_key_value ikv_a, const internal_key_value ikv_b)
{
    if (internalkey_compare(ikv_a.key, ikv_b.key) < 0)
        return true;
    else 
        return false;
}

/* GaoLong modified 2013/11/18 20:07:54
 * >0:   Either the value of the first character that does not match is lower in the compared string, or all compared characters match but the compared string is shorter.
 * <0:   Either the value of the first character that does not match is greater in the compared string, or all compared characters match but the compared string is longer.
 * 0:    a=b
 */
int32_t internalkey_compare(const internalkey i_a, const internalkey i_b)
{
    //return i_a.i_key.compare(i_b.i_key);
    parsed_internalkey parsed_i_a, parsed_i_b;
    parse_internalkey(i_a, &parsed_i_a);
    parse_internalkey(i_b, &parsed_i_b);

    int32_t compare_rst = slice_userkey_compare(parsed_i_a.user_key, parsed_i_b.user_key);

    free(parsed_i_a.user_key.data);
    free(parsed_i_b.user_key.data);
    
    if(compare_rst == 0 ) {
        if(parsed_i_a.seq_num > parsed_i_b.seq_num) {
            compare_rst = -1;
        } else if(parsed_i_a.seq_num < parsed_i_b.seq_num) {
            compare_rst = +1;
        }
    }
    return compare_rst;
}


uint64_t total_sst_size(const std::list<sst_index_node*>& ssts) {
    int64_t sum = 0;
    for (std::list<sst_index_node*>::const_iterator iter = ssts.begin(); iter != ssts.end(); ++iter) {
        sum += (*iter)->size;
    }
    return sum;
}

//GaoLong added 2013/12/2
uint64_t total_sst_size2(const std::list<sst_index_node>& ssts) {
    int64_t sum = 0;
    for (std::list<sst_index_node>::const_iterator iter = ssts.begin(); iter != ssts.end(); ++iter) {
        sum += iter->size;
    }
    return sum;
}

//GaoLong added 2013/12/4
//Be assure: The target_num is 2^E by us.
uint32_t log2_int(uint64_t target_num)
{
    assert((target_num & (target_num - 1)) == 0);
    uint32_t exponent = 0;
    for(uint64_t number = target_num; number > 1; number = number >> 1, exponent++)  ;//Calcualte the exponent of base 2.
    return exponent;    
}
