/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: format.cpp
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
#include "util.h"

#include <string.h>
#include <stdlib.h>
#include <string>
#include <assert.h>
#include <stdint.h>

char * msg_status[6] = {
"Ok", "NotFound ", "Corruption", "NotSupported", "InvalidArgument", "IOError"
};

bool userkey_to_internalkey(slice user_key, ValueType type, uint64_t sequence, internalkey* internal_key)
{
    internal_key->i_key.clear();
//internal_key->i_key.assign(user_key.data);//deleted by zxx @2013/12/05
    internal_key->i_key.assign(user_key.data, user_key.size);
    char a_sequence[8];
    memset(a_sequence, 0, sizeof(a_sequence));
    uint64_t seq_type = (sequence << 8) | type;
    
    encode_fixed64(a_sequence, seq_type);
    
//    internal_key->i_key.append(a_sequence);//deleted by zxx @2013/12/05
    internal_key->i_key.append(a_sequence, 8);

    return true;
}

//be careful: s_key malloc here and free outside here.
bool copy_internalkey_to_slice(internalkey internal_key, slice* s_key)
{
    s_key->size = internal_key.i_key.size();
    s_key->data = (char *) malloc ( s_key->size + 1);
    memset( s_key->data, 0x00, s_key->size + 1);
    memcpy( s_key->data, internal_key.i_key.data(), s_key->size );
    return true;
}

bool slice_to_internalkey(slice sli, internalkey* internal_key)
{
    internal_key->i_key.clear();
    internal_key->i_key.assign(sli.data, sli.size);
    return true;
}

/*
 *  translate the internalkey to parsed_internalkey
 */
bool parse_internalkey(const internalkey ikey, parsed_internalkey *pkey)
{
    char *a_user_key;
    char *a_seq_type;
    uint64_t seq_type;
    
    a_user_key = (char*)malloc(ikey.i_key.size() - 8);//free at the end of the pkey's life. //GL almost finished free operator.
    a_seq_type = (char*)malloc(8);//free below
    
    ikey.i_key.copy(a_user_key, ikey.i_key.size() - 8, 0);
    pkey->user_key.data = a_user_key;
    pkey->user_key.size = ikey.i_key.size() - 8;
    ikey.i_key.copy(a_seq_type, 8, ikey.i_key.size()-8);
 
    seq_type = decode_fixed64(a_seq_type);
    free(a_seq_type);
    
    pkey->type = (ValueType)(seq_type & 0xFF);
    pkey->seq_num = (uint64_t) (seq_type >> 8);

    return (pkey->type <= e_TypeValue); //similar assert 
}

/*
 *  draw userkey from internalkey
 *  user_key's data malloc before calling this function.
 */
bool draw_userkey_from_internalkey(const internalkey ikey, slice * p_user_key)
{
    assert(p_user_key->data != NULL);
    assert(ikey.i_key.size() == p_user_key->size);
    parsed_internalkey pkey;
    parse_internalkey(ikey, &pkey);
    
    p_user_key->size = pkey.user_key.size;

    memset( p_user_key->data, 0x00, p_user_key->size + 1);
    memcpy( p_user_key->data, pkey.user_key.data, p_user_key->size );
    free(pkey.user_key.data);
    return true;
}

double max_bytes_for_level(int level)
{
    double max_bytes = 2 * 1048576.0;  // Result for both level-0 and level-1
    if(level == MAX_LEVEL - 1) {
        return 1099511627776.0; //1TB
    }
    while (level > 1) {
        max_bytes *= 2; //The growth rate of two adjacent levels
        level--;
    }
    return max_bytes;
}
