/*************************************************
  Copyright (C), 2013,  ict.ac
  File name:     
  Author:       Version:        Date: 
  Description:    // 用于详细说明此程序文件完成的主要功能，与其他模块
                  // 或函数的接口，输出值、取值范围、含义及参数间的控
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

#ifndef PORT_H_
#define PORT_H_

// Define C99 equivalent types.
typedef signed char             int8_t;
typedef signed short            int16_t;
typedef signed int              int32_t;
//typedef signed long long        int64_t; //deleted. To compile under 64bit os.

typedef unsigned char           uint8_t;
typedef unsigned short          uint16_t;
typedef unsigned int            uint32_t;
//typedef unsigned long long      uint64_t; //changed to compile under 64bit os.
//typedef unsigned int            size_t; //changed to compile under 64bit os.
typedef long unsigned int uint64_t;
typedef long unsigned int size_t;

#endif 
