/*************************************************
  Copyright (C), 2013,  ict.ac
  File name: error.h   
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

#ifndef ERROR_H_
#define ERROR_H_

//#include "port.h"

#define LINE_MAX 256

extern void err_doit(uint32_t, const char *, va_list);

/* Nonfatal error related to a system call.
 * Print a message and return. */
void err_ret(const char *fmt, ...);

/* Fatal error related to a system call.
 * Print a message and terminate. */
void err_sys(const char *fmt, ...);


/* Fatal error related to a system call.
 * Print a message, dump core, and terminate. */
void err_dump(const char *fmt, ...);

/* Nonfatal error unrelated to a system call.
 * Print a message and return. */
void err_msg(const char *fmt, ...);

/* Fatal error unrelated to a system call.
 * Print a message and terminate. */
void err_quit(const char *fmt, ...);


void err_log(uint32_t err_level, char * format, ...);


#endif //ERROR_H_
