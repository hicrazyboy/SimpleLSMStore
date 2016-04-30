/*************************************************
  Copyright (C), 2013,  ict.ac
  File name: error.h   
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
