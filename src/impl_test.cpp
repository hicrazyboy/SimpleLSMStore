/*
 * =====================================================================================
 *
 *       Filename:  impl_test.cpp
 *
 *    Description:  Test the implement module
 *
 *        Version:  1.0
 *        Created:  12/10/2013 12:05:51 AM
 *       Revision:  none
 *       Compiler:  g++
 *
 *         Author:  GaoLong 
 *   Organization:  
 *
 * =====================================================================================
 */

#include "impl.h"
#include <iostream>
#include <stdlib.h>
#include <string>

using namespace std;

// ===  FUNCTION  ======================================================================
//         Name:  bg_call
//  Description:  
// =====================================================================================
	void
bg_call ( void* arg)
{
	char *prt = (char *)arg;
	cout<<prt<<endl;
	return;
}		// -----  end of function bg_call  ----- 

// ===  FUNCTION  ======================================================================
//         Name:  test_bg_schedule
//  Description:  
// =====================================================================================
	void
test_bg_schedule ( void * arg )
{
	pthread_mutex_init(&(cq.mu), NULL);
	pthread_cond_init(&(cq.bgsignal), NULL);
	bg_schedule( (void(*)(void*)) bg_call, arg);
	cout<<"parent thread sleep 1 second!"<<endl;
	sleep(10);//waiting for bg_call()
	return;
}		// -----  end of function test_bg_schedule  ----- 

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  test_pthread_log
 *  Description:  
 * =====================================================================================
 */
    void
test_pthread_log ( void )
{
    pthread_log("lock", 0);
    return ;
}       /* -----  end of function test_pthread_log  ----- */

//struct item{
//	void (*function) (void*);
//	void *arg;
//};
//
//typedef void (*fp_type) (void *);

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  main
 *  Description:  
 *  
 * =====================================================================================
 */
int
main ( int argc, char *argv[] )
{
#if 0 	
	test_pthread_log();
	test_bg_schedule((void *)"hello world!");
#endif



    return EXIT_SUCCESS;
}               /* ----------  end of function main  ---------- */

