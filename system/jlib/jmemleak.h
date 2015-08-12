/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */


#ifndef JMEMLEAK_H
#define JMEMLEAK_H


#if (defined WIN32) || (defined _WIN32) || (defined __WIN32__) || (defined WIN64) || (defined _WIN64) || (defined __WIN64__)

#ifdef _DEBUG
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#include <signal.h>
#include <stdio.h>
#include <process.h>


void __cdecl IntHandler(int)
{
#ifndef USING_MPATROL
    // Don't care about memory leaks on a ctrl-Break!
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag &= ~_CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag( tmpFlag );
#endif //USING_MPATROL

    _exit(2);
}


int __init()
{
    signal(SIGINT, IntHandler);
#ifndef USING_MPATROL
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag |= _CRTDBG_LEAK_CHECK_DF;// | _CRTDBG_CHECK_ALWAYS_DF | _CRTDBG_DELAY_FREE_MEM_DF ;
    _CrtSetDbgFlag( tmpFlag );
#endif //USING_MPATROL
    return 0;
}

static int __doinit = __init();


#define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif
#endif




#endif

