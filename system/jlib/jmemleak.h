/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#ifndef JMEMLEAK_H
#define JMEMLEAH_H


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

