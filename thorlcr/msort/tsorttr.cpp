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

#include "platform.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#include <process.h>

#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jsocket.hpp"

#include "tsorttr.hpp"

#define MULTI

#ifdef TRACEINTERCEPT
    #define FILEINTERCEPT
    //define HODIAGINTERCEPT // TBD
#endif

#ifdef FILEINTERCEPT

#include "jmutex.hpp"
#include "jsem.hpp"

#define PRINTTIME

class CFileLog: public ILogIntercept
{
public:
    char trcfile[256];
    ILogIntercept *previntercept;
    bool nl;
    unsigned id;

    CFileLog(unsigned _id) { previntercept = NULL; trcfile[0]=0; nl = true; id = _id; }
    ~CFileLog() { if (previntercept) interceptLog(previntercept); }

    virtual unsigned setLogDetail(unsigned)
    {
        return 0;
    }

    virtual void print(const char *str)
    {
        if (!str||!*str)
            return;
        static CriticalSection sect;
        CriticalBlock proc(sect);
        static bool ateol=true;
        if (trcfile[0]==0) {
#ifdef MULTI
            strcpy(trcfile,"c:\\msortlog.");
#else
            strcpy(trcfile,"msortlog.");
#endif
            itoa(id/256,trcfile+strlen(trcfile),10);
            strcat(trcfile,".");
            itoa(id%256,trcfile+strlen(trcfile),10);
        }
        int f;
        static int started=0;
        if (started)
            f = _open(trcfile,_O_WRONLY | _O_APPEND | _O_BINARY);
        else {
            f = _open(trcfile,_O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY , _S_IREAD|_S_IWRITE);
            started = 1;
        }
#ifdef PRINTTIME
        if (nl) {
            char timeStamp[32];
            time_t tNow;
            time(&tNow);
        #ifdef _WIN32
            struct tm *ltNow;
            ltNow = localtime(&tNow);
            strftime(timeStamp, 32, "%H:%M:%S: ", ltNow);
        #else
            struct tm ltNow;
            localtime_r(&tNow, &ltNow);
            strftime(timeStamp, 32, "%H:%M:%S: ", &ltNow);
        #endif
            write(f,timeStamp,strlen(timeStamp));
        }
#endif
        size32_t l = strlen(str);
        write(f,str,l);
        nl = (str[l-1]=='\n');
        ::close(f);
    }
    virtual void close()
    {
    }
    void open()
    {
        if (previntercept) interceptLog(previntercept); 
        previntercept = interceptLog(this); 
    }

} *FileLogServer=NULL;

void ClosedownMSortTrace()
{
    if (FileLogServer) {
        FileLogServer->close();
        delete FileLogServer;
        FileLogServer=NULL;
    }
}

void InitMSortTrace(SocketEndpoint &ep,unsigned id)
{
    // diag must run on same machine as master
    ClosedownMSortTrace();
    FileLogServer = new CFileLog(id?id:((unsigned)ep.ip.ip[3]+ep.ip.ip[2]*256));
    FileLogServer->open();
}



#else
void InitMSortTrace(SocketEndpoint &ep,unsigned id)
{
}

void ClosedownMSortTrace()
{
}
#endif
