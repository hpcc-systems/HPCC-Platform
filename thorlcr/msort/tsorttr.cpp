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
