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



#include "stdafx.h"

#include "util.hpp"


#define INIFILE                 ".\\treeview.ini"




void showFIOErr(LPCSTR fname, bool open)
{
    char msg[384], lastErrTxt[256];
    if(GetLastError())  
        FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, GetLastError(), 0, lastErrTxt, sizeof(lastErrTxt), NULL);   
    else
        lastErrTxt[0] = NULL;

    if(open)
        sprintf(msg, "Unable to load %s.\n%s", fname, lastErrTxt);
    else
        sprintf(msg, "Unable to save to %s.\n%s", fname, lastErrTxt);

    MessageBox(NULL, msg, "File Open Error", MB_OK | MB_ICONWARNING);
}





void putProfile(LPCSTR section, LPCSTR key, LPCSTR value)
{
    WritePrivateProfileString(section, key, value, INIFILE);
}

void putProfile(LPCSTR section, LPCSTR key, int value)
{
    char buf[16];
    itoa(value, buf, 10);
    putProfile(section, key, buf);
}

LPCSTR getProfileStr(LPCSTR section, LPCSTR key)
{
    static char buffer[256];

    DWORD b = GetPrivateProfileString(section, key, "0", buffer, sizeof(buffer), INIFILE);

    buffer[b] = 0;
    return buffer;
}

int getProfileInt(LPCSTR section, LPCSTR key)
{
    return atoi(getProfileStr(section, key));
}




void toEp(SocketEndpoint & ep, LPCSTR epTxt)
{
    char * tmp = static_cast <char *> (alloca(strlen(epTxt) + 1));
    strcpy(tmp, epTxt);
    for(unsigned i = (unsigned)strlen(tmp); i >= 0; i--)       
    {
        if(*(tmp + i) == ':')
        {   
            ep.port = atoi(tmp + i + 1);
            *(tmp + i) = 0;
            ep.ipset(tmp);              
            break;
        }
    }
}




void reportException(IException * e)
{
    StringBuffer eMsg;
    e->errorMessage(eMsg);
    CString msg("An error occured whilst attempting connection:\n");
    msg += eMsg.str();
    msg += ".";
    MessageBox(NULL, msg, "Connection Error", MB_OK | MB_ICONEXCLAMATION);
    e->Release();
}

