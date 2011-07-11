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
    char cdir[256];
    GetCurrentDirectory(256, cdir);



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
    for(int i = strlen(tmp); i >= 0; i--)       
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
    msg += eMsg.toCharArray();
    msg += ".";
    MessageBox(NULL, msg, "Connection Error", MB_OK | MB_ICONEXCLAMATION);
    e->Release();
}

