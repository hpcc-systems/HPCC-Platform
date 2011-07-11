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

#include "win32.hpp"
#include <stdio.h>

int main(int argc, char** argv)
{
    STARTUPINFO si={sizeof si, 0, NULL};
    PROCESS_INFORMATION pi;

    char* cmd=GetCommandLine();
    while (isspace(*cmd)) cmd++;
    if (*cmd=='"') {
        cmd++;
        while (*cmd && *cmd != '"')
            cmd++;
        if (*cmd)
            cmd++;
    } else {
        while (!isspace(*cmd)) cmd++;
    }
    while (isspace(*cmd)) cmd++;

    if(!::CreateProcess(NULL, cmd, NULL, NULL, FALSE, CREATE_NEW_CONSOLE | NORMAL_PRIORITY_CLASS, NULL, NULL, &si, &pi))
    {
        win32::SystemError e("CreateProcess");
        fprintf(stderr,e.GetMessage());
        return -1;
    }

    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    return 0;
}