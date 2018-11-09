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

#include "win32.hpp"
#include <stdio.h>
#include <platform.h>

int main(int argc, char** argv)
{
    STARTUPINFO si={sizeof si, 0, NULL};
    PROCESS_INFORMATION pi;

    char* cmd=GetCommandLine();
    while (isspace_char(*cmd)) cmd++;
    if (*cmd=='"') {
        cmd++;
        while (*cmd && *cmd != '"')
            cmd++;
        if (*cmd)
            cmd++;
    } else {
        while (!isspace_char(*cmd)) cmd++;
    }
    while (isspace_char(*cmd)) cmd++;

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