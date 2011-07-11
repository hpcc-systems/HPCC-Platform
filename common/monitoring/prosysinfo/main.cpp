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

#include <stdlib.h>
#include <stdio.h>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#define Li2Double(x) ((double)((x).HighPart) * 4.294967296E9 + (double)((x).LowPart))
typedef union
{
    LONGLONG li;
    FILETIME ft;
} BIGTIME;
class TSpan
{
public:
    int s, m, h, d;
    TSpan(int secs)
    {
        s = secs % 60;
        m = (secs % 3600) / 60;
        h = (secs % 86400) / 3600;
        d = secs / 86400;
    }
};
#endif

int main(int argc, char** argv)
{
#if !defined(_WIN32) && !defined(_WIN64)
    printf("Only Windows OS is supported.\n");
#else
    if(argc < 3)
    {
        printf("usage: %s <dir> <command>\n", argv[0]);
        return -1;
    }

    char path[512];
    sprintf_s(path, 511, "%s\\%s.pid", argv[1], argv[2]);

    DWORD pid = 0;
    FILE* f = NULL;
    fopen_s(&f, path, "r");
    if(!f)
    {
        fprintf(stderr, "Can't open file %s\n", path);
    }
    else
    {   
        char* pidbuf[32];
        int numread = fread(pidbuf, sizeof(char), 31, f);
        if(numread > 0)
        {
            pidbuf[numread] = '\0';
            pid = atoi((const char*)pidbuf);
        }
    }
    if(pid > 0)
    {
        printf("ProcessID: %d\n", pid);
        HANDLE h = OpenProcess(PROCESS_QUERY_INFORMATION, false, pid);
        if(h <= 0)
        {
            fprintf(stderr, "Process %d can't be opened.\n", pid);
            printf("ProcessUpTime: \n");
        }
        else
        {
            //Process elapsed time.
            BIGTIME CreateTime, ExitTime, ElapsedTime, Now;
            FILETIME KernelTime, UserTime;
            GetProcessTimes(h, &CreateTime.ft, &ExitTime.ft, &KernelTime, &UserTime);
            if(ExitTime.li > CreateTime.li)
                ElapsedTime.li = ExitTime.li - CreateTime.li;
            else
            {
                GetSystemTimeAsFileTime(&Now.ft);
                ElapsedTime.li = Now.li - CreateTime.li;
            }
            unsigned elapsedsecs = (unsigned)(ElapsedTime.li/10000000);
            TSpan span(elapsedsecs);
            printf("ProcessUpTime: %d-%02d:%02d:%02d\n", span.d, span.h, span.m, span.s);
        }
    }
    else
    {
        printf("ProcessID: \nProcessUpTime: \n");
    }

    //CPU usage
    BIGTIME idle1, kernel1, user1, idle2, kernel2, user2;
    GetSystemTimes(&idle1.ft, &kernel1.ft, &user1.ft);
    Sleep(1000);
    GetSystemTimes(&idle2.ft, &kernel2.ft, &user2.ft);
    int IdleTime = (int)(idle2.li - idle1.li);
    int TotalTime = (int)((kernel2.li + user2.li) - (kernel1.li + user1.li));
    int idleRate = (int)(100.0 * IdleTime / TotalTime);
    printf("CPU-Idle: %d%%\n", idleRate);

    //Computer uptime
    LARGE_INTEGER ticks, unit;
    QueryPerformanceCounter(&ticks);
    QueryPerformanceFrequency(&unit);
    int secs = (int)(ticks.QuadPart/unit.QuadPart);
    TSpan u((int)secs);
    printf("ComputerUpTime: %d days, %d:%d\n", u.d, u.h, u.m);

    printf("---SpaceUsedAndFree---\n");

    //Physical and virtual memory usage.
    MEMORYSTATUS memstatus;
    GlobalMemoryStatus(&memstatus);
    printf("Physical Memory: %d %d\nVirtual Memory: %d %d\n", 
        (memstatus.dwTotalPhys - memstatus.dwAvailPhys)/1024, memstatus.dwAvailPhys/1024,
        (memstatus.dwTotalVirtual - memstatus.dwAvailVirtual)/1024, memstatus.dwAvailVirtual/1024);

    // Disk Usage
    char        drivePath[] = "?:\\";
    char        driveName;
    for( driveName = 'A'; driveName <= 'Z'; driveName++ ) 
    {
        drivePath[0] = driveName;
        int dtype = GetDriveTypeA(drivePath);
        if(dtype == DRIVE_FIXED || dtype == DRIVE_RAMDISK || dtype == DRIVE_REMOVABLE || dtype == DRIVE_CDROM) 
        {
            ULARGE_INTEGER diskAvailStruct;
            ULARGE_INTEGER diskTotalStruct;
            diskAvailStruct.QuadPart = 0;
            diskTotalStruct.QuadPart = 0;
            GetDiskFreeSpaceExA(drivePath, &diskAvailStruct, &diskTotalStruct, 0);
            double DiskSize = diskTotalStruct.QuadPart / 1024.0; 
            double FreeSize = diskAvailStruct.QuadPart / 1024.0;
            printf("%s: %.0f %.0f\n", drivePath, DiskSize - FreeSize, FreeSize);
        }
    }
#endif
    return 0;
}
