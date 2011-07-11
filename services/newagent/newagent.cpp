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
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h>
#ifdef _WIN32
#include <process.h> 
#include <windows.h> 
#include <tchar.h> 

#endif

#define LOCAL_newagent
#include "nagent.cpp"
#include "homisc.hpp"
#include "hrpc.hpp"
#include "hrpcsock.hpp"
#include "hrpcutil.hpp"

#ifndef _WIN32
char cur_dir[1024];
#endif
extern bool is_debug_session;

#ifdef _WIN32
char * key_name="Software\\Seisint\\newagent\\0.01.00";

void write_key(const char * name, LPBYTE val, UINT len)
{
  HKEY k;
  if (!RegCreateKey(HKEY_LOCAL_MACHINE,key_name,&k))
  { RegSetValueEx(k,name,NULL,REG_BINARY,val,len);
    RegCloseKey(k);
  }
}

BOOL read_key(const char * name,LPBYTE val, DWORD len)
{ HKEY k;
  DWORD typ;
  BOOL res=FALSE;
  typ = REG_BINARY;
  if (!RegOpenKey(HKEY_LOCAL_MACHINE,key_name,&k))
  { res=!RegQueryValueEx(k,name,NULL,&typ,val,&len);
    RegCloseKey(k);
  }
  return res;
}

char * unscr(char * s) { 
    if (s) {
        int j=0;
        while (s[j])
        {
            if (s[j]!=0x55) s[j] ^=0x55; 
            j++;
        }
    }
    return s;
}


//===========================================================================


static HANDLE server_user=0;

#define maped_drive "T:"

static bool drive_maped=false;

void unmap_drive()
{ 
  if (server_user&&drive_maped)
  { logfile("unmap\r\n");
    ImpersonateLoggedOnUser(server_user);
    int ec=WNetCancelConnection2(maped_drive,CONNECT_UPDATE_PROFILE,true);
    if (NO_ERROR!=ec) logfile("WNetCancelConnection2 failed %i\r\n",ec);
    RevertToSelf();
    drive_maped=false;
  }
}


void logonu(HANDLE & server_user,char * user,char * password, int & error_code) {
    char usr[100],srv[100];
    char *us=user,*sr=0;
    item(usr,sizeof(usr),user,"\\\n\t",1,FALSE);
    if (usr[0])  { 
        item(srv,sizeof(srv),user,"\\\n\t",0,FALSE);
        us=usr; sr=srv;
    }
    if (!LogonUser(us,sr,password,LOGON32_LOGON_INTERACTIVE,LOGON32_PROVIDER_DEFAULT,&server_user)) {
        error_code=GetLastError();
        if (sr) 
            logfile("LogonUser failed %s %s %u \r\n",us,sr,error_code);
        else 
            logfile("LogonUser failed %s %u \r\n",us,error_code);   
        server_user=NULL;
    }
}

void logon(int & error_code) {
    char user[256]=" ;103<;01",password[256]=" ;103<;01";
    read_key("01",(unsigned char *)user,sizeof(user));
    read_key("02",(unsigned char *)password,sizeof(password));
    unscr(user);
    unscr(password);
    if (!server_user) logonu(server_user,user,password,error_code);
}



void do_map_drive(int & error_code) {
    unmap_drive();
    char share[256]=" ;103<;01";
    logon(error_code);
    if (server_user){
        ImpersonateLoggedOnUser(server_user);
        if (read_key("03",(unsigned char *)share,sizeof(share))) {
            char * finger=share;
            int bs=4;
            while (*finger&&bs) {
                if (*finger=='\\') bs--;
                if (!bs) *finger=0;
                finger++;
            }
            
            logfile("map %s\r\n",share);            
            char user[256]=" ;103<;01",password[256]=" ;103<;01";
            read_key("01",(unsigned char *)user,sizeof(user));
            read_key("02",(unsigned char *)password,sizeof(password));
            unscr(user);
            unscr(password);
            NETRESOURCE ns={RESOURCE_CONNECTED,RESOURCETYPE_DISK,RESOURCEDISPLAYTYPE_SHARE,0,maped_drive,share,"test",0};
            int ec=WNetAddConnection2(&ns,password,user,0/*CONNECT_UPDATE_PROFILE*/);
            if (NO_ERROR!=ec) {
                error_code=ec;
                logfile("WNetAddConnection2 failed %i %s\r\n",ec,share);
            }
            drive_maped=true;
        } 
        else {
            error_code=267L;
            logfile("directory not defined\r\n");
        }
        RevertToSelf();
    }   
}

#endif

int newagent::account(const char * user,
                      const char * password)
{ int res=0;
#ifdef _WIN32
  if (user)              write_key("01",(LPBYTE)user,strlen((char*)user)+1);
  if (password)          write_key("02",(LPBYTE)password,strlen((char*)password)+1);
  unmap_drive();
  CloseHandle(server_user);
  server_user=0;
  logon(res);
#endif
  return res;
}

int newagent::set_map_dir(char * dir)
{ int res=0;
#ifdef _WIN32
    if (dir) write_key("03",(LPBYTE)dir,strlen((char*)dir)+1);
    do_map_drive(res);
#else
    strcpy(cur_dir, dir);
    char *p, *c_d;
    c_d=cur_dir;
    for (p=dir; p<(dir+strlen(dir)); p++, c_d++)
    {
        if (*p == '\\')
            *p = '/';
        else
            *p = *c_d;
    }
    *p = '\0';
#endif
  logfile("set_map_dir %s\n",dir);
  return res;
}


int newagent::alive(int x)
{ return  x+1; 
}


 
int newagent::start_process(const char *command,int & error_code,const char * local_dir,const char * user,const char * password) {
    error_code=0;
#ifdef _WIN32
    PROCESS_INFORMATION process;
    STARTUPINFO si;
    HANDLE res=0;
    bool ok=false;
    HANDLE other_user=0;
#ifdef _DEBUG
    if (!strcmp(command,"forceerror")) *((int *)0)=5;
#endif
    memset(&si,0,sizeof(si));
    si.cb          = sizeof(si);
    si.lpTitle     = (char*)command;
    si.wShowWindow = SW_SHOWDEFAULT;
    si.dwFlags     = STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_SHOWNORMAL;
    char current_dir[256]=" ;103<;01",c_dir[256];
    if (local_dir&&*local_dir) {
        strcpy(c_dir,local_dir);
    }
    else {    
        if (!is_debug_session&&read_key("03",(unsigned char *)current_dir,sizeof(current_dir))) {
            char * finger=current_dir;
            int bs=4;
            while (*finger&&bs) {
                if (*finger=='\\') bs--;
                finger++;
            }
            if (*finger) sprintf(c_dir,maped_drive"\\%s",finger); else sprintf(c_dir,maped_drive"\\");
        } 
        else 
            strcpy(c_dir,".");
        
    }
    if (is_debug_session) {
        ok=(bool)CreateProcess(NULL,(char*)command,NULL,NULL,TRUE,NORMAL_PRIORITY_CLASS,NULL,c_dir,&si,&process);
        if (!ok) error_code=GetLastError();
    } 
    else {
        if (!local_dir&&!drive_maped) do_map_drive(error_code);
        if(!error_code) {
            HANDLE uh=server_user;
            if (user&&*user) {
                logonu(other_user,unscr((char*)user),unscr((char*)password),error_code);
                uh=other_user;
            }
            if (!error_code) {
                if (!ImpersonateLoggedOnUser(uh)) error_code=GetLastError();
                if (!error_code) {
                    char saved_dir[256];
                    if (!GetCurrentDirectory(sizeof(saved_dir),saved_dir)) {
                        logfile("GetCurrentDirectory failed\r\n");
                        error_code=GetLastError();
                    }
                    else {
                        if (!SetCurrentDirectory(c_dir)) {
                            error_code=GetLastError();
                            if (error_code==2) error_code=267;
                            logfile ("SetCurrentDirectory failed %s %i\r\n",c_dir,error_code);
                        }
                        else {
                            ok=(bool)CreateProcessAsUser( uh,NULL,(char*)command,NULL,NULL,TRUE,NORMAL_PRIORITY_CLASS,NULL,c_dir,&si,&process);
                            if (!ok) error_code=GetLastError();
                            if (ok&&!SetCurrentDirectory(saved_dir))  {
                                logfile("restore current dirctory failed %s\r\n",saved_dir);
                                error_code=GetLastError();
                            }
                        } 
                    }             
                    RevertToSelf();
                }
            }
        }
    }
    if (ok) {
        res=process.hProcess;
        CloseHandle(process.hThread);
        logfile("Process started: %s\r\n",command);
    }
    else {
        logfile("Process failed: %s error(%i) %s\r\n",command,error_code,c_dir);
    }
    if (other_user) CloseHandle(other_user);
    if (ok) 
        return (int)res;
    else
        return 0;
    
#else
    //  if (is_debug_session)
    {
        long res;
        _chdir(cur_dir);
        pid_t pid;
        if (invoke_program(command, res, false, NULL, NULL, &pid))
            return pid;
        printf("invoke_program error %d\n", res);
        error_code=res;
        logfile("Process failed: %s error(%i) %s\r\n",command,error_code,cur_dir);
        /* not sure these should be freed here
        free(user);
        free(password);
        */
        return 0;
    }
#endif
}


int newagent::stop_process(int process)
{
#ifdef _WIN32
  TerminateProcess( (HANDLE) process,-1);
#else
    kill(process, 9); // JCSMOE : both these may leak resources, no?
#endif
  return 0; 
}


HRPCserver *serverp=0;

void RunAgentServer(unsigned port)
{ logfile("agent start %u\r\n",port);
  IHRPCtransport *transport = TryMakeServerTransport(port, "Cannot start NewAgent");
  if (!transport) return;
  HRPCserver server(transport);
  serverp=&server;
  newagent stub;
  server.AttachStub(&stub);
  ListenUntilDead(server,"NewAgent terminated");
  serverp=0;
  transport->Release();
}


void KillAgentServer()
{
#ifdef _WIN32
  unmap_drive();
#endif
  logfile("KillAgentServer called\r\n");
  serverp->Stop();
}
