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
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <jencrypt.hpp>

#include "jlib.hpp"
#include "jsem.hpp"
#include "jthread.hpp"

#include "hrpcsock.hpp"
#include "hrpcutil.hpp"
#include "homisc.hpp"

#include "hagent.hpp"
#include "portlist.h"

#ifdef _WIN32
#include <Winsock2.h>
#endif

#include "jsocket.hpp"
#include "hodisp_base.hpp"

#ifdef _WIN32
const char* statcmd = "cmd /c dir c:\\hoagent.txt";
#else
const char* statcmd = "pwd";
#endif

int num_threads=25;

char * unscr(char * s)
{
    int j=0;
    while (s[j])
    {
        if (s[j]==0x55)
            throw_error ("Invalid (upper case U) in username/password");
        s[j++] ^=0x55;
    }
    return s;
}

static int _nn = 1;

class cmd_dispatch : public c_dispatch 
{
public :
    int argc;
    int node_number;
    char ** argv;
    char * name;
    char *next;

    virtual void action();
    
    cmd_dispatch(int _argc,char * _argv[],const char * _name) { argc=_argc; argv=_argv; name=strdup(_name);id=name;node_number=_nn++; next = NULL;};
    
    cmd_dispatch(int _argc,char * _argv[],const char * _name, const char* _next) 
    { 
        argc=_argc; 
        argv=_argv; 
        name=strdup(_name);
        id=name;
        node_number=_nn++; 
        next = strdup(_next);
    };

    ~cmd_dispatch() 
    { 
        free(name);
        if(next)
            free(next);
    };
};

m_dispatch *disp=0;

class split_node : public split_nodes_base_ex 
{
public:
    int argc;
    char ** argv;
    
    virtual void add_machine(const char * s);
    virtual void add_machine_ex(const char * s, const char* nxt);
    split_node(int _argc,char * _argv[] ){ argc=_argc; argv=_argv; };
};

int calltimeout=0;
bool encrypted = false;
unsigned replicationoffset = 1;

void cmd_dispatch::action() 
{
    //  printf("%s\n",name);
    char x[100];
    sprintf(x,"elapsed time : %s",name);
    //  elapsed_time_trace t(x);
    
    IHRPCtransport * transport = MakeTcpTransportFromUrl(name, HOAGENT_PORT);
    hoagent agent;
    agent.UseTransport(transport);
    transport->SetConnectTimeout(calltimeout?(calltimeout*1000):(num_threads==1?600:0));
    transport->SetTimeout(calltimeout?calltimeout:3);
    StringBuffer result;
    result.append(name).appendf("(%d) ",node_number);
    
    if (stricmp(argv[2], "alive") == 0)
        result.append(agent.alive(atoi(argv[3])));
    else if (stricmp(argv[2], "start") == 0)
    {
        StringBuffer cmdbuf;
        for (char *cp=argv[3]; *cp; cp++)
        {
            if (*cp == '%' && *(cp+1))
            {
                cp++;
                switch (*cp)
                {
                case 'n': // Node number
                    cmdbuf.append(node_number);
                    break;
                case 'a': // Node address
                    cmdbuf.append(name);
                    break;
                case 'l': // Node list
                    cmdbuf.append(argv[1]);
                    break;
                case 'x': // Next Node
                    if(next != NULL)
                        cmdbuf.append(next);
                    break;
                default: // treat as literal (?)
                    cmdbuf.append('%').append(*cp);
                    break;
                }
            }
            else
                cmdbuf.append(*cp);
        }
        result.append(agent.start_process(cmdbuf.str()));
    }
    else if (stricmp(argv[2], "stop") == 0)
        result.append(agent.stop_process(atoi(argv[3])));
    else if (stricmp(argv[2], "account") == 0) 
    { 
        transport->SetTimeout(calltimeout?calltimeout:15);
        int cd=25;
        bool success=false;
        while (cd&&!success) {
            char * u=unscr(strdup(argv[3]));
            StringBuffer pw;
            if (encrypted)
                decrypt(pw, argv[4]);
            else
                pw.append(argv[4]);
            char *p = unscr(pw.detach());
            if (cd>1) 
            {
                try 
                {
                    agent.account(u, p, argv[5]);
                    if (agent.start_process(statcmd) )
                        success=true;
                    
                }
                catch (...)
                {
                    
                }
                if (!success)
                {
                    srand(GetCurrentThreadId()+clock());
                    MilliSleep((rand() * 3000) / RAND_MAX);
                }
            }
            else 
            {
                agent.account(u, p, argv[5]);
                if (agent.start_process(statcmd) )
                    success=true;
            }
            
            cd--;
        }
        if (!success) result.append(" failed"); else result.appendf(" ok (retries=%i)",24-cd);
    }
    else if (stricmp(argv[2], "dir") == 0) 
    { 
        transport->SetTimeout(15);
        agent.set_dir(argv[3]);     
    }
    
    
    if (result.length()) {
        printf("%s\n", result.toCharArray());
    }
    transport->Release();
}

void split_node::add_machine(const char *n) 
{
    disp->dispatch(new cmd_dispatch(argc,argv,n));
}

void split_node::add_machine_ex(const char *n, const char *nxt) 
{
    disp->dispatch(new cmd_dispatch(argc,argv,n,nxt));
}

void setoptions(int argc,char * argv[] ) 
{
    calltimeout=0;
    for (int i=1; i<argc; i++) 
    {
        if (argv[i]==stristr(argv[i],"/n"))
        { 
            int c=atoi(&argv[i][2]);
            printf("%i threads\n",c);
            num_threads=c;
        }
        else if (argv[i]==stristr(argv[i],"/t"))
        { 
            calltimeout=atoi(&argv[i][2]);
        }
        else if (argv[i]==stristr(argv[i],"/e"))
        { 
            encrypted=true;
        }
        else if (argv[i]==stristr(argv[i],"/o"))
        {
            replicationoffset = atoi(&argv[i][2]);
        }
    }
    if (num_threads<0) num_threads=1;
    if (num_threads>200) num_threads=200;
    if (!disp) disp=new m_dispatch(num_threads);
}

int main( int argc, char *argv[] )
{ 
    int res=0;
    if (argc < 3)
    {
        printf("frunagent <nodelist> start \"command\" [options] \n"
            "                     stop <commandid> [options]\n"
            "                     account <user> <password> <dir> [option]\n"
            "                     dir <dir> [option]\n"
            "                     alive <integer> [option]\n\n"
            "where <nodelist> is of the form h009100:h009119,h007010:h007020\n"
            "or 192.168.6.100:192.168.6.119,192.168.7.10:192.168.7.20\n"
            "or @filename where filename contains a file in the above format\n"
            "options: /n<number_of_thread> /t<call_time_out> /encrypt /o<replication_offset>\n"
            );
        return 255;
    }

    InitModuleObjects();

    StringBuffer tracepath;
    tracepath.append(".").append(PATHSEPCHAR).append("frunagent.txt");
    settrace(tracepath.str(),false);
    ECHO_TO_CONSOLE=true;
    
    try 
    {
        setoptions(argc,argv);
        split_node x(argc,argv);
        if (argv[1][0]=='@')
        {
            StringBuffer b;
            b.loadFile(argv[1]+1);
            char *finger = (char *) b.str();
            while (*finger)
            {
                if (*finger == '\n') 
                    *finger++ = ';';
                else if (*finger == '#')
                {   
                    while (*finger && *finger != '\n')
                        *finger++ = ' ';
                }
                else
                    finger++;
            }
            x.split_nodes_ex(b.str(),replicationoffset);
        }
        else
            x.split_nodes_ex(argv[1],replicationoffset);
        disp->all_done_ex(false);
    }
    catch(IException *e) 
    { 
        pexception("",e);
        e->Release();
        res=255;
    } 
    catch (...) 
    {
        traceft("Caught unknown exception");
    }
#ifdef _TRACING
    traceflush();
#endif
    if (disp) delete disp;

    return res;
}


