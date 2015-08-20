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
#include "portlist.h"

#include "jlib.hpp"
#include "jio.hpp"
#include "jlog.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jencrypt.hpp"

#include "rmtssh.hpp"

#ifndef _WIN32
#include <wordexp.h>
#endif

//----------------------------------------------------------------------------

//#define PLINK_USE_CMD

class CFRunSSH: public CInterface, implements IFRunSSH
{


    unsigned numthreads;
    unsigned connecttimeout;
    unsigned attempts;
    StringAttr cmd;
    StringAttr identityfile;
    StringAttr user;
    StringAttr password; // encrypted
    StringAttr workdir;
    StringAttr slavesfile;
    StringArray slaves;
    StringAttr treeroot;
    StringArray replytext;
    UnsignedArray reply;
    UnsignedArray done;
    bool background;
    bool strict;
    bool verbose;
    bool dryrun;
    bool useplink;
    int replicationoffset;
    CriticalSection sect;



    StringBuffer expandCmd(StringBuffer &cmdbuf, unsigned nodenum, unsigned treefrom)
    {
        const char *cp=cmd.get();
        if (!cp)
            return cmdbuf;
        for (; *cp; cp++) {
            if ((*cp=='%') && cp[1]) {
                cp++;
                switch (*cp) {
                case 'n': // Node number
                    cmdbuf.append(nodenum+1);
                    break;
                case 'a': // Node address
                    cmdbuf.append(slaves.item(nodenum));
                    break;
                case 'l': // Node list
                    cmdbuf.append(slavesfile);
                    break;
                case '%': 
                    cmdbuf.append('%');
                    break;
                case 'x': // Next Node
                    cmdbuf.append(slaves.item((nodenum+replicationoffset)%slaves.ordinality()));
                    break;
                case 'c':
                    cmdbuf.append(slaves.ordinality());
                    break;
                case 't': // Tree Node
                    if (treefrom)
                        cmdbuf.append(slaves.item(treefrom-1));
                    else
                        cmdbuf.append(treeroot);
                    break;
                case 's': { // ssh params
                        bool usepssh = !password.isEmpty();
                        cmdbuf.appendf("%s -o LogLevel=QUIET -o StrictHostKeyChecking=%s -o BatchMode=yes ",usepssh?"pssh":"ssh",strict?"yes":"no");
                        if (!identityfile.isEmpty())
                            cmdbuf.appendf("-i %s ",identityfile.get());
                        if (background)
                            cmdbuf.append("-f ");
                        if (connecttimeout)
                            cmdbuf.appendf("-o ConnectTimeout=%d ",connecttimeout);
                        if (attempts)
                            cmdbuf.appendf("-o ConnectionAttempts=%d ",attempts);
                        if (!user.isEmpty())
                            cmdbuf.appendf("-l %s ",user.get());
                    }
                    break;
                default: // treat as literal (?)
                    cmdbuf.append('%').append(*cp);
                    break;
                }
            }
            else
                cmdbuf.append(*cp);
        }
        return cmdbuf;
    }

    void loadSlaves()
    {
        FILE *slavesFile  = fopen(slavesfile.get(), "rt");
        if( !slavesFile) {
            const char * s = slavesfile.get();
            while (*s&&(isdigit(*s)||(*s=='.')||(*s==',')||(*s==':')||(*s=='-')||(*s=='*')))
                s++;
            if (!*s) {
                SocketEndpointArray sa;
                sa.fromText(slavesfile.get(),0);
                if (sa.ordinality()) {
                    StringBuffer ns;
                    ForEachItemIn(i,sa) {
                        sa.item(i).getIpText(ns.clear());
                        slaves.append(ns.str());
                    }
                    return;
                }
            }
            throw MakeStringException(-1, "Failed to open slaves file %s", slavesfile.get());
        }
        char inbuf[1000];
        StringAttr slave;
        while (fgets( inbuf, sizeof(inbuf), slavesFile)) {
            char *hash = strchr(inbuf, '#');
            if (hash)
                *hash = 0;
            char *finger = inbuf;
            loop {
                while (isspace(*finger))
                    finger++;
                char *start = finger;
                while (*finger && !isspace(*finger))
                    finger++;
                if (finger > start) {
                    slave.set(start, finger - start);
                    slaves.append(slave);
                }
                else
                    break;
            }
        }
        fclose(slavesFile);
    }
public:
    IMPLEMENT_IINTERFACE;

    CFRunSSH()
    {
        numthreads = 5;
        connecttimeout = 0; // no timeout
        attempts = 3;
        background = false;
        strict = false;
        verbose = false;
        dryrun = false;
        useplink = false;
        replicationoffset = 0;
    }

    void init(int argc,char * argv[])
    {
        numthreads = 10;
        connecttimeout = 0; // no timeout
        attempts = 3;
        background = false;
        strict = false;
        verbose = false;
        dryrun = false;
        useplink = false;
        for (int i=1; i<argc; i++) {
            const char *arg = argv[i];
            if (arg[0]=='-') {
                arg++;
                const char *parm = (arg[1]==':')?(arg+2):(arg+1);
                switch (toupper(*arg)) {
                    case 'N':
                        numthreads = *parm?atoi(parm):numthreads;
                        break;
                    case 'T':
                        if (toupper(arg[1])=='R') {
                            parm = (arg[2]==':')?(arg+3):(arg+2);
                            treeroot.set(parm);
                            break;
                        }
                        connecttimeout=*parm?atoi(parm):connecttimeout;
                        break;
                    case 'A':
                        attempts=*parm?atoi(parm):attempts;
                        break;
                    case 'I':
                        identityfile.set(parm);
                        break;
                    case 'U':
                        user.set(parm);
                        break;
                    case 'D':
                        if (*parm)
                            workdir.set(parm);
                        else 
                            dryrun = true;
                        break;
                    case 'S':
                        strict = true;
                        break;
                    case 'B':
                        background = true;
                        break;
                    case 'V':
                        verbose = true;
                        break;
                    case 'O':
                        replicationoffset = atoi(parm);
                        break;
                    case 'P':
#ifdef _WIN32
                        if (toupper(arg[1])=='L') {
                            useplink = true;
                            break;
                        }
#endif
                        parm = (arg[2]==':')?(arg+3):(arg+2);
                        if (!*parm)
                            break;
                        if (toupper(arg[1])=='W') {
                            StringBuffer buf;
                            encrypt(buf,parm);
                            password.set(buf.str());
                            break;
                        }
                        else if (toupper(arg[1])=='E') {
                            password.set(parm);
                            break;
                        }
                        // continue
                    default:
                        throw MakeStringException(-1,"Unknown option %s",argv[i]);
                }
            }
            else {
                if (slavesfile.isEmpty()) {
                    slavesfile.set(argv[i]);
                    loadSlaves();
                }
                else if (cmd.isEmpty())
                    cmd.set(argv[i]);
                else
                    throw MakeStringException(-1,"Unknown parameter %s",argv[i]);
            }
        }
        if (dryrun||(numthreads<=0))
            numthreads=1;
        if (!identityfile.isEmpty()&&!checkFileExists(identityfile.get()))
            throw MakeStringException(-1,"Cannot find identity file: %s",identityfile.get());
        if (!password.isEmpty()&&!identityfile.isEmpty()) {
            WARNLOG("SSH identity file specified, ignoring password");
            password.clear();
        }
    }

    void init(  
                const char *cmdline,
                const char *identfilename,
                const char *username,
                const char *passwordenc,
                unsigned timeout,
                unsigned retries)
    {
        strict = false;
        verbose = false;
        numthreads = 1;
        connecttimeout=timeout;
        attempts=retries;
#ifdef _WIN32
        identityfile.set(identfilename);
#else
        if (identfilename&&*identfilename) {
            wordexp_t exp_result;  // expand ~ etc
            wordexp(identfilename, &exp_result, 0);
            identityfile.set(exp_result.we_wordv[0]);
            wordfree(&exp_result);
        }
        else 
            identityfile.clear();
#endif
        user.set(username);
        password.set(passwordenc);
        cmd.set(cmdline);
    }

    unsigned log2(unsigned n)
    {
        assertex(n);
        unsigned ret=0;
        while (n>1) {
            ret++;
            n /= 2;
        }
        return ret;
    }

    unsigned pow2(unsigned n)
    {
        unsigned ret=1;
        while (n--) 
            ret *= 2;
        return ret;
    }

    unsigned treesrc(unsigned n)
    {
        return n-pow2(log2(n));
    }

 
    void exec(unsigned i,unsigned treefrom)
    {
        {
            CriticalBlock block1(sect);
            if (!dryrun) {
                if (slaves.ordinality()>1)
                    PROGLOG("%d: starting %s (%d of %d finished)",i,slaves.item(i),done.ordinality(),slaves.ordinality());
            }
        }
        int retcode=-1;
        StringBuffer outbuf;
        try {
            bool usepssh = false;
            StringBuffer cmdline;
            if (!password.isEmpty()) {
#ifdef _WIN32
                if (useplink) {
                    cmdline.append("plink -ssh -batch ");
                    if (!user.isEmpty())
                        cmdline.append(" -l ").append(user);
                    StringBuffer tmp;
                    decrypt(tmp,password);
                    cmdline.append(" -pw ").append(tmp);
                    cmdline.append(' ').append(slaves.item(i)).append(' ');
#ifdef PLINK_USE_CMD
                    // bit of a kludge
                    cmdline.append("cmd /c \"");
                    const char *dir = cmd.get();
                    const char *s = dir;
                    const char *e = NULL;
                    while (*s>' ') {
                        if (*s=='\\')
                            e = s;
                        s++;
                    }
#endif

                    expandCmd(cmdline,i,treefrom);
#ifdef PLINK_USE_CMD
                    cmdline.append('"');
#endif
                }
                else {
                // windows use psexec
                    cmdline.append("psexec \\\\").append(slaves.item(i));
                    if (!user.isEmpty())
                        cmdline.append(" -u ").append(user);
                    StringBuffer tmp;
                    decrypt(tmp,password);
                    cmdline.append(" -p ").append(tmp);
                    if (background)
                        cmdline.append("-d ");
                    cmdline.append(' ');
                    expandCmd(cmdline,i,treefrom);
                }
#else
                // linux use pssh
                usepssh = true;
#endif
            }
            if (cmdline.length()==0) {
                // ssh
                cmdline.appendf("%s -n -o LogLevel=QUIET -o StrictHostKeyChecking=%s ",usepssh?"pssh":"ssh",strict?"yes":"no");
                if (!usepssh)
                    cmdline.append("-o BatchMode=yes ");
                if (!identityfile.isEmpty())
                    cmdline.appendf("-i %s ",identityfile.get());
                if (background)
                    cmdline.append("-f ");
                if (connecttimeout)
                    cmdline.appendf("-o ConnectTimeout=%d ",connecttimeout);
                if (attempts)
                    cmdline.appendf("-o ConnectionAttempts=%d ",attempts);
                if (usepssh) {
                    StringBuffer tmp;
                    decrypt(tmp,password);
                    cmdline.appendf("-o password=%s ",tmp.str());

                }
                if (!user.isEmpty())
                    cmdline.appendf("%s@",user.get());
                cmdline.appendf("%s \"",slaves.item(i));
                expandCmd(cmdline,i,treefrom);
                cmdline.append('"');
            }
            if (dryrun) 
                printf("%s\n",cmdline.str());
            else {
                Owned<IPipeProcess> pipe = createPipeProcess();
                if (pipe->run((verbose&&!usepssh)?"FRUNSSH":NULL,cmdline.str(),workdir,
                    useplink, // for some reason plink needs input handle
                    true,true)) {
                    byte buf[4096];
                    loop {
                        size32_t read = pipe->read(sizeof(buf),buf);
                        if (!read)
                            break;
                        outbuf.append(read,(const char *)buf);
                    }
                    retcode = pipe->wait();
                    bool firsterr=true;
                    loop {
                        size32_t read = pipe->readError(sizeof(buf),buf);
                        if (!read)
                            break;
                        if (firsterr) {
                            firsterr = false;
                            if (outbuf.length())
                                outbuf.append('\n');
                            outbuf.append("ERR: ");
                        }
                        outbuf.append(read,(const char *)buf);
                    }
                }
            }
        }
        catch (IException *e) {
            e->errorMessage(outbuf);
            retcode = -2;
        }
        CriticalBlock block(sect);
        done.append(i);
        replytext.append(outbuf.str());
        reply.append((unsigned)retcode);
    }

    void exec()
    {
        if (!treeroot.isEmpty()) {
            // remove from slaves
            ForEachItemInRev(i,slaves) 
                if (strcmp(slaves.item(i),treeroot)==0)
                    slaves.remove(i);
        }
        if (slaves.ordinality()==0)
            return;
        class cRun: public CAsyncFor
        {
            bool treemode;
            CFRunSSH &parent;
            Semaphore *treesem;
        public:
            cRun(CFRunSSH &_parent)
                : parent(_parent)
            {
                treemode = !parent.treeroot.isEmpty();
                if (treemode) {
                    treesem = new Semaphore[parent.slaves.ordinality()+1];  // don't actually use all 
                    treesem[0].signal();
                }
                else
                    treesem = NULL;
            }
            ~cRun()
            {
                delete [] treesem;
            }
            void Do(unsigned i)
            {
                if (treemode) {
                    unsigned from = parent.treesrc(i+1);
                    treesem[from].wait();
                    parent.exec(i,from);
                    treesem[from].signal();
                    treesem[i+1].signal();
                }
                else
                    parent.exec(i,0);
            }
        } afor(*this);
        afor.For(slaves.ordinality(),(numthreads>slaves.ordinality())?slaves.ordinality():numthreads,!treeroot.isEmpty(),treeroot.isEmpty());
        if (dryrun)
            return;
        if (slaves.ordinality()>1) {
            PROGLOG("Results: (%d of %d finished)",done.ordinality(),slaves.ordinality());
            int errCode = 0;
            Owned<IMultiException> multiException = MakeMultiException();
            for (unsigned i=0;i<done.ordinality();i++) {
                unsigned n = done.item(i);
                StringBuffer res(replytext.item(n));
                while (res.length()&&(res.charAt(res.length()-1)<=' '))
                    res.setLength(res.length()-1);
                if (res.length()==0)
                    PROGLOG("%d: %s(%d): [OK]",n+1,slaves.item(n),reply.item(n));
                else if (strchr(res.str(),'\n')==NULL) {
                    PROGLOG("%d: %s(%d): %s",n+1,slaves.item(n),reply.item(n),res.str());
                    if (reply.item(n)) {
                        errCode = reply.item(n);
                        multiException->append(*MakeStringExceptionDirect(reply.item(n),res.str()));
                    }
                }
                else {
                    PROGLOG("%d: %s(%d):\n---------------------------\n%s\n===========================",n+1,slaves.item(n),reply.item(n),res.str());
                    if (reply.item(n)) {
                        errCode = reply.item(n);
                        multiException->append(*MakeStringExceptionDirect(reply.item(n),res.str()));
                    }
                }
            }
            if (errCode)
                throw multiException.getClear();
        }
        else {
            StringBuffer res(replytext.item(0));
            while (res.length()&&(res.charAt(res.length()-1)<=' '))
                res.setLength(res.length()-1);
            PROGLOG("%s result(%d):\n%s",useplink?"plink":"ssh",reply.item(0),res.str());
            if (res.length()) {
                int code = reply.item(0);
                if (code == 0)
                    code = -1;
                throw MakeStringExceptionDirect(code, res.str());
            }
        }
    }
    void exec(
              const IpAddress &ip,
              const char *workdirname,
              bool _background)
    {
        background = _background;
        strict = false;
        verbose = false;
        StringBuffer ips;
        ip.getIpText(ips);
        slaves.kill();
        slaves.append(ips.str());
        numthreads = 1;
        workdir.set(workdirname);
        exec();
    }
};

IFRunSSH *createFRunSSH()
{
    return new CFRunSSH;
}
