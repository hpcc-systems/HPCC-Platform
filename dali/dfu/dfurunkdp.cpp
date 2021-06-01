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

#include <platform.h>
#include <stdio.h>
#include <limits.h>
#include "jexcept.hpp"
#include "jstring.hpp"
#include "jsocket.hpp"
#include "jfile.hpp"

#include "dalienv.hpp"
#include "dafdesc.hpp"
#include "rmtssh.hpp"
#include "rmtspawn.hpp"

#define FULL_TRACE

class CDKDPitem : public CInterface
{
    SocketEndpoint ep;
    StringBuffer path;
    StringBuffer resultfile;
    StringBuffer cmdline;
    bool done;
    bool started;
    
    StringBuffer &getExePath(const char *tail,StringBuffer &ret,StringBuffer &workdir)
    {
        StringBuffer p;
#ifdef _CONTAINERIZED
        UNIMPLEMENTED_X("CONTAINERIZED(dkc)");
#else
        querySlaveExecutable("DKCSlaveProcess", "dkcslave", NULL, ep, p, workdir);
#endif
        splitDirTail(p.str(),ret);
        addPathSepChar(ret).append(tail);
        if (getPathSepChar(ret.str())=='\\') {
            ret.append(".bat");
            if (workdir.length()==0)
                workdir.append("c:\\dkcslave");
        }
        else {
            if (workdir.length()==0)
                workdir.append("/c$/dkcslave");
        }
        return ret;
    }

public:

    CDKDPitem()
    {
        done = false;
        started = false;
    }

    void setArgs(const char *prog,SocketEndpoint &_ep,const RemoteFilename &rfnold, const RemoteFilename &rfnnew, RemoteFilename &rfnpatch)
    {
        ep = _ep;
        StringBuffer workdir;
        getExePath(prog,path,workdir);
        cmdline.clear().append('\"').append(workdir).append("\" \"");
        rfnnew.getLocalPath(cmdline);
        cmdline.append("\" \"");
        if (rfnold.queryIP().ipequals(ep))
            rfnold.getLocalPath(cmdline);
        else
            rfnold.getRemotePath(cmdline);
        resultfile.clear().append(workdir);
        addPathSepChar(resultfile).append(prog);
        genUUID(resultfile,true).append(".tmp");
        cmdline.append("\" \"");
        rfnpatch.getLocalPath(cmdline);
        cmdline.append("\" \"").append(resultfile).append('\"');
    }

    bool run()
    {
        StringAttr SSHidentfilename;
        StringAttr SSHusername;
        StringAttr SSHpasswordenc;
        unsigned SSHtimeout;
        unsigned SSHretries;
        StringAttr SSHexeprefix;
        getRemoteSpawnSSH(
                SSHidentfilename,
                SSHusername, // if isEmpty then disable SSH
                SSHpasswordenc,
                SSHtimeout,
                SSHretries,
                SSHexeprefix);

        if (SSHusername.isEmpty()) 
            throw MakeStringException(-1,"No SSH user configured");
        Owned<IFRunSSH> runssh = createFRunSSH();
        StringBuffer cmd(path);
        cmd.append(' ').append(cmdline);
        runssh->init(cmd.str(),SSHidentfilename,SSHusername,SSHpasswordenc,SSHtimeout,SSHretries);
        runssh->exec(ep,NULL,true); // need workdir? TBD
        return true;
    }

    bool isDone()
    {
        return done;
    }

    bool isStarted()
    {
        return started;
    }

    SocketEndpoint &queryEP()
    {
        return ep;
    }

    bool check()
    {
        if (done)
            return true;
        RemoteFilename rfn;
        rfn.setPath(ep,resultfile);
        RemoteFilename rfnlog;  // exists while running
        if (!started) {
            StringBuffer fn(resultfile);
            fn.append(".log");
            rfnlog.setPath(ep,fn.str());
        }
        for (unsigned attempt=0;attempt<10;attempt++) {
            try {
                if (!started) { // see if running   (NB may skip right to done)
                    Owned<IFile> filelog = createIFile(rfnlog);
                    if (filelog->exists())
                        started = true;
                }
                Owned<IFile> file = createIFile(rfn);
                Owned<IFileIO> fio = file->open(IFOread);
                if (fio) {
                    size32_t sz = (size32_t)fio->size();
                    if (sz) {
                        StringBuffer s;
                        fio->read(0,sz,s.reserve(sz));
                        throw MakeStringException(-1, "%s", s.str());
                    }
                    try {
                        fio.clear();
                        file->remove(); 
                    }
                    catch (IException *e) {
                        EXCLOG(e,"CDKDPitem::wait(2)");
                        e->Release();
                    }
                    started = true;
                    done = true;
                    break;
                }
            }
            catch (IException *e) {
                if (attempt==9) 
                    throw e;
                EXCLOG(e,"CDKDPitem::wait(2)");
                e->Release();
            }
            Sleep(5000);
        }
        return done;
    }

};

static void runKDPNodes(const char *title,CIArrayOf<CDKDPitem> &nodes)
{
    ForEachItemIn(i2,nodes) {
        CDKDPitem &it = nodes.item(i2);
        it.run();
    }
    // first check running within 15mins 
    bool allstarted = false;
    unsigned start = msTick();
    unsigned nstarted = 0;
    unsigned ndone = 0;
    while (nstarted<nodes.ordinality()) {   
#ifdef FULL_TRACE
        PROGLOG("started = %d done = %d",ndone,nstarted);
#endif
        ForEachItemIn(i3,nodes) {
            CDKDPitem &it = nodes.item(i3);
            if (!it.isStarted()) {
                if (it.check())
                    ndone++;
                if (it.isStarted())
                    nstarted++;
                else {
                    if (msTick()-start>15*60*1000) {
                        StringBuffer err;
                        err.append(title).append(" failed to start on node ");
                        it.queryEP().getUrlStr(err);
                        throw MakeStringException(-1, "%s", err.str());
                    }
                    Sleep(5000); // no point in rushing when some left
                }
            }
        }
    }
    while (ndone<nodes.ordinality()) {      // add timeout add progress later
        ForEachItemIn(i4,nodes) {
#ifdef FULL_TRACE
        PROGLOG("started = %d done = %d",ndone,nstarted);
#endif
            CDKDPitem &it = nodes.item(i4);
            if (!it.isDone()) {
                if (it.check())
                    ndone++;
                else {
                    if (msTick()-start>6*60*60*1000) {
                        StringBuffer err;
                        err.append(title).append(" failed to finish on node ");
                        it.queryEP().getUrlStr(err);
                        throw MakeStringException(-1, "%s", err.str());
                    }
                    Sleep(5000); // no point in rushing when some left
                }
            }
        }
    }
}

void doKeyDiff(IFileDescriptor *oldf,IFileDescriptor *newf,IFileDescriptor *patchf)
{
    unsigned n = newf->numParts();
    if (oldf->numParts()!=n)
        throw MakeStringException(-1,"KeyDiff - old and new files do not have the same size");
    CIArrayOf<CDKDPitem> nodes;
    for (unsigned i1=0;i1<n;i1++) {
        RemoteFilename rfnold;
        oldf->getFilename(i1,0,rfnold);     // only do primary for first attempt
        RemoteFilename rfnnew;
        newf->getFilename(i1,0,rfnnew);     // only do primary for first attempt
        SocketEndpoint ep = rfnnew.queryEndpoint();     
        Owned<CDKDPitem> it = new CDKDPitem();
        RemoteFilename rfnpatch;
        StringBuffer fn;
        rfnnew.getLocalPath(fn);
        fn.append(".__patch__");
        rfnpatch.setPath(ep,fn.str());
        it->setArgs("run_keydiff",ep,rfnold,rfnnew,rfnpatch);
        patchf->setPart(i1,rfnpatch,NULL);
        nodes.append(*it.getClear());
    }
    runKDPNodes("keydiff",nodes);
}

void doKeyPatch(IFileDescriptor *oldf,IFileDescriptor *newf,IFileDescriptor *patchf)
{
    unsigned n = patchf->numParts();
    if (oldf->numParts()!=n)
        throw MakeStringException(-1,"KeyPatch - old and patch files do not have the same size");
    CIArrayOf<CDKDPitem> nodes;
    for (unsigned i1=0;i1<n;i1++) {
        RemoteFilename rfnold;
        oldf->getFilename(i1,0,rfnold);     // only do primary for first attempt
        RemoteFilename rfnnew;
        newf->getFilename(i1,0,rfnnew);     
        RemoteFilename rfnpatch;
        patchf->getFilename(i1,0,rfnpatch);     
        SocketEndpoint ep = rfnpatch.queryEndpoint();       
        Owned<CDKDPitem> it = new CDKDPitem();
        it->setArgs("run_keypatch",ep,rfnold,rfnnew,rfnpatch);
        patchf->setPart(i1,rfnpatch,NULL);
        nodes.append(*it.getClear());
    }
    runKDPNodes("keypatch",nodes);

}





