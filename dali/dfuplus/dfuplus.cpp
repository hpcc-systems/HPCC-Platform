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

#pragma warning( disable : 4786)

#include "platform.h"
#include "portlist.h"

#include "dfuplus.hpp"
#include "dfuwu.hpp"
#include "bindutil.hpp"
#include "ws_fs.hpp"
#include "ws_dfu.hpp"

#define WAIT_SECONDS 30

#ifdef DAFILESRV_LOCAL
#include "rmtfile.hpp"
#include "sockfile.hpp"

#include "jutil.hpp"

static class CSecuritySettings
{
    bool useSSL;
    unsigned short daliServixPort;
public:
    CSecuritySettings()
    {
        querySecuritySettings(&useSSL, &daliServixPort, NULL, NULL);
    }

    unsigned short queryDaliServixPort() { return daliServixPort; }
} securitySettings;

class CDafsThread: public Thread
{
    Owned<IRemoteFileServer> server;
    SocketEndpoint listenep;
    Owned<IException> exc;

public:
    CDafsThread(SocketEndpoint &_listenep,bool requireauthenticate) 
        : listenep(_listenep)
    {
        if (listenep.port==0)
            listenep.port = securitySettings.queryDaliServixPort();
        StringBuffer eps;
        if (listenep.isNull())
            eps.append(listenep.port);
        else
            listenep.getUrlStr(eps);
        enableDafsAuthentication(requireauthenticate);
        server.setown(createRemoteFileServer());
        server->setThrottle(ThrottleStd, 0); // disable throttling
        server->setThrottle(ThrottleSlow, 0); // disable throttling
    }

    int run()
    {
        try {
            server->run(listenep);
        }
        catch (IException *e) {
            EXCLOG(e,"dfuplus(dafilesrv)");
            if (exc.get())
                e->Release();
            else
                exc.setown(e);
        }
        return 0;
    }

    void stop()
    {
        try {
            if (server) 
                server->stop();
        }
        catch (IException *e) {
            EXCLOG(e,"dfuplus(dafilesrvstop)");
            e->Release();
        }
        server.clear();

    }

    bool ok() { return exc.get()==NULL; }

    unsigned idleTime() { return server.get()?server->idleTime():0; }

};

bool CDfuPlusHelper::runLocalDaFileSvr(SocketEndpoint &listenep,bool requireauthenticate, unsigned timeout)
{
    Owned<CDafsThread> thr = new CDafsThread(listenep,requireauthenticate); 
    if (!thr->ok())
        return false;
    thr->start();
    StringBuffer eps;
    if (listenep.isNull())
        progress("Started local Dali file server on port %d\n", listenep.port?listenep.port:securitySettings.queryDaliServixPort());
    else
        progress("Started local Dali file server on %s\n", listenep.getUrlStr(eps).str());
    if (timeout==0) {
        setDafsTrace(NULL,0); // disable client tracing
        dafsthread.setown(thr.getClear());
    }
    else {
        loop {
            Sleep(500);
            if (thr->idleTime()>timeout) { 
                thr->stop();
                break;
            }
        }
    }
    return true;
}

bool CDfuPlusHelper::checkLocalDaFileSvr(const char *eps,SocketEndpoint &epout)
{
    if (!eps||!*eps)
        epout.setLocalHost(securitySettings.queryDaliServixPort());
    else {
        epout.set(eps,securitySettings.queryDaliServixPort());
        if (!epout.isLocal())
            return false;
    }
    progress("Checking for local Dali File Server\n");
    if (!testDaliServixPresent(epout)) // only lookup local
        runLocalDaFileSvr(epout,false,0);
    return true;
}

#else

bool CDfuPlusHelper::runLocalDaFileSvr(SocketEndpoint &listenep,bool requireauthenticate, unsigned timeout)
{
    return false;
}

bool CDfuPlusHelper::checkLocalDaFileSvr(const char *,SocketEndpoint &)
{
    return false;
}


#endif


CDfuPlusHelper::CDfuPlusHelper(IProperties* _globals,   CDfuPlusMessagerIntercept *_msgintercept)
{
    msgintercept = _msgintercept;
    globals.setown(_globals);
    sprayclient.setown(createFileSprayClient());

    const char* server = globals->queryProp("server");
    if(server == NULL)
        throw MakeStringException(-1, "Server url not specified");
    
    StringBuffer url;
    if(Utils::strncasecmp(server, "http://", 7) != 0 && Utils::strncasecmp(server, "https://", 8) != 0)
        url.append("http://");
    url.append(server);

    if(strchr(url.str() + 7, ':') == NULL)
    url.append(":8010/");

    if(url.charAt(url.length() - 1) != '/')
        url.append("/");

    StringBuffer fsurl(url.str());
    fsurl.append("FileSpray");
    sprayclient->addServiceUrl(fsurl.str());
    
    dfuclient.setown(createWsDfuClient());
    StringBuffer dfuurl(url.str());
    dfuurl.append("WsDfu");
    dfuclient->addServiceUrl(dfuurl.str());

    const char* username = globals->queryProp("username");
    const char* password = globals->queryProp("password");
    if ( username && *username && (!password || !*password))
    {
        VStringBuffer prompt("%s's password: ", username);
        StringBuffer passw;
        passwordInput(prompt, passw);
        globals->setProp("password",passw.str());
        password = globals->queryProp("password");
    }

    sprayclient->setUsernameToken(username, password, NULL);
    dfuclient->setUsernameToken(username, password, NULL);

}

CDfuPlusHelper::~CDfuPlusHelper()
{
#ifdef DAFILESRV_LOCAL
    if (dafsthread.get()) {
        CDafsThread *dthread = QUERYINTERFACE(dafsthread.get(),CDafsThread);
        if (dthread)
            dthread->stop();
        if (!dafsthread->join(1000*60))
            error("CDfuPlusHelper dafsthread not stopped\n");
        dafsthread.clear();
    }
#endif
}

int CDfuPlusHelper::doit()
{
    const char* action = globals->queryProp("action");
    if(action == NULL || *action == '\0')
        throw MakeStringException(-1, "action is missing");
    else if(stricmp(action, "spray") == 0)
        return spray();
    else if(stricmp(action, "replicate") == 0)
        return replicate();
    else if(stricmp(action, "despray") == 0)
        return despray();
    else if(stricmp(action, "copy") == 0)
        return copy();
    else if(stricmp(action, "copysuper") == 0)
        return copysuper();
    else if(stricmp(action, "remove") == 0)
        return remove();
    else if(stricmp(action, "rename") == 0)
        return rename();
    else if(stricmp(action, "list") == 0)
        return list();
    else if(stricmp(action, "recover") == 0)
        return recover();
    else if(stricmp(action, "addsuper") == 0)
        return superfile("add");
    else if(stricmp(action, "removesuper") == 0)
        return superfile("remove");
    else if(stricmp(action, "listsuper") == 0)
        return superfile("list");
    else if(stricmp(action, "savexml") == 0)
        return savexml();
    else if(stricmp(action, "add") == 0)
        return add();
    else if(stricmp(action, "status") == 0)
        return status();
    else if(stricmp(action, "abort") == 0)
        return abort();
    else if(stricmp(action, "resubmit") == 0)
        return resubmit();
    else if(stricmp(action, "monitor") == 0)
        return monitor();
#ifdef DAFILESRV_LOCAL
    else if(stricmp(action, "dafilesrv") == 0)
        return rundafs();
#endif
    else
        throw MakeStringException(-1, "Unknown dfuplus action");
    return 0;
}

bool CDfuPlusHelper::fixedSpray(const char* srcxml,const char* srcip,const char* srcfile,const MemoryBuffer &xmlbuf,const char* dstcluster,const char* dstname,const char *format, StringBuffer &retwuid, StringBuffer &except)
{
    int recordsize;
    if(stricmp(format, "recfmvb") == 0) {
        recordsize = RECFMVB_RECSIZE_ESCAPE;  // special value for recfmvb 
    }
    else if(stricmp(format, "recfmv") == 0) {
        recordsize = RECFMV_RECSIZE_ESCAPE;  // special value for recfmv
    }
    else if(stricmp(format, "variable") == 0) {
        recordsize = PREFIX_VARIABLE_RECSIZE_ESCAPE;  // special value for variable 
    }
    else if(stricmp(format, "variablebigendian") == 0) {
        recordsize = PREFIX_VARIABLE_BIGENDIAN_RECSIZE_ESCAPE;  // special value for variable bigendian
    }
    else {
        const char* rsstr = globals->queryProp("recordsize");
        recordsize = rsstr?atoi(rsstr):0;
        if(!recordsize && !globals->hasProp("nosplit"))
            throw MakeStringException(-1, "recordsize not specified for fixed");
    }

    Owned<IClientSprayFixed> req = sprayclient->createSprayFixedRequest();
    if(srcxml == NULL)
    {
        req->setSourceIP(srcip);
        req->setSourcePath(srcfile);
    }
    else
        req->setSrcxml(xmlbuf);

    if(recordsize != 0)
        req->setSourceRecordSize(recordsize);

    if(dstcluster != NULL)
        req->setDestGroup(dstcluster);
    req->setDestLogicalName(dstname);
    req->setOverwrite(globals->getPropBool("overwrite", false));
    req->setReplicate(globals->getPropBool("replicate", true));
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    if(globals->hasProp("prefix"))
        req->setPrefix(globals->queryProp("prefix"));
    if(globals->hasProp("nosplit"))
        req->setNosplit(globals->getPropBool("nosplit", false));
    if(globals->hasProp("wrap"))
        req->setWrap(globals->getPropBool("wrap", false));
    if(globals->hasProp("compress"))
        req->setCompress(globals->getPropBool("compress", false));
    if(globals->hasProp("encrypt"))
        req->setEncrypt(globals->queryProp("encrypt"));
    if(globals->hasProp("decrypt"))
        req->setDecrypt(globals->queryProp("decrypt"));
    if(globals->hasProp("push")) {
        if (globals->getPropBool("push"))
            req->setPush(true);
        else 
            req->setPull(true);
    }
    if(globals->hasProp("norecover"))
        req->setNorecover(globals->getPropBool("norecover", false));
    else if(globals->hasProp("noRecover"))
        req->setNorecover(globals->getPropBool("noRecover", false));
    
    if(globals->hasProp("connect"))
        req->setMaxConnections(globals->getPropInt("connect"));
    if(globals->hasProp("throttle"))
        req->setThrottle(globals->getPropInt("throttle"));
    if(globals->hasProp("transferbuffersize"))
        req->setTransferBufferSize(globals->getPropInt("transferbuffersize"));

    if(globals->hasProp("failIfNoSourceFile"))
        req->setFailIfNoSourceFile(globals->getPropBool("failIfNoSourceFile",false));

    if(srcxml == NULL)
        info("\nFixed spraying from %s on %s to %s\n", srcfile, srcip, dstname);
    else
        info("\nSpraying to %s\n", dstname);

    Owned<IClientSprayFixedResponse> result = sprayclient->SprayFixed(req);
    const char *wuid = result->getWuid();
    if (!wuid||!*wuid) {
        result->getExceptions().errorMessage(except);
        return false;
    }
    retwuid.append(wuid);
    return true;
}

bool CDfuPlusHelper::variableSpray(const char* srcxml,const char* srcip,const char* srcfile,const MemoryBuffer &xmlbuf,const char* dstcluster,const char* dstname,const char *format,StringBuffer &retwuid, StringBuffer &except)
{
    Owned<IClientSprayVariable> req = sprayclient->createSprayVariableRequest();
    if(srcxml == NULL)
    {
        req->setSourceIP(srcip);
        req->setSourcePath(srcfile);
    }
    else
    {
        req->setSrcxml(xmlbuf);
    }

    const char* mrsstr = globals->queryProp("maxRecordSize");
    if(mrsstr != NULL)
        req->setSourceMaxRecordSize(atoi(mrsstr));
    else
        req->setSourceMaxRecordSize(8192);

    const char* encoding = globals->queryProp("encoding");
    const char* rowtag = globals->queryProp("rowtag");
    const char* rowpath = globals->queryProp("rowpath");
    if(strieq(format, "json"))
    {
        req->setIsJSON(true);
        if (!encoding)
            encoding = "utf8";
        else if (strieq(encoding, "ascii"))
            throw MakeStringExceptionDirect(-1, "json format only accepts utf encodings");
        if(rowtag && *rowtag)
            throw MakeStringExceptionDirect(-1, "You can't use rowtag option with json format");
        if (rowpath && *rowpath)
            req->setSourceRowPath(rowpath);
    }
    else if(stricmp(format, "xml") == 0)
    {
        if(encoding == NULL)
            encoding = "utf8";
        else if(stricmp(encoding, "ascii") == 0)
            throw MakeStringException(-1, "xml format only accepts utf encodings");
        if(rowtag == NULL || *rowtag == '\0')
            throw MakeStringException(-1, "rowtag not specified.");
        if(rowpath && *rowpath)
            throw MakeStringException(-1, "You can't use rowpath option with xml format");
    }
    else if(stricmp(format, "csv") == 0)
    {
        if(encoding == NULL)
            encoding = "ascii";

        if(rowtag != NULL && *rowtag != '\0')
            throw MakeStringException(-1, "You can't use rowtag option with csv/delimited format");
        if(rowpath && *rowpath)
            throw MakeStringException(-1, "You can't use rowpath option with csv/delimited format");

        const char* separator = globals->queryProp("separator");
        if(separator)
        {
            // Pass separator definition string from command line if defined
            // even it is empty to override default value
            req->setSourceCsvSeparate(separator);
            if (*separator == '\0')
                req->setNoSourceCsvSeparator(true);
        }
        const char* terminator = globals->queryProp("terminator");
        if(terminator && *terminator)
            req->setSourceCsvTerminate(terminator);
        const char* quote = globals->queryProp("quote");
        if(quote)
        {
            // Pass quote definition string from command line if defined
            // even it is empty to override default value
            req->setSourceCsvQuote(quote);
        }
        const char* escape = globals->queryProp("escape");
        if(escape && *escape)
            req->setSourceCsvEscape(escape);
    }
    else 
        encoding = format; // may need extra later

    req->setSourceFormat(CDFUfileformat::decode(encoding));
    if(rowtag != NULL)
        req->setSourceRowTag(rowtag);

    if(dstcluster != NULL)
        req->setDestGroup(dstcluster);
    req->setDestLogicalName(dstname);
    req->setOverwrite(globals->getPropBool("overwrite", false));
    req->setReplicate(globals->getPropBool("replicate", true));
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    if(globals->hasProp("prefix"))
        req->setPrefix(globals->queryProp("prefix"));
    if(globals->hasProp("nosplit"))
        req->setNosplit(globals->getPropBool("nosplit", false));
    if(globals->hasProp("push")) {
        if (globals->getPropBool("push"))
            req->setPush(true);
        else 
            req->setPull(true);
    }
    if(globals->hasProp("compress"))
        req->setCompress(globals->getPropBool("compress", false));
    if(globals->hasProp("encrypt"))
        req->setEncrypt(globals->queryProp("encrypt"));
    if(globals->hasProp("decrypt"))
        req->setDecrypt(globals->queryProp("decrypt"));

    if(globals->hasProp("norecover"))
        req->setNorecover(globals->getPropBool("norecover", false));
    else if(globals->hasProp("noRecover"))
        req->setNorecover(globals->getPropBool("noRecover", false));

    if(globals->hasProp("connect"))
        req->setMaxConnections(globals->getPropInt("connect"));
    if(globals->hasProp("throttle"))
        req->setThrottle(globals->getPropInt("throttle"));
    if(globals->hasProp("transferbuffersize"))
        req->setTransferBufferSize(globals->getPropInt("transferbuffersize"));

    if(globals->hasProp("failIfNoSourceFile"))
        req->setFailIfNoSourceFile(globals->getPropBool("failIfNoSourceFile",false));

    if(globals->hasProp("recordStructurePresent"))
        req->setRecordStructurePresent(globals->getPropBool("recordStructurePresent",false));

    if(globals->hasProp("quotedTerminator"))
        req->setQuotedTerminator(globals->getPropBool("quotedTerminator",true));

    if(srcxml == NULL)
        info("\nVariable spraying from %s on %s to %s\n", srcfile, srcip, dstname);
    else
        info("\nSpraying to %s\n", dstname);
    Owned<IClientSprayResponse> result = sprayclient->SprayVariable(req);
    const char *wuid = result->getWuid();
    if (!wuid||!*wuid) {
        result->getExceptions().errorMessage(except);
        return false;
    }
    retwuid.append(wuid);
    return true;
}

int CDfuPlusHelper::spray()
{
    const char* srcxml = globals->queryProp("srcxml");
    const char* srcip = globals->queryProp("srcip");
    const char* srcfile = globals->queryProp("srcfile");
    
    bool nowait = globals->getPropBool("nowait", false);

    MemoryBuffer xmlbuf;

    if(srcxml == NULL)
    {
        if(srcfile == NULL)
            throw MakeStringException(-1, "srcfile not specified");
        if(srcip == NULL) {
#ifdef DAFILESRV_LOCAL
            progress("srcip not specified - assuming spray from local machine\n");
            srcip = ".";
#else
            throw MakeStringException(-1, "srcip not specified");
#endif
        }
    }
    else
    {
        if(srcip != NULL || srcfile != NULL)
            throw MakeStringException(-1, "srcip/srcfile and srcxml can't be used at the same time");
        StringBuffer buf;
        buf.loadFile(srcxml);
        int len = buf.length();
        xmlbuf.setBuffer(len, buf.detach(), true);
    }

    const char* dstname = globals->queryProp("dstname");
    if(dstname == NULL)
        throw MakeStringException(-1, "dstname not specified");
    const char* dstcluster = globals->queryProp("dstcluster");
    if(dstcluster == NULL)
        throw MakeStringException(-1, "dstcluster not specified");
    const char* format = globals->queryProp("format");
    if(format == NULL)
        format = "fixed";
    else if (stricmp(format, "delimited") == 0)
        format="csv";

    SocketEndpoint localep;
    StringBuffer localeps;
    if (checkLocalDaFileSvr(srcip,localep))
        srcip = localep.getUrlStr(localeps).str();
    StringBuffer wuid;
    StringBuffer errmsg;
    bool ok;
    if ((stricmp(format, "fixed") == 0)||(stricmp(format, "recfmvb") == 0)||(stricmp(format, "recfmv") == 0)||(stricmp(format, "variablebigendian") == 0))
        ok = fixedSpray(srcxml,srcip,srcfile,xmlbuf,dstcluster,dstname,format,wuid,errmsg);
    else if((stricmp(format, "csv") == 0)||(stricmp(format, "xml") == 0)||(stricmp(format, "json") == 0)||(stricmp(format, "variable") == 0))
        ok = variableSpray(srcxml,srcip,srcfile,xmlbuf,dstcluster,dstname,format,wuid, errmsg);
    else
        throw MakeStringException(-1, "format %s not supported", format);
    if (!ok) {
        if(errmsg.length())
            error("%s\n", errmsg.str());
        else
            throw MakeStringException(-1, "unknown error spraying");
    }
    else {
        const char* jobname = globals->queryProp("jobname");
        if(jobname && *jobname)
            updatejobname(wuid.str(), jobname);

        info("Submitted WUID %s\n", wuid.str());
        if(!nowait)
            waitToFinish(wuid.str());
    }

    return 0;
}

int CDfuPlusHelper::replicate()
{
    const char* srcname = globals->queryProp("srcname");
    if(srcname == NULL)
        throw MakeStringException(-1, "srcname not specified");

    bool nowait = globals->getPropBool("nowait", false);

    Owned<IClientReplicate> req = sprayclient->createReplicateRequest();
    req->setSourceLogicalName(srcname);
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    bool repeatlast = globals->getPropBool("repeatlast");
    bool onlyrepeated = repeatlast&&globals->getPropBool("onlyrepeated");
    StringBuffer cluster;
    globals->getProp("cluster",cluster);
    if (cluster.length()) 
        req->setCluster(cluster.str());
    else if (repeatlast) {
        error("replicate repeatlast specified with no cluster\n");
        return 0;
    }
    if (repeatlast) 
        req->setRepeatLast(true);
    if (onlyrepeated) 
        req->setOnlyRepeated(true);

    Owned<IClientReplicateResponse> result = sprayclient->Replicate(req);
    const char* wuid = result->getWuid();
    if(wuid == NULL || *wuid == '\0')
        exc(result->getExceptions(),"replicating");
    else
    {
        const char* jobname = globals->queryProp("jobname");
        if(jobname && *jobname)
            updatejobname(wuid, jobname);

        info("Submitted WUID %s\n", wuid);
        if(!nowait)
            waitToFinish(wuid);
    }

    return 0;
}

int CDfuPlusHelper::despray()
{
    const char* srcname = globals->queryProp("srcname");
    if(srcname == NULL)
        throw MakeStringException(-1, "srcname not specified");
    
    const char* dstxml = globals->queryProp("dstxml");
    const char* dstip = globals->queryProp("dstip");
    const char* dstfile = globals->queryProp("dstfile");

    bool nowait = globals->getPropBool("nowait", false);

    MemoryBuffer xmlbuf;
    if(dstxml == NULL)
    {
        if(dstfile == NULL)
            throw MakeStringException(-1, "dstfile not specified");
        if(dstip == NULL) {
#ifdef DAFILESRV_LOCAL
            progress("dstip not specified - assuming spray from local machine\n");
            dstip = ".";
#else
            throw MakeStringException(-1, "dstip not specified");
#endif
        }
    }
    else
    {
        if(dstip != NULL || dstfile != NULL)
            throw MakeStringException(-1, "dstip/dstfile and dstxml can't be used at the same time");
        StringBuffer buf;
        buf.loadFile(dstxml);
        int len = buf.length();
        xmlbuf.setBuffer(len, buf.detach(), true);
    }

    if(dstxml == NULL)
        info("\nDespraying %s to host %s file %s\n", srcname, dstip, dstfile);
    else
        info("\nDespraying %s\n", srcname);

    Owned<IClientDespray> req = sprayclient->createDesprayRequest();
    req->setSourceLogicalName(srcname);
    if(dstxml == NULL)
    {
        req->setDestIP(dstip);
        req->setDestPath(dstfile);
    }
    else
        req->setDstxml(xmlbuf);

    req->setOverwrite(globals->getPropBool("overwrite", false));
    if(globals->hasProp("connect"))
        req->setMaxConnections(globals->getPropInt("connect"));
    if(globals->hasProp("throttle"))
        req->setThrottle(globals->getPropInt("throttle"));
    if(globals->hasProp("transferbuffersize"))
        req->setTransferBufferSize(globals->getPropInt("transferbuffersize"));

    if(globals->hasProp("norecover"))
        req->setNorecover(globals->getPropBool("norecover", false));
    else if(globals->hasProp("noRecover"))
        req->setNorecover(globals->getPropBool("noRecover", false));

    if(globals->hasProp("splitprefix"))
        req->setSplitprefix(globals->queryProp("splitprefix"));
    else if(globals->hasProp("splitPrefix"))
        req->setSplitprefix(globals->queryProp("splitPrefix"));

    if(globals->hasProp("wrap"))
        req->setWrap(globals->getPropBool("wrap", false));
    if(globals->hasProp("multicopy"))
        req->setMultiCopy(globals->getPropBool("multicopy", false));
    else if(globals->hasProp("multiCopy"))
        req->setMultiCopy(globals->getPropBool("multiCopy", false));

    if(globals->hasProp("compress"))
        req->setCompress(globals->getPropBool("compress", false));
    if(globals->hasProp("encrypt"))
        req->setEncrypt(globals->queryProp("encrypt"));
    if(globals->hasProp("decrypt"))
        req->setDecrypt(globals->queryProp("decrypt"));


    SocketEndpoint localep;
    StringBuffer localeps;
    if (checkLocalDaFileSvr(dstip,localep))
        dstip = localep.getUrlStr(localeps).str();
    Owned<IClientDesprayResponse> result = sprayclient->Despray(req);
    const char* wuid = result->getWuid();
    if(wuid == NULL || *wuid == '\0')
        exc(result->getExceptions(),"despraying");
    else
    {
        const char* jobname = globals->queryProp("jobname");
        if(jobname && *jobname)
            updatejobname(wuid, jobname);

        info("Submitted WUID %s\n", wuid);
        if(!nowait)
            waitToFinish(wuid);
    }

    return 0;
}

int CDfuPlusHelper::copy()
{
    const char* srcname = globals->queryProp("srcname");
    if(srcname == NULL)
        throw MakeStringException(-1, "srcname not specified");
    const char* dstname = globals->queryProp("dstname");
    if(dstname == NULL)
        throw MakeStringException(-1, "dstname not specified");
    const char* dstcluster = globals->queryProp("dstcluster");
    const char* dstclusterroxie = globals->queryProp("dstclusterroxie");
    const char* srcdali = globals->queryProp("srcdali");
    const char* srcusername = globals->queryProp("srcusername");
    const char* srcpassword = globals->queryProp("srcpassword");
    const char* diffkeydst = globals->queryProp("diffkeysrc");
    const char* diffkeysrc = globals->queryProp("diffkeydst");

    bool nowait = globals->getPropBool("nowait", false);
    
    info("\nCopying from %s to %s\n", srcname, dstname);

    Owned<IClientCopy> req = sprayclient->createCopyRequest();
    req->setSourceLogicalName(srcname);
    req->setDestLogicalName(dstname);
    if(dstcluster != NULL)
        req->setDestGroup(dstcluster);
    if(dstclusterroxie && !stricmp(dstclusterroxie, "Yes"))
        req->setDestGroupRoxie("Yes");
    if(srcdali !=  NULL)
        req->setSourceDali(srcdali);
    if(diffkeysrc !=  NULL)
        req->setSourceDiffKeyName(diffkeysrc);
    if(diffkeydst !=  NULL)
        req->setDestDiffKeyName(diffkeydst);
    if(srcusername && *srcusername)
        req->setSrcusername(srcusername);
    if(srcpassword && *srcpassword)
        req->setSrcpassword(srcpassword);
    req->setOverwrite(globals->getPropBool("overwrite", false));
    req->setReplicate(globals->getPropBool("replicate", true));
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    if(globals->hasProp("nosplit"))
        req->setNosplit(globals->getPropBool("nosplit", false));
    if(globals->hasProp("push")) {
        if (globals->getPropBool("push"))
            req->setPush(true);
        else 
            req->setPull(true);
    }
    if(globals->hasProp("compress"))
        req->setCompress(globals->getPropBool("compress", false));
    if(globals->hasProp("preserveCompression"))
        req->setPreserveCompression(globals->getPropBool("preserveCompression", true));
    if(globals->hasProp("encrypt"))
        req->setEncrypt(globals->queryProp("encrypt"));
    if(globals->hasProp("decrypt"))
        req->setDecrypt(globals->queryProp("decrypt"));
    if(globals->hasProp("connect"))
        req->setMaxConnections(globals->getPropInt("connect"));
    if(globals->hasProp("throttle"))
        req->setThrottle(globals->getPropInt("throttle"));
    if(globals->hasProp("transferbuffersize"))
        req->setTransferBufferSize(globals->getPropInt("transferbuffersize"));
    if(globals->hasProp("wrap"))
        req->setWrap(globals->getPropBool("wrap", false));
    if(globals->hasProp("multicopy"))
        req->setMulticopy(globals->getPropBool("multicopy", false));
    else if(globals->hasProp("multiCopy"))
        req->setMulticopy(globals->getPropBool("multiCopy", false));

    if(globals->hasProp("norecover"))
        req->setNorecover(globals->getPropBool("norecover", false));
    else if(globals->hasProp("noRecover"))
        req->setNorecover(globals->getPropBool("noRecover", false));

    Owned<IClientCopyResponse> result = sprayclient->Copy(req);
    const char* wuid = result->getResult();
    if(wuid == NULL || *wuid == '\0')
        exc(result->getExceptions(),"copying");
    else
    {
        const char* jobname = globals->queryProp("jobname");
        if(jobname && *jobname)
            updatejobname(wuid, jobname);

        info("Submitted WUID %s\n", wuid);
        if(!nowait)
            waitToFinish(wuid);
    }

    return 0;
}

int CDfuPlusHelper::copysuper()
{
    const char* srcname = globals->queryProp("srcname");
    if(srcname == NULL)
        throw MakeStringException(-1, "srcname not specified");
    const char* dstname = globals->queryProp("dstname");
    if(dstname == NULL)
        throw MakeStringException(-1, "dstname not specified");
    const char* dstcluster = globals->queryProp("dstcluster");
    if(dstcluster == NULL)
        throw MakeStringException(-1, "dstcluster not specified");
    const char* srcdali = globals->queryProp("srcdali");
    if(srcdali == NULL)
        throw MakeStringException(-1, "srcdali not specified");
    const char* dstclusterroxie = globals->queryProp("dstclusterroxie"); // not sure if this applicable
    const char* srcusername = globals->queryProp("srcusername");
    const char* srcpassword = globals->queryProp("srcpassword");

    bool nowait = globals->getPropBool("nowait", false);
    
    info("\nCopying superfile from %s to %s\n", srcname, dstname);

    Owned<IClientCopy> req = sprayclient->createCopyRequest();
    req->setSuperCopy(true);
    req->setSourceLogicalName(srcname);
    req->setDestLogicalName(dstname);
    req->setDestGroup(dstcluster);
    if(dstclusterroxie && !stricmp(dstclusterroxie, "Yes")) // not sure if this applicable
        req->setDestGroupRoxie("Yes");
    req->setSourceDali(srcdali);
    if(srcusername && *srcusername)
        req->setSrcusername(srcusername);
    if(srcpassword && *srcpassword)
        req->setSrcpassword(srcpassword);
    req->setOverwrite(globals->getPropBool("overwrite", false));
/* the foolowing are not currently supported serverside */
    req->setReplicate(globals->getPropBool("replicate", true));
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    if(globals->hasProp("nosplit"))
    req->setNosplit(globals->getPropBool("nosplit", false));
    if(globals->hasProp("push")) {
        if (globals->getPropBool("push"))
            req->setPush(true);
        else 
            req->setPull(true);
    }
    if(globals->hasProp("compress"))
        req->setCompress(globals->getPropBool("compress", false));
    if(globals->hasProp("encrypt"))
        req->setEncrypt(globals->queryProp("encrypt"));
    if(globals->hasProp("decrypt"))
        req->setDecrypt(globals->queryProp("decrypt"));
    if(globals->hasProp("connect"))
        req->setMaxConnections(globals->getPropInt("connect"));
    if(globals->hasProp("throttle"))
        req->setThrottle(globals->getPropInt("throttle"));
    if(globals->hasProp("transferbuffersize"))
        req->setTransferBufferSize(globals->getPropInt("transferbuffersize"));
    if(globals->hasProp("wrap"))
        req->setWrap(globals->getPropBool("wrap", false));
    if(globals->hasProp("norecover"))
        req->setNorecover(globals->getPropBool("norecover", false));
    else if(globals->hasProp("noRecover"))
        req->setNorecover(globals->getPropBool("noRecover", false));

    Owned<IClientCopyResponse> result = sprayclient->Copy(req);
    const char* ret = result->getResult();  
    if(ret == NULL || *ret == '\0')
        exc(result->getExceptions(),"copying");
    else if (stricmp(ret,"OK")==0)
        info("Superfile copy completed\n");
    else
        info("Superfile copy failed: %s\n",ret);

    return 0;
}

int CDfuPlusHelper::monitor()
{
    const char* eventname = globals->queryProp("event");
    const char* lfn = globals->queryProp("lfn");
    const char* eps = globals->queryProp("ip");
    const char* filename = globals->queryProp("file");
    bool sub = globals->getPropBool("sub");
    if ((lfn == NULL) && (filename == NULL))
        throw MakeStringException(-1, "neither lfn nor filename specified");
    int shotlimit = globals->getPropInt("shotlimit");
    if (shotlimit == 0)
        shotlimit = 1;  
    if (lfn)
        info("\nEvent %s: Monitoring logical file name %s\n", eventname?eventname:"", lfn);
    else if (eps)
        info("\nEvent %s: Monitoring file(s) %s on %s\n", eventname?eventname:"", filename, eps );
    else
        info("\nEvent %s: Monitoring file(s) %s\n", eventname?eventname:"", filename );


    Owned<IClientDfuMonitorRequest> req = sprayclient->createDfuMonitorRequest();
    if (eventname)
        req->setEventName(eventname);
    if (lfn)
        req->setLogicalName(lfn);
    if (eps) 
        req->setIp(eps);
    if (filename)
        req->setFilename(filename);
    req->setSub(sub);
    req->setShotLimit(shotlimit);
    Owned<IClientDfuMonitorResponse> result = sprayclient->DfuMonitor(req);

    const char* wuid = result->getWuid();
    if(wuid == NULL || *wuid == '\0')
        exc(result->getExceptions(),"monitoring");
    else
    {
        info("Submitted WUID %s\n", wuid);
    }
    return 0;
}

int CDfuPlusHelper::rundafs()
{
    unsigned idletimeoutsecs = (unsigned)globals->getPropInt("idletimeout",INFINITE);
    SocketEndpoint listenep;
    if (runLocalDaFileSvr(listenep,false,(idletimeoutsecs!=INFINITE)?idletimeoutsecs*1000:INFINITE)) 
        return 0;
    return 1;
}


int CDfuPlusHelper::remove()
{
    StringArray files;
    const char* name = globals->queryProp("name");
    const char* names = globals->queryProp("names");
    const char* namelist = globals->queryProp("namelist");
    if(name && *name)
    {
        files.append(name);
        info("\nRemoving %s\n", name);
    }
    else if(names && *names)
    {
        info("\nRemoving %s\n", names);

        const char* ptr = names;
        while(*ptr != '\0')
        {
            StringBuffer onesub;
            while(*ptr != '\0' && *ptr != ',')
            {
                onesub.append((char)(*ptr));
                ptr++;
            }
            if(onesub.length() > 0)
                files.append(onesub.str());
            if(*ptr != '\0')
                ptr++;
        }
    }
    else if(namelist && *namelist)
    {
        FILE* f = fopen(namelist, "r");
        if (f)
        {
            char buffer[1024*1];
            while(fgets(buffer,sizeof(buffer)-1,f))
            {
                if(strlen(buffer) == 0)
                    break;

                info("\nRemoving %s\n", buffer);

                const char* ptr = buffer;
                while(*ptr != '\0')
                {
                    StringBuffer onesub;
                    while(*ptr != '\0' && *ptr != ',')
                    {
                        onesub.append((char)(*ptr));
                        ptr++;
                    }
                    if(onesub.length() > 0)
                        files.append(onesub.str());
                    if(*ptr != '\0')
                        ptr++;
                }
            }

            fclose(f);
        }
    }

    if(files.length() < 1)
        throw MakeStringException(-1, "file name not specified");
    
    Owned<IClientDFUArrayActionRequest> req = dfuclient->createDFUArrayActionRequest();
    req->setType("Delete");
    req->setLogicalFiles(files);

    Owned<IClientDFUArrayActionResponse> resp = dfuclient->DFUArrayAction(req);

    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        if(errmsg.length() > 0)
            error("%s\n", errmsg.str());
        return -1;
    }

    const char* result = resp->getDFUArrayActionResult();
    StringBuffer resultbuf;
    if(result != NULL && *result != '\0')
    {
        if(*result != '<')
            resultbuf.append(result);
        else
        {
            bool intag = false;
            int i = 0;
            char c;
            while((c = result[i]) != '\0')
            {
                if(c == '<')
                    intag = true;
                else if(c == '>')
                    intag = false;
                else if(!intag)
                    resultbuf.append(c);
                i++;
            }
        }
    }
    info("%s\n", resultbuf.str());

    return 0;
}

int CDfuPlusHelper::rename()
{
    const char* srcname = globals->queryProp("srcname");
    if(srcname == NULL)
        throw MakeStringException(-1, "srcname not specified");
    const char* dstname = globals->queryProp("dstname");
    if(dstname == NULL)
        throw MakeStringException(-1, "dstname not specified");

    bool nowait = globals->getPropBool("nowait", false);
    
    info("\nRenaming from %s to %s\n", srcname, dstname);

    Owned<IClientRename> req = sprayclient->createRenameRequest();
    req->setSrcname(srcname);
    req->setDstname(dstname);

    Owned<IClientRenameResponse> result = sprayclient->Rename(req);
    const char* wuid = result->getWuid();
    if(wuid == NULL || *wuid == '\0')
        exc(result->getExceptions(),"copying");
    else
    {
        const char* jobname = globals->queryProp("jobname");
        if(jobname && *jobname)
            updatejobname(wuid, jobname);

        info("Submitted WUID %s\n", wuid);
        if(!nowait)
            waitToFinish(wuid);
    }

    return 0;
}


int CDfuPlusHelper::list()
{
    const char* name = globals->queryProp("name");

    if(name != NULL)
        info("List %s\n", name);
    else
        info("List *\n");

    Owned<IClientDFUQueryRequest> req = dfuclient->createDFUQueryRequest();
    if(name != NULL)
        req->setLogicalName(name);
    
    req->setPageSize(-1);

    Owned<IClientDFUQueryResponse> resp = dfuclient->DFUQuery(req);
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL &&  excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        info("%s\n", errmsg.str());
        return -1;
    }

    IArrayOf<IConstDFULogicalFile>& files = resp->getDFULogicalFiles();

    FILE *f = NULL;
    const char* fileName = globals->queryProp("saveto");
    if (fileName && *fileName)
    {
        f = fopen(fileName, "wb");
        if (!f)
        {
            error("Failed to open the file: %s\n", fileName);
        }
    }

    for(unsigned i = 0; i < files.length(); i++)
    {
        IConstDFULogicalFile* onefile = &files.item(i);
        if(onefile != NULL)
        {
            info("%s\n", onefile->getName());
            if (f)
            {
                StringBuffer output;
                output.append(onefile->getName());
                output.append("\n");
                fputs(output.str(),f);
            }
        }
    }

    if (f)
        fclose(f);
    return 0;
}

int CDfuPlusHelper::recover()
{
    return 0;
}

int CDfuPlusHelper::superfile(const char* action)
{
    const char* superfile = globals->queryProp("superfile");
    if(superfile == NULL || *superfile == '\0')
        throw MakeStringException(-1, "superfile name is not specified");
    
    if(stricmp(action, "add") == 0 || stricmp(action, "remove") == 0)
    {
        const char* before = globals->queryProp("before");
        StringArray subfiles;
        const char* sfprop = globals->queryProp("subfiles");
        if(sfprop != NULL && *sfprop != '\0')
        {
            const char* ptr = sfprop;
            while(*ptr != '\0')
            {
                StringBuffer onesub;
                while(*ptr != '\0' && *ptr != ',')
                {
                    onesub.append((char)(*ptr));
                    ptr++;
                }
                if(onesub.length() > 0)
                    subfiles.append(onesub.str());
                if(*ptr != '\0')
                    ptr++;
            }
        }

        Owned<IClientSuperfileActionRequest> req = dfuclient->createSuperfileActionRequest();
        req->setAction(action);
        req->setSuperfile(superfile);
        req->setSubfiles(subfiles);
        req->setBefore(before);
        req->setDelete(globals->getPropBool("delete", false));

        Owned<IClientSuperfileActionResponse> resp = dfuclient->SuperfileAction(req);

        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            info("%s\n", errmsg.str());
            return -1;
        }

        if(stricmp(action, "add") == 0)
        {
            info("Addsuper successfully finished");
        }
        else if(stricmp(action, "remove") == 0)
        {
            info("Removesuper successfully finished");
        }
    }
    else if(stricmp(action, "list") == 0)
    {
        Owned<IClientSuperfileListRequest> req = dfuclient->createSuperfileListRequest();
        req->setSuperfile(superfile);
        Owned<IClientSuperfileListResponse> resp = dfuclient->SuperfileList(req);
        
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            error("%s\n", errmsg.str());
            return -1;
        }
        StringArray& result = resp->getSubfiles();
        info("%s has %d subfiles:\n", superfile, result.length());
        for(unsigned i = 0; i < result.length(); i++)
        {
            info("\t%s\n", result.item(i));
        }
    }

    return 0;
}

int CDfuPlusHelper::savexml()
{
    const char* lfn = globals->queryProp("srcname");
    if(lfn == NULL || *lfn == '\0')
        throw MakeStringException(-1, "srcname not specified");
    
    Owned<IClientSavexmlRequest> req = dfuclient->createSavexmlRequest();
    req->setName(lfn);
    
    Owned<IClientSavexmlResponse> resp = dfuclient->Savexml(req);
    
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
        return -1;
    }

    const char* dstxml = globals->queryProp("dstxml");
    int ofile = 1;
    if(dstxml != NULL && *dstxml != '\0')
    {
        ofile = open(dstxml, _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE);
        if(ofile == -1)
            throw MakeStringException(-1, "can't open file %s\n", dstxml);
    }
    
    const MemoryBuffer& xmlmap = resp->getXmlmap();
    ssize_t written = write(ofile, xmlmap.toByteArray(), xmlmap.length());
    if (written < 0)
        throw MakeStringException(-1, "can't write to file %s\n", dstxml);
    if (written != xmlmap.length())
        throw MakeStringException(-1, "truncated write to file %s\n", dstxml);
    close(ofile);
    return 0;
}

int CDfuPlusHelper::add()
{
    const char* lfn = globals->queryProp("dstname");
    if(lfn == NULL || *lfn == '\0')
        throw MakeStringException(-1, "dstname not specified");
    
    bool isRemote = false;
    const char* xmlfname = globals->queryProp("srcxml");
    const char* srcname = globals->queryProp("srcname");
    const char* srcdali = globals->queryProp("srcdali");
    const char* srcusername = globals->queryProp("srcusername");
    const char* srcpassword = globals->queryProp("srcpassword");
    if(xmlfname == NULL || *xmlfname == '\0')
    {
        if(srcname == NULL || *srcname == '\0')
            throw MakeStringException(-1, "Please specify srcxml for adding from xml, or srcname for adding from remote dali");
        else
        {
            isRemote = true;
            if(srcdali == NULL || *srcdali == '\0')
                throw MakeStringException(-1, "srcdali not specified for adding remote");
        }
    }

    if(!isRemote)
    {
        MemoryBuffer xmlbuf;
        StringBuffer buf;
        buf.loadFile(xmlfname);
        int len = buf.length();
        xmlbuf.setBuffer(len, buf.detach(), true);

        Owned<IClientAddRequest> req = dfuclient->createAddRequest();
        req->setDstname(lfn);
        req->setXmlmap(xmlbuf);
        
        Owned<IClientAddResponse> resp = dfuclient->Add(req);
        
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            error("%s\n", errmsg.str());
            return -1;
        }
    }
    else
    {
        Owned<IClientAddRemoteRequest> req = dfuclient->createAddRemoteRequest();
        req->setDstname(lfn);
        req->setSrcname(srcname);
        req->setSrcdali(srcdali);
        if(srcusername != NULL)
            req->setSrcusername(srcusername);
        if(srcpassword != NULL)
            req->setSrcpassword(srcpassword);

        Owned<IClientAddRemoteResponse> resp = dfuclient->AddRemote(req);
        
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            error("%s\n", errmsg.str());
            return -1;
        }
    }

    info("%s successfully added", lfn);
    return 0;
}


int CDfuPlusHelper::status()
{
    const char* wuid = globals->queryProp("wuid");
    if(!wuid || !*wuid)
        throw MakeStringException(-1, "wuid not specified");

    Owned<IClientGetDFUWorkunit> req = sprayclient->createGetDFUWorkunitRequest();
    req->setWuid(wuid);

    Owned<IClientGetDFUWorkunitResponse> resp = sprayclient->GetDFUWorkunit(req);
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL &&  excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
        return -1;
    }

    IConstDFUWorkunit & dfuwu = resp->getResult();
    
    switch(dfuwu.getState())
    {
        case DFUstate_unknown:
            progress("%s status: unknown\n", wuid);
            break;
        case DFUstate_scheduled:
            progress("%s status: scheduled\n", wuid);
            break;
        case DFUstate_queued:
            progress("%s status: queued\n", wuid);
            break;
        case DFUstate_started:
            progress("%s\n", dfuwu.getProgressMessage());
            break;

        case DFUstate_aborted:
            info("%s status: aborted\n", wuid);
            return -1;

        case DFUstate_failed:
            info("%s status: failed - %s", wuid, dfuwu.getSummaryMessage());
            return -1;

        case DFUstate_finished:
            info("%s Finished\n", wuid);
            progress("%s\n", dfuwu.getSummaryMessage());
            break;
        default:
            error("%s is in an unrecognizable state - %d\n", wuid, dfuwu.getState());
    }

    return 0;

}

int CDfuPlusHelper::abort()
{
    const char* wuid = globals->queryProp("wuid");
    if(!wuid || !*wuid)
        throw MakeStringException(-1, "wuid not specified");

    Owned<IClientAbortDFUWorkunit> req = sprayclient->createAbortDFUWorkunitRequest();
    req->setWuid(wuid);

    Owned<IClientAbortDFUWorkunitResponse> resp = sprayclient->AbortDFUWorkunit(req);
    
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
    }
    else
    {
        error("Aborted %s\n", wuid);
    }
    
    return 0;
}

int CDfuPlusHelper::resubmit()
{
    const char* wuid = globals->queryProp("wuid");
    if(!wuid || !*wuid)
        throw MakeStringException(-1, "wuid not specified");

    Owned<IClientSubmitDFUWorkunit> req = sprayclient->createSubmitDFUWorkunitRequest();
    req->setWuid(wuid);

    Owned<IClientSubmitDFUWorkunitResponse> resp = sprayclient->SubmitDFUWorkunit(req);
    
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL &&  excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
    }
    else
    {
        info("Resubmitted %s\n", wuid);
    }
    
    return 0;
}

int CDfuPlusHelper::waitToFinish(const char* wuid)
{
    if(wuid == NULL || *wuid == '\0')
        return 0;

    Owned<IClientGetDFUWorkunit> req = sprayclient->createGetDFUWorkunitRequest();
    req->setWuid(wuid);

    int wait_cycle = 10;
    while(true)
    {
        Owned<IClientGetDFUWorkunitResponse> resp = sprayclient->GetDFUWorkunit(req);
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL &&  excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            error("%s\n", errmsg.str());
            break;
        }

        IConstDFUWorkunit & dfuwu = resp->getResult();

        
        switch(dfuwu.getState())
        {
            case DFUstate_unknown:
                info("%s status: unknown\n", wuid);
                break;
            case DFUstate_scheduled:
                info("%s status: scheduled\n", wuid);
                break;
            case DFUstate_queued:
                info("%s status: queued\n", wuid);
                break;
            case DFUstate_started:
                info("%s\n", dfuwu.getProgressMessage());
                break;

            case DFUstate_aborting:
                info("%s aborting\n", wuid);
                break;

            case DFUstate_aborted:
                info("%s aborted\n", wuid);
                return -1;

            case DFUstate_failed:
                {
                    const char* errmsg = dfuwu.getSummaryMessage();
                    if(errmsg && *errmsg)
                        info("%s\n", errmsg);
                    else
                        info("%s failed.\n", wuid);
                    return -1;
                }
            case DFUstate_finished:
                info("%s Finished\n", wuid);
                info("%s\n", dfuwu.getSummaryMessage());
                return 0;
        }

        sleep(wait_cycle);
        // make it wait shorter time at the beginning to avoid waiting too long for short jobs.
        if(wait_cycle < WAIT_SECONDS)
            wait_cycle = (wait_cycle+10<WAIT_SECONDS)?(wait_cycle+10):WAIT_SECONDS;
    }

    return 0;

}

int CDfuPlusHelper::updatejobname(const char* wuid, const char* jobname)
{
    if(!wuid || !*wuid)
        throw MakeStringException(-1, "wuid not specified");
    if(!jobname || !*jobname)
        throw MakeStringException(-1, "jobname not specified");

    Owned<IClientGetDFUWorkunit> req = sprayclient->createGetDFUWorkunitRequest();
    req->setWuid(wuid);

    Owned<IClientGetDFUWorkunitResponse> resp = sprayclient->GetDFUWorkunit(req);
    const IMultiException* excep = &resp->getExceptions();
    if(excep != NULL &&  excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
        return -1;
    }

    IConstDFUWorkunit & dfuwu1 = resp->getResult();
    IEspDFUWorkunit& dfuwu = dynamic_cast<IEspDFUWorkunit&>(dfuwu1);
    dfuwu.setJobName(jobname);

    Owned<IClientUpdateDFUWorkunit> updatereq = sprayclient->createUpdateDFUWorkunitRequest();
    updatereq->setWu(dfuwu);
    updatereq->setStateOrig(dfuwu.getState());
    sprayclient->UpdateDFUWorkunit(updatereq);
    return 0;
}

void CDfuPlusHelper::exc(const IMultiException& excep,const char *title)
{
    StringBuffer errmsg;
    excep.errorMessage(errmsg);
    error("%s\n",errmsg.str());
}

void CDfuPlusHelper::info(const char *fmt, ...) 
{
    va_list args;
    va_start(args, fmt);
    StringBuffer buf;
    buf.valist_appendf(fmt,args);
    va_end(args);
    if (msgintercept) {
        StringArray strs;
        strs.appendList(buf.str(), "\n");
        ForEachItemIn(i,strs) {
            const char *s = strs.item(i);
            if (*s)
                msgintercept->info(s);
        }
    }
    else
        printf("%s",buf.str());
}

void CDfuPlusHelper::progress(const char *fmt, ...) 
{
    va_list args;
    va_start(args, fmt);
    StringBuffer buf;
    buf.valist_appendf(fmt,args);
    va_end(args);
    if (msgintercept) {
        StringArray strs;
        strs.appendList(buf.str(), "\n");
        ForEachItemIn(i,strs) {
            const char *s = strs.item(i);
            if (*s)
                PROGLOG("%s",s);
        }
    }
    else
        printf("%s",buf.str());
}

void CDfuPlusHelper::error(const char *fmt, ...) 
{
    va_list args;
    va_start(args, fmt);
    StringBuffer buf;
    buf.valist_appendf(fmt,args);
    va_end(args);
    if (msgintercept) {
        unsigned l = buf.length();
        const char *s = buf.str();
        while (l&&(s[l-1]=='\n'))
            l--;
        buf.setLength(l);
        s = buf.str();
        while (*s&&(*s=='\n'))
            s++;
        msgintercept->err(s);
    }
    else
        printf("%s",buf.str());
}
