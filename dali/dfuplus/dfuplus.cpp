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
#include "rmtclient.hpp"
#include "dafsserver.hpp"

#include "jutil.hpp"

static class CSecuritySettings
{
    DAFSConnectCfg connectMethod;
    unsigned short daliServixPort;
    unsigned short daliServixSSLPort;
public:
    CSecuritySettings()
    {
        queryDafsSecSettings(&connectMethod, &daliServixPort, &daliServixSSLPort, nullptr, nullptr, nullptr);
    }

    DAFSConnectCfg queryDAFSConnectCfg() { return connectMethod; }
    unsigned short queryDaliServixPort() { return daliServixPort; }
    unsigned short queryDaliServixSSLPort() { return daliServixSSLPort; }
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
#if 0
        StringBuffer eps;
        if (listenep.isNull())
            eps.append(listenep.port);
        else
            listenep.getUrlStr(eps);
#endif
        server.setown(createRemoteFileServer());
        server->setThrottle(ThrottleStd, 0); // disable throttling
        server->setThrottle(ThrottleSlow, 0); // disable throttling
    }

    int run()
    {
        try {
            server->run(securitySettings.queryDAFSConnectCfg(), listenep);
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

    bool ok() { return exc.get()==nullptr; }

    unsigned idleTime() { return server.get()?server->idleTime():0; }

};

bool CDfuPlusHelper::runLocalDaFileSvr(SocketEndpoint &listenep,bool requireauthenticate, unsigned timeout)
{
    Owned<CDafsThread> thr = new CDafsThread(listenep,requireauthenticate);
    if (!thr->ok())
        return false;

    unsigned port = securitySettings.queryDaliServixPort();
    unsigned sslport = securitySettings.queryDaliServixSSLPort();

    DAFSConnectCfg connectMethod = securitySettings.queryDAFSConnectCfg();

    StringBuffer addlPort;
    SocketEndpoint printep(listenep);
    if (printep.isNull())
    {
        if (connectMethod == SSLNone)
            addlPort.appendf("port %u", port);
        else if (connectMethod == SSLOnly)
            addlPort.appendf("port %u", sslport);
        else if (connectMethod == SSLFirst)
            addlPort.appendf("ports %u:%u", sslport, port);
        else
            addlPort.appendf("ports %u:%u", port, sslport);
        progress("Started local Dali file server on %s\n", addlPort.str());
    }
    else
    {
        if (connectMethod == SSLNone)
            printep.port = port;
        else if (connectMethod == SSLOnly)
            printep.port = sslport;
        else if (connectMethod == SSLFirst)
        {
            printep.port = sslport;
            addlPort.appendf(":%u", port);
        }
        else
        {
            printep.port = port;
            addlPort.appendf(":%u", sslport);
        }
        StringBuffer eps;
        progress("Started local Dali file server on %s%s\n", printep.getUrlStr(eps).str(), addlPort.str());
    }

    thr->start();

    if (timeout==0) {
        setDafsTrace(nullptr,0); // disable client tracing
        dafsthread.setown(thr.getClear());
    }
    else {
        for (;;) {
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
    unsigned port = securitySettings.queryDaliServixPort();
    unsigned sslport = securitySettings.queryDaliServixSSLPort();

    DAFSConnectCfg connectMethod = securitySettings.queryDAFSConnectCfg();

    unsigned dafsPort;
    StringBuffer addlPort;
    if (connectMethod == SSLNone)
    {
        dafsPort = port;
        addlPort.appendf("port %u", port);
    }
    else if (connectMethod == SSLOnly)
    {
        dafsPort = sslport;
        addlPort.appendf("port %u", sslport);
    }
    else if (connectMethod == SSLFirst)
    {
        dafsPort = sslport;
        addlPort.appendf("ports %u:%u", sslport, port);
    }
    else
    {
        dafsPort = port;
        addlPort.appendf("ports %u:%u", port, sslport);
    }
    if (!eps||!*eps)
        epout.setLocalHost(dafsPort);
    else {
        epout.set(eps,dafsPort);
        if (!epout.isLocal())
            return false;
    }
    progress("Checking for local Dali File Server on %s\n", addlPort.str());

    unsigned short nPort = getActiveDaliServixPort(epout);
    if (nPort)
    {
        epout.port = nPort;
        return true;
    }

    return runLocalDaFileSvr(epout,false,0);
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
    if(server == nullptr)
        throw MakeStringException(-1, "Server url not specified");

    StringBuffer url;
    if(Utils::strncasecmp(server, "http://", 7) != 0 && Utils::strncasecmp(server, "https://", 8) != 0)
        url.append("http://");
    url.append(server);

    if(strchr(url.str() + 7, ':') == nullptr)
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

    sprayclient->setUsernameToken(username, password, nullptr);
    dfuclient->setUsernameToken(username, password, nullptr);

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
    if(action == nullptr || *action == '\0')
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
    else if(stricmp(action, "listhistory") == 0)
        return listhistory();
    else if(stricmp(action, "erasehistory") == 0)
        return erasehistory();
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
    if(srcxml == nullptr)
    {
        req->setSourceIP(srcip);
        req->setSourcePath(srcfile);
    }
    else
        req->setSrcxml(xmlbuf);

    if(recordsize != 0)
        req->setSourceRecordSize(recordsize);

    if(dstcluster != nullptr)
        req->setDestGroup(dstcluster);
    req->setDestLogicalName(dstname);
    req->setOverwrite(globals->getPropBool("overwrite", false));
    req->setReplicate(globals->getPropBool("replicate", true));
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    if(globals->hasProp("prefix"))
        req->setPrefix(globals->queryProp("prefix"));
    if(globals->hasProp("nosplit"))
        req->setNosplit(globals->getPropBool("nosplit", false));
    if(globals->hasProp("nocommon"))
        req->setNoCommon(globals->getPropBool("nocommon", false));
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

    if(globals->hasProp("expireDays"))
        req->setExpireDays(globals->getPropInt("expireDays"));

    if(srcxml == nullptr)
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
    if(srcxml == nullptr)
    {
        req->setSourceIP(srcip);
        req->setSourcePath(srcfile);
    }
    else
    {
        req->setSrcxml(xmlbuf);
    }

    const char* mrsstr = globals->queryProp("maxRecordSize");
    if(mrsstr != nullptr)
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
        if(encoding == nullptr)
            encoding = "utf8";
        else if(stricmp(encoding, "ascii") == 0)
            throw MakeStringException(-1, "xml format only accepts utf encodings");
        if(rowtag == nullptr || *rowtag == '\0')
            throw MakeStringException(-1, "rowtag not specified.");
        if(rowpath && *rowpath)
            throw MakeStringException(-1, "You can't use rowpath option with xml format");
    }
    else if(stricmp(format, "csv") == 0)
    {
        if(encoding == nullptr)
            encoding = "ascii";

        if(rowtag != nullptr && *rowtag != '\0')
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
    if(rowtag != nullptr)
        req->setSourceRowTag(rowtag);

    if(dstcluster != nullptr)
        req->setDestGroup(dstcluster);
    req->setDestLogicalName(dstname);
    req->setOverwrite(globals->getPropBool("overwrite", false));
    req->setReplicate(globals->getPropBool("replicate", true));
    req->setReplicateOffset(globals->getPropInt("replicateoffset",1));
    if(globals->hasProp("prefix"))
        req->setPrefix(globals->queryProp("prefix"));
    if(globals->hasProp("nosplit"))
        req->setNosplit(globals->getPropBool("nosplit", false));
    if(globals->hasProp("nocommon"))
        req->setNoCommon(globals->getPropBool("nocommon", false));
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

    if(globals->hasProp("expireDays"))
        req->setExpireDays(globals->getPropInt("expireDays"));

    if(srcxml == nullptr)
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

    if(srcxml == nullptr)
    {
        if(srcfile == nullptr)
            throw MakeStringException(-1, "srcfile not specified");
        if(srcip == nullptr) {
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
        if(srcip != nullptr || srcfile != nullptr)
            throw MakeStringException(-1, "srcip/srcfile and srcxml can't be used at the same time");
        StringBuffer buf;
        buf.loadFile(srcxml);
        int len = buf.length();
        xmlbuf.setBuffer(len, buf.detach(), true);
    }

    const char* dstname = globals->queryProp("dstname");
    if(dstname == nullptr)
        throw MakeStringException(-1, "dstname not specified");
    const char* dstcluster = globals->queryProp("dstcluster");
    if(dstcluster == nullptr)
        throw MakeStringException(-1, "dstcluster not specified");
    const char* format = globals->queryProp("format");
    if(format == nullptr)
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
    if(srcname == nullptr)
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
    if(wuid == nullptr || *wuid == '\0')
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
    if(srcname == nullptr)
        throw MakeStringException(-1, "srcname not specified");

    const char* dstxml = globals->queryProp("dstxml");
    const char* dstip = globals->queryProp("dstip");
    const char* dstfile = globals->queryProp("dstfile");

    bool nowait = globals->getPropBool("nowait", false);

    MemoryBuffer xmlbuf;
    if(dstxml == nullptr)
    {
        if(dstfile == nullptr)
            throw MakeStringException(-1, "dstfile not specified");
        if(dstip == nullptr) {
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
        if(dstip != nullptr || dstfile != nullptr)
            throw MakeStringException(-1, "dstip/dstfile and dstxml can't be used at the same time");
        StringBuffer buf;
        buf.loadFile(dstxml);
        int len = buf.length();
        xmlbuf.setBuffer(len, buf.detach(), true);
    }

    if(dstxml == nullptr)
        info("\nDespraying %s to host %s file %s\n", srcname, dstip, dstfile);
    else
        info("\nDespraying %s\n", srcname);

    Owned<IClientDespray> req = sprayclient->createDesprayRequest();
    req->setSourceLogicalName(srcname);
    if(dstxml == nullptr)
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
    if(wuid == nullptr || *wuid == '\0')
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
    if(srcname == nullptr)
        throw MakeStringException(-1, "srcname not specified");
    const char* dstname = globals->queryProp("dstname");
    if(dstname == nullptr)
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
    if(dstcluster != nullptr)
        req->setDestGroup(dstcluster);
    if(dstclusterroxie && !stricmp(dstclusterroxie, "Yes"))
        req->setDestGroupRoxie("Yes");
    if(srcdali !=  nullptr)
        req->setSourceDali(srcdali);
    if(diffkeysrc !=  nullptr)
        req->setSourceDiffKeyName(diffkeysrc);
    if(diffkeydst !=  nullptr)
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
    if(globals->hasProp("nocommon"))
        req->setNoCommon(globals->getPropBool("nocommon", false));
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

    if(globals->hasProp("expireDays"))
        req->setExpireDays(globals->getPropInt("expireDays"));

    Owned<IClientCopyResponse> result = sprayclient->Copy(req);
    const char* wuid = result->getResult();
    if(wuid == nullptr || *wuid == '\0')
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
    if(srcname == nullptr)
        throw MakeStringException(-1, "srcname not specified");
    const char* dstname = globals->queryProp("dstname");
    if(dstname == nullptr)
        throw MakeStringException(-1, "dstname not specified");
    const char* dstcluster = globals->queryProp("dstcluster");
    if(dstcluster == nullptr)
        throw MakeStringException(-1, "dstcluster not specified");
    const char* srcdali = globals->queryProp("srcdali");
    if(srcdali == nullptr)
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
    if(globals->hasProp("nocommon"))
        req->setNoCommon(globals->getPropBool("nocommon", false));
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
    const char* wuid = result->getResult();
    if(wuid == nullptr || *wuid == '\0')
        exc(result->getExceptions(),"copying");
    else
    {
        const char* jobname = globals->queryProp("jobname");
        if(jobname && *jobname)
            updatejobname(wuid, jobname);

        info("Superfile copy completed, WUID: %s\n", wuid);
    }


    return 0;
}

int CDfuPlusHelper::monitor()
{
    const char* eventname = globals->queryProp("event");
    const char* lfn = globals->queryProp("lfn");
    const char* eps = globals->queryProp("ip");
    const char* filename = globals->queryProp("file");
    bool sub = globals->getPropBool("sub");
    if ((lfn == nullptr) && (filename == nullptr))
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
    if(wuid == nullptr || *wuid == '\0')
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
    if(excep != nullptr && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        if(errmsg.length() > 0)
            error("%s\n", errmsg.str());
        return -1;
    }

    StringBuffer resultbuf;
    IArrayOf<IConstDFUActionInfo>& actionResults = resp->getActionResults();
    if (actionResults.ordinality())
    {
        ForEachItemIn(i, actionResults)
        {
            IConstDFUActionInfo& actionResult = actionResults.item(i);
            resultbuf.append(actionResult.getActionResult()).append("\n");
        }
    }
    else
    {
        const char* result = resp->getDFUArrayActionResult();
        if(result != nullptr && *result != '\0')
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
    }
    info("%s\n", resultbuf.str());

    return 0;
}

int CDfuPlusHelper::rename()
{
    const char* srcname = globals->queryProp("srcname");
    if(srcname == nullptr)
        throw MakeStringException(-1, "srcname not specified");
    const char* dstname = globals->queryProp("dstname");
    if(dstname == nullptr)
        throw MakeStringException(-1, "dstname not specified");

    bool nowait = globals->getPropBool("nowait", false);

    info("\nRenaming from %s to %s\n", srcname, dstname);

    Owned<IClientRename> req = sprayclient->createRenameRequest();
    req->setSrcname(srcname);
    req->setDstname(dstname);

    Owned<IClientRenameResponse> result = sprayclient->Rename(req);
    const char* wuid = result->getWuid();
    if(wuid == nullptr || *wuid == '\0')
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

    if(name != nullptr)
        info("List %s\n", name);
    else
        info("List *\n");

    Owned<IClientDFUQueryRequest> req = dfuclient->createDFUQueryRequest();
    if(name != nullptr)
        req->setLogicalName(name);

    req->setPageSize(-1);

    Owned<IClientDFUQueryResponse> resp = dfuclient->DFUQuery(req);
    const IMultiException* excep = &resp->getExceptions();
    if(excep != nullptr &&  excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        info("%s\n", errmsg.str());
        return -1;
    }

    IArrayOf<IConstDFULogicalFile>& files = resp->getDFULogicalFiles();

    FILE *f = nullptr;
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
        if(onefile != nullptr)
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
    if(superfile == nullptr || *superfile == '\0')
        throw MakeStringException(-1, "superfile name is not specified");

    if(stricmp(action, "add") == 0 || stricmp(action, "remove") == 0)
    {
        const char* before = globals->queryProp("before");
        StringArray subfiles;
        const char* sfprop = globals->queryProp("subfiles");
        if(sfprop != nullptr && *sfprop != '\0')
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
        if(excep != nullptr && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            info("%s\n", errmsg.str());
            return -1;
        }

        if(stricmp(action, "add") == 0)
        {
            info("Addsuper successfully finished\n");
        }
        else if(stricmp(action, "remove") == 0)
        {
            info("Removesuper successfully finished\n");
        }
    }
    else if(stricmp(action, "list") == 0)
    {
        Owned<IClientSuperfileListRequest> req = dfuclient->createSuperfileListRequest();
        req->setSuperfile(superfile);
        Owned<IClientSuperfileListResponse> resp = dfuclient->SuperfileList(req);

        const IMultiException* excep = &resp->getExceptions();
        if(excep != nullptr && excep->ordinality() > 0)
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
    if(lfn == nullptr || *lfn == '\0')
        throw MakeStringException(-1, "srcname not specified");

    Owned<IClientSavexmlRequest> req = dfuclient->createSavexmlRequest();
    req->setName(lfn);

    Owned<IClientSavexmlResponse> resp = dfuclient->Savexml(req);

    const IMultiException* excep = &resp->getExceptions();
    if(excep != nullptr && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
        return -1;
    }

    const char* dstxml = globals->queryProp("dstxml");
    int ofile = 1;
    if(dstxml != nullptr && *dstxml != '\0')
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
    if(lfn == nullptr || *lfn == '\0')
        throw MakeStringException(-1, "dstname not specified");

    bool isRemote = false;
    const char* xmlfname = globals->queryProp("srcxml");
    const char* srcname = globals->queryProp("srcname");
    const char* srcdali = globals->queryProp("srcdali");
    const char* srcusername = globals->queryProp("srcusername");
    const char* srcpassword = globals->queryProp("srcpassword");
    if(xmlfname == nullptr || *xmlfname == '\0')
    {
        if(srcname == nullptr || *srcname == '\0')
            throw MakeStringException(-1, "Please specify srcxml for adding from xml, or srcname for adding from remote dali");
        else
        {
            isRemote = true;
            if(srcdali == nullptr || *srcdali == '\0')
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
        if(excep != nullptr && excep->ordinality() > 0)
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
        if(srcusername != nullptr)
            req->setSrcusername(srcusername);
        if(srcpassword != nullptr)
            req->setSrcpassword(srcpassword);

        Owned<IClientAddRemoteResponse> resp = dfuclient->AddRemote(req);

        const IMultiException* excep = &resp->getExceptions();
        if(excep != nullptr && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            error("%s\n", errmsg.str());
            return -1;
        }
    }

    info("%s successfully added\n", lfn);
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
    if(excep != nullptr &&  excep->ordinality() > 0)
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
            info("%s status: failed - %s\n", wuid, dfuwu.getSummaryMessage());
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
    if(excep != nullptr && excep->ordinality() > 0)
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

StringBuffer & historyToXml(const IArrayOf<IConstHistory>& arrHistory, StringBuffer & out)
{
    out.append("<History>\n");
    for(unsigned i = 0; i < arrHistory.length(); i++)
    {
        IConstHistory* historyRecord = &arrHistory.item(i);
        if(historyRecord != nullptr)
        {
            out.appendf("\t<Origin ip=\"%s\"\n", historyRecord->getIP());
            out.appendf("\t\tname=\"%s\"\n", historyRecord->getName());
            out.appendf("\t\toperation=\"%s\"\n", historyRecord->getOperation());
            out.appendf("\t\towner=\"%s\"\n", historyRecord->getOwner());
            out.appendf("\t\tpath=\"%s\"\n", historyRecord->getPath());
            out.appendf("\t\ttimestamp=\"%s\"\n", historyRecord->getTimestamp());
            out.appendf("\t\tworkunit=\"%s\"/>\n", historyRecord->getWorkunit());
        }
    }
    out.append("</History>\n");
    return out;
}

int CDfuPlusHelper::listhistory()
{
    const char* lfn = globals->queryProp("lfn");
    if (isEmptyString(lfn))
        throw MakeStringException(-1, "srcname not specified");

    Owned<IClientListHistoryRequest> req = dfuclient->createListHistoryRequest();
    req->setName(lfn);

    Owned<IClientListHistoryResponse> resp = dfuclient->ListHistory(req);

    const IMultiException* excep = &resp->getExceptions();
    if (excep != nullptr && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
        return -1;
    }

    typedef enum
    {
        xml = 0,
        ascii = 1,
        csv = 2,
        json = 3,
    } out_t;

    out_t format;

    const char *formatPar = globals->queryProp("outformat");
    if (formatPar == nullptr)
        format = xml;
    else if (stricmp(formatPar, "csv") == 0)
        format=csv;
    else if (stricmp(formatPar, "xml") == 0)
        format=xml;
    else if (stricmp(formatPar, "json") == 0)
        format=json;
    else if (stricmp(formatPar, "ascii") == 0)
        format=ascii;
    else
    {
        error("Unsupported output format: '%s'\n",formatPar);
        return -1;
    }

    IArrayOf<IConstHistory>& arrHistory = resp->getHistory();
    if (0 == arrHistory.length())
    {
        error("%s doesn't have stored history!\n", lfn);
        return -2;
    }

    switch (format)
    {
        case xml:
        {
            StringBuffer xmlDump;
            historyToXml(arrHistory, xmlDump);
            info("\n%s\n",xmlDump.str());
        }
        break;

        case ascii:
        {
            progress("\nHistory of %s:\n", lfn);
            unsigned index = 1;
            StringBuffer asciiDump;
            for(unsigned i = 0; i < arrHistory.length(); i++)
            {
                asciiDump.append(index++);
                IConstHistory* historyRecord = &arrHistory.item(i);
                if(historyRecord != nullptr)
                {
                    asciiDump.appendf(", ip=\"%s\"", historyRecord->getIP());
                    asciiDump.appendf(", name=\"%s\"", historyRecord->getName());
                    asciiDump.appendf(", operation=\"%s\"", historyRecord->getOperation());
                    asciiDump.appendf(", owner=\"%s\"", historyRecord->getOwner());
                    asciiDump.appendf(", path=\"%s\"", historyRecord->getPath());
                    asciiDump.appendf(", timestamp=\"%s\"", historyRecord->getTimestamp());
                    asciiDump.appendf(", workunit=\"%s\"/>\n", historyRecord->getWorkunit());
                }
            }
            info("%s", asciiDump.str());
        }
        break;

        case csv:
        {
            // TODO We need more sophisticated method if the record structure change in the future
            bool showCsvHeader = globals->getPropBool("csvheader", true);
            StringBuffer csvDump("\n");

            if (showCsvHeader)
            {
                // header
                csvDump.append("ip,name,operation,owner,path,timestamp,workunit\n");
            }

            for(unsigned i = 0; i < arrHistory.length(); i++)
            {
                IConstHistory* historyRecord = &arrHistory.item(i);
                if(historyRecord != nullptr)
                {
                    csvDump.appendf("%s,", historyRecord->getIP());
                    csvDump.appendf("%s,", historyRecord->getName());
                    csvDump.appendf("%s,", historyRecord->getOperation());
                    csvDump.appendf("%s,", historyRecord->getOwner());
                    csvDump.appendf("%s,", historyRecord->getPath());
                    csvDump.appendf("%s,", historyRecord->getTimestamp());
                    csvDump.appendf("%s\n",  historyRecord->getWorkunit());
                }
            }
            info("%s", csvDump.str());
        }
        break;

        case json:
        {
            StringBuffer jsonDump;
            const char * level1 = "    ";        // 4 space 'tab'
            const char * level2 = "        ";
            const char * level3 = "            ";
            //                     123456789012

            unsigned index = 1;

            jsonDump.append("{\n");       // Level 0: Start History object

            jsonDump.appendf("%s\"%s\": [", level1, "History");   // Begin History object's array of historical records

            for(unsigned i = 0; i < arrHistory.length(); i++)
            {
                if (index++ != 1)
                    jsonDump.append(",");

                jsonDump.appendf("\n%s{\n", level2);    // Historical record object begin

                bool first = true;
                IConstHistory* historyRecord = &arrHistory.item(i);
                if(historyRecord != nullptr)
                {
                    // Historical record item object
                    jsonDump.append(level3);
                    appendJSONValue(jsonDump, "ip", historyRecord->getIP());
                    jsonDump.appendf(",\n%s",level3);

                    appendJSONValue(jsonDump, "name",historyRecord->getName());
                    jsonDump.appendf(",\n%s",level3);

                    appendJSONValue(jsonDump, "operation", historyRecord->getOperation());
                    jsonDump.appendf(",\n%s",level3);

                    appendJSONValue(jsonDump, "owner", historyRecord->getOwner());
                    jsonDump.appendf(",\n%s",level3);

                    appendJSONValue(jsonDump, "path", historyRecord->getPath());
                    jsonDump.appendf(",\n%s",level3);

                    appendJSONValue(jsonDump, "timestamp", historyRecord->getTimestamp());
                    jsonDump.appendf(",\n%s",level3);

                    appendJSONValue(jsonDump, "workunit", historyRecord->getWorkunit());
                    jsonDump.append("\n");
                }
                jsonDump.appendf("\n%s}", level2);   // Historical record object end
            }
            jsonDump.appendf("\n%s]\n", level1);   // History object array end

            jsonDump.appendf("}\n");       // Level 0 : History object end

            info("\n%s\n",jsonDump.str());
        }
        break;
    }
    return 0;
}

int CDfuPlusHelper::erasehistory()
{
    const char* lfn = globals->queryProp("lfn");
    if (isEmptyString(lfn))
        throw MakeStringException(-1, "srcname not specified");

    bool backup = globals->getPropBool("backup", true);
    const char* dstxml = globals->queryProp("dstxml");
    if (backup && isEmptyString(dstxml))
        throw MakeStringException(-1, "dstxml not specified");

    progress("\nErase history of '%s' with%s backup.\n", lfn, (backup ? "" : "out"));

    if (backup)
    {
        // Get and backup file history before erased.
        // If any problem happens during the backup the history remain intact.
        Owned<IClientListHistoryRequest> req = dfuclient->createListHistoryRequest();
        req->setName(lfn);

        Owned<IClientListHistoryResponse> resp = dfuclient->ListHistory(req);

        const IMultiException* excep = &resp->getExceptions();
        if (excep != nullptr && excep->ordinality() > 0)
        {
            StringBuffer errmsg;
            excep->errorMessage(errmsg);
            error("%s\n", errmsg.str());
            return -1;
        }

        IArrayOf<IConstHistory>& arrHistory = resp->getHistory();
        if (0 == arrHistory.length())
        {
            error("%s doesn't have stored history!\n", lfn);
            return -2;
        }

        StringBuffer xmlDump;
        historyToXml(arrHistory, xmlDump);

        info("\n%s\n",xmlDump.str());

        int ofile = open(dstxml, _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE);
        if(ofile == -1)
            throw MakeStringException(-1, "can't open file %s, erase history cancelled.\n", dstxml);

        ssize_t written = write(ofile, xmlDump.str(), xmlDump.length());
        if (written < 0)
            throw MakeStringException(-1, "can't write to file %s, erase history cancelled.\n", dstxml);

        if (written != xmlDump.length())
            throw MakeStringException(-1, "truncated write to file %s, erase history cancelled.\n", dstxml);

        close(ofile);

        info("History written into %s.\n",dstxml);
    }

    Owned<IClientEraseHistoryRequest> req = dfuclient->createEraseHistoryRequest();
    req->setName(lfn);

    Owned<IClientEraseHistoryResponse> resp = dfuclient->EraseHistory(req);

    const IMultiException* excep = &resp->getExceptions();
    if (excep != nullptr && excep->ordinality() > 0)
    {
        StringBuffer errmsg;
        excep->errorMessage(errmsg);
        error("%s\n", errmsg.str());
        return -1;
    }

    info("History erased.\n");

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
    if(excep != nullptr &&  excep->ordinality() > 0)
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
    if(wuid == nullptr || *wuid == '\0')
        return 0;

    Owned<IClientGetDFUWorkunit> req = sprayclient->createGetDFUWorkunitRequest();
    req->setWuid(wuid);

    int wait_cycle = 10;
    while(true)
    {
        Owned<IClientGetDFUWorkunitResponse> resp = sprayclient->GetDFUWorkunit(req);
        const IMultiException* excep = &resp->getExceptions();
        if(excep != nullptr &&  excep->ordinality() > 0)
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
    if(excep != nullptr &&  excep->ordinality() > 0)
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
