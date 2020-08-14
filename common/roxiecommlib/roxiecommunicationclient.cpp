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

#pragma warning (disable : 4786)

#include "jmisc.hpp"
#include "portlist.h"
#include "roxiecommlib.hpp"



#define GET_LOCK_FAILURE            1100
#define EMPTY_RESULT_FAILURE        1200

//////////////////////////////////////////
class CRoxieCommunicationClient: implements IRoxieCommunicationClient, public CInterface
{
private:
    void processRoxieQueryList(IPropertyTree *info)
    {
        StringAttrMapping aliasOriginalNames(false);

        Owned<IPropertyTreeIterator> alaises = info->getElements("Endpoint/Alias");
        ForEach (*alaises)
        {
            IPropertyTree &alias = alaises->query();
            const char *id = alias.queryProp("@id");
            const char *original = alias.queryProp("@original");
            aliasOriginalNames.setValue(original, original);
        }

        Owned<IPropertyTreeIterator> queries = info->getElements("Endpoint/Query");
        ForEach (*queries)
        {
            IPropertyTree &query = queries->query();
            const char *id = query.queryProp("@id");
            // look for id in the stringArray
            if (aliasOriginalNames.find(id) != 0)
                query.setProp("@hasAlias", "1");
            else
                query.setProp("@hasAlias", "0");
        }
    }

protected:
    SocketEndpoint ep;
    unsigned roxieTimeout;

    IPropertyTree *sendRoxieControlQuery(const char *xml, bool deployAll, const char *remoteRoxieIP = NULL)
    {
        unsigned len = strlen(xml);
        size32_t sendlen = len;
        _WINREV(sendlen);
        Owned<ISocket> sock = getRoxieSocket(remoteRoxieIP);
        if (deployAll)
            if (!getLock(sock, deployAll))  // if we want to deploy to all roxie servers, make sure we can lock all servers
                throw MakeStringException(GET_LOCK_FAILURE, "Request Failed! All roxie nodes unable to process this request at this time.  Roxie is busy - possibly in the middle of another deployment.  Try again later, if problem persists, make sure all nodes are running");

        sock->write(&sendlen, sizeof(sendlen));
        sock->write(xml, len);
        StringBuffer response;
        for (;;)
        {
            sock->read(&sendlen, sizeof(sendlen));
            if (!sendlen)
                break;
            _WINREV(sendlen);
            sock->read(response.reserveTruncate(sendlen), sendlen);
        }
        if (response.isEmpty())
            throw MakeStringException(EMPTY_RESULT_FAILURE, "CRoxieCommunicationClient empty response for control request(%s)", xml);
        Owned<IPropertyTree> ret = createPTreeFromXMLString(response.str());
        if (!ret)
            throw MakeStringException(EMPTY_RESULT_FAILURE, "CRoxieCommunicationClient empty result tree for control request(%s)", xml);
        Owned<IMultiException> me = MakeMultiException();
        Owned<IPropertyTreeIterator> endpoints = ret->getElements("Endpoint");
        ForEach(*endpoints)
        {
            Owned<IPropertyTreeIterator> exceptions = endpoints->query().getElements("Exception");
            ForEach (*exceptions)
            {
                me->append(*MakeStringException(exceptions->query().getPropInt("Code"), "Endpoint %s: %s", endpoints->query().queryProp("@ep"), exceptions->query().queryProp("Message")));
            }
        }
        if (me->ordinality())
            throw me.getClear();
        return ret.getClear();

    }


    const char *sendRoxieOnDemandQuery(const char *xml, SCMStringBuffer &response, bool deployAll, const char *remoteRoxieIP = NULL)
    {
        unsigned len = strlen(xml);
        size32_t sendlen = len;
        _WINREV(sendlen);
        Owned<ISocket> sock = getRoxieSocket(remoteRoxieIP);
        if (deployAll)
            if (!getLock(sock, deployAll))  // if we want to deploy to all roxie servers, make sure we can lock all servers
                throw MakeStringException(GET_LOCK_FAILURE, "Request Failed! All roxie nodes unable to process this request at this time.  Roxie is busy - possibly in the middle of another deployment.  Try again later, if problem persists, make sure all nodes are running");

        sock->write(&sendlen, sizeof(sendlen));
        sock->write(xml, len);

        Owned<IException> exception;
        for (;;)
        {
            sock->read(&sendlen, sizeof(sendlen));
            if (!sendlen)
                break;
            _WINREV(sendlen);
            char *block = response.s.reserveTruncate(sendlen);
            sock->read(block, sendlen);
            if (!exception && sendlen > 11 && memicmp(block, "<Exception>", 11) == 0)
            {
                Owned<IPropertyTree> eTree = createPTreeFromXMLString(sendlen, block, ipt_caseInsensitive);
                exception.setown(MakeStringException(eTree->getPropInt("Code", 0), "%s", eTree->queryProp("Message")));
            }
        }
        if (exception)
            throw exception.getClear();

        return response.str();

    }

    IPropertyTree *sendRoxieControlQuery(IPropertyTree *pt)
    {
        StringBuffer xml;
        toXML(pt, xml);
        return sendRoxieControlQuery(xml, true);
    }

    bool sendRoxieControlLock(ISocket *sock, bool allOrNothing, unsigned wait)
    {
        Owned<IPropertyTree> resp = sendRoxieControlQuery(sock, "<control:lock/>", wait);
        if (allOrNothing)
        {
            int lockCount = resp->getPropInt("Lock", 0);
            int serverCount = resp->getPropInt("NumServers", 0);
            return (lockCount && (lockCount == serverCount));
        }

        return resp->getPropInt("Lock", 0) != 0;
    }

    void checkRoxieControlExceptions(IPropertyTree *msg)
    {
        Owned<IMultiException> me = MakeMultiException();
        Owned<IPropertyTreeIterator> endpoints = msg->getElements("Endpoint");
        ForEach(*endpoints)
        {
            IPropertyTree &endp = endpoints->query();
            Owned<IPropertyTreeIterator> exceptions = endp.getElements("Exception");
            ForEach (*exceptions)
            {
                IPropertyTree &ex = exceptions->query();
                me->append(*MakeStringException(ex.getPropInt("Code"), "Endpoint %s: %s", endp.queryProp("@ep"), ex.queryProp("Message")));
            }
        }
        if (me->ordinality())
            throw me.getClear();
    }

    unsigned waitMsToSeconds(unsigned wait)
    {
        if (wait==0 || wait==(unsigned)-1)
            return wait;
        return wait/1000;
    }

    unsigned remainingMsWait(unsigned wait, unsigned start)
    {
        if (wait==0 || wait==(unsigned)-1)
            return wait;
        unsigned waited = msTick()-start;
        return (wait>waited) ? wait-waited : 0;
    }

    const char* buildRoxieName(const char* orig_name, StringBuffer& roxiename)
    {
        const char *last_dot = strrchr(orig_name, '.');
        if (last_dot)
            roxiename.append((last_dot - orig_name), orig_name);
        else
            roxiename.append(orig_name);

        return roxiename.str();
    }

    ISocket *getRoxieSocket(const char *remoteIP)
    {
        ISocket* s = NULL;
        try
        {
            if (remoteIP)
            {
                SocketEndpoint roxie_ep;
                roxie_ep.set(remoteIP);
                return ISocket::connect_timeout(roxie_ep, 20000);
            }

            s = ISocket::connect_timeout(ep, 20000);
        }
        catch(IException* e)
        {
            e->Release();
        }
        catch(...)
        {
        }

        if (s==NULL)
        {
            StringBuffer buf;
            buf.append("Failed to connect to Roxie cluster at ");

            if (remoteIP)
                buf.append(remoteIP);
            else
                ep.getUrlStr(buf);

            throw MakeStringException(ROXIECOMM_SOCKET_ERROR, "%s", buf.str());
        }
        return s;
    }

    bool getLock(ISocket *sock, bool lockAll)
    {
        const char *lock = "<control:lock/>";
        unsigned locklen = strlen(lock);
        _WINREV(locklen);
        sock->write(&locklen, sizeof(locklen));
        sock->write(lock, strlen(lock));
        StringBuffer lockResponse;
        for (;;)
        {
            unsigned sendlen;
            sock->read(&sendlen, sizeof(sendlen));
            if (!sendlen)
                break;
            _WINREV(sendlen);
            sock->read(lockResponse.reserveTruncate(sendlen), sendlen);
        }
        Owned<IPropertyTree> lockRet = createPTreeFromXMLString(lockResponse.str());
        if (lockAll)
        {
            int lockCount = lockRet->getPropInt("Lock", 0);
            int serverCount = lockRet->getPropInt("NumServers", 0);
            return (lockCount && (lockCount == serverCount));
        }

        return lockRet->getPropInt("Lock", 0) != 0;
    }

    void buildPackageFileInfo(IPropertyTree *packageTree, const char *filename)
    {
        StringBuffer packageInfo;
        IPropertyTree *pkgInfo = createPTreeFromXMLFile(filename, ipt_caseInsensitive);

        StringBuffer baseFileName;
        splitFilename(filename, NULL, NULL, &baseFileName, &baseFileName);

        pkgInfo->setProp("@pkgFileName", baseFileName.str());
        packageTree->addPropTree(pkgInfo->queryName(), pkgInfo);
    }


public:
    IMPLEMENT_IINTERFACE;
    CRoxieCommunicationClient(const SocketEndpoint& _ep, unsigned _roxieTimeout)
        :ep(_ep), roxieTimeout(_roxieTimeout)
    {
    }

    IPropertyTree * sendRoxieControlRequest(const char *xml, bool lockAll)
    {
        return sendRoxieControlQuery(xml, lockAll);
    }

    const char * sendRoxieOnDemandRequest(const char *xml, SCMStringBuffer &response, bool lockAll)
    {
        return sendRoxieOnDemandQuery(xml, response, lockAll);
    }

    void sendRoxieReloadControlRequest()
    {
        StringBuffer xpath;
        xpath.appendf("<control:reload/>");
        try
        {
            Owned<IPropertyTree> result = sendRoxieControlQuery(xpath, true);
        }
        catch(IException *e)  // not a fatal error - just log the error
        {
            int errCode = e->errorCode();
            StringBuffer err;
            err.appendf("%d ", errCode);
            e->errorMessage(err);
            DBGLOG("ERROR loading query info directly to roxie %s", err.str());
            e->Release();
        }
    }

    IPropertyTree *sendRoxieControlAllNodes(const char *msg, bool allOrNothing)
    {
        Owned<ISocket> sock = ISocket::connect_timeout(ep, roxieTimeout);
        return sendRoxieControlAllNodes(sock, msg, allOrNothing, roxieTimeout);
    }

    IPropertyTree *sendRoxieControlAllNodes(ISocket *sock, const char *msg, bool allOrNothing, unsigned wait)
    {
        unsigned start = msTick();
        if (!sendRoxieControlLock(sock, allOrNothing, wait))
            throw MakeStringException(-1, "Roxie is too busy (control:lock failed) - please try again later.");
        return sendRoxieControlQuery(sock, msg, remainingMsWait(wait, start));
    }

    IPropertyTree *sendRoxieControlQuery(ISocket *sock, const char *msg, unsigned wait)
    {
        size32_t msglen = strlen(msg);
        size32_t len = msglen;
        _WINREV(len);
        sock->write(&len, sizeof(len));
        sock->write(msg, msglen);

        StringBuffer resp;
        for (;;)
        {
            sock->read(&len, sizeof(len));
            if (!len)
                break;
            _WINREV(len);
            size32_t size_read;
            sock->read(resp.reserveTruncate(len), len, len, size_read, waitMsToSeconds(wait));
            if (size_read<len)
                throw MakeStringException(-1, "Error reading roxie control message response");
        }

        Owned<IPropertyTree> ret = createPTreeFromXMLString(resp.str());
        checkRoxieControlExceptions(ret);
        return ret.getClear();
    }

    IPropertyTree * retrieveQuery(const char *id)
    {
        StringBuffer xpath;
        xpath.appendf("<control:getQuery id='%s'/>", id);
        return sendRoxieControlQuery(xpath.str(), false);   // assume we only need info from one server - they all must be the same or roxie is in trouble
    }

    virtual IPropertyTree * retrieveAllQueryInfo(const char *id)
    {
        StringBuffer xpath;
        xpath.appendf("<control:getAllQueryInfo id='%s'/>", id);
        return sendRoxieControlQuery(xpath.str(), false);
    }

    IPropertyTree * retrieveState()
    {
        return sendRoxieControlQuery("<control:state/>", false);
    }

    IPropertyTree * retrieveQueryStats(const char *queryName, const char *action, const char *graphName, bool lockAll)
    {
        StringBuffer xpath;
        xpath.appendf("<control:querystats><Query id='%s'", queryName);

        if (action)
            xpath.appendf(" action='%s'", action);

        if (graphName)
            xpath.appendf(" name='%s'", graphName);


        xpath.appendf("/></control:querystats>");

        return sendRoxieControlQuery(xpath.str(), lockAll);
    }

    IPropertyTree * retrieveTopology()
    {
        return sendRoxieControlQuery("<control:topology/>", false);
    }

    virtual bool updateQueryComment(const char *id, const char *comment, bool append)
    {
        StringBuffer xpath;
        xpath.appendf("<control:updateQueryComment><Query mode='update' id='%s' comment='%s' append='%d'/></control:updateQueryComment>", id, comment, append);
        Owned<IPropertyTree> tree = sendRoxieControlQuery(xpath, true);
        return true;
    }

    virtual const char * queryRoxieClusterName(SCMStringBuffer &clusterName)
    {
        Owned<IPropertyTree> tree = sendRoxieControlQuery("<control:getClusterName/>", false);
        clusterName.set(tree->queryProp("Endpoint/clusterName/@id"));
        return clusterName.str();
    }

    virtual IPropertyTree * retrieveQueryWuInfo(const char *queryName)
    {
        StringBuffer xpath;
        xpath.appendf("<control:querywuid><Query id='%s'/></control:querywuid>", queryName);
        return sendRoxieControlQuery(xpath.str(), false);   // assume we only need info from one server - they all must be the same or roxie is in trouble
    }

    virtual bool updateACLInfo(bool allow, const char *ip, const char *mask, const char *query, const char *errorMsg, int errorCode, const char *roxieAddress, SCMStringBuffer &status)
    {
        StringBuffer xpath;
        StringBuffer errMsg;
        if (errorMsg && *errorMsg)
            errMsg.appendf("error='%s'", errorMsg);

        xpath.appendf("<control:aclupdate><ACL><Access allow='%d' ip='%s' mask='%s' query='%s' errorCode='%d' %s/></ACL></control:aclupdate>", allow, ip, mask, query, errorCode, (errMsg.length() ? errMsg.str() : ""));
        Owned<IPropertyTree> tree = sendRoxieControlQuery(xpath.str(), true, roxieAddress);
        status.set("update ACL info");
        return true;
    }

    virtual IPropertyTree* queryACLInfo()
    {
        StringBuffer xpath;
        xpath.appendf("<control:queryaclinfo/>");
        return sendRoxieControlQuery(xpath.str(), true);
    }

    virtual IPropertyTree * retrieveActiveMetaInformation(bool addClusterName, bool groupByDataPackage, unsigned version)
    {
        StringBuffer xpath;

        xpath.appendf("<control:queryActiveMetaInfo addClusterName='%d' groupByDataPackage='%d' version='%d'", addClusterName, groupByDataPackage, version);

        xpath.appendf("/>");
        
        return sendRoxieControlQuery(xpath.str(), false);   // assume we only need info from one server - they all must be the same or roxie is in trouble
    }

    virtual unsigned retrieveRoxieStateRevision()
    {
        StringBuffer xpath;
        xpath.appendf("<control:revision/>");
        Owned<IPropertyTree> tree = sendRoxieControlQuery(xpath.str(), false);
        return tree->getPropInt("Endpoint/Revision", 0);
    }

    virtual IPropertyTree * retrieveRoxieQueryMetrics(SCMStringBuffer &queryName, SCMStringBuffer &startDateTime, SCMStringBuffer &endDateTime)
    {
        StringBuffer xpath;
        xpath.append("<control:queryAggregates");  // leave off > for now - put it on later
        if (startDateTime.length())
            xpath.appendf(" from='%s'", startDateTime.str());

        if (endDateTime.length())
            xpath.appendf(" to='%s'", endDateTime.str());

        if (queryName.length())
        {
            xpath.append(">");
            xpath.appendf("<Query id='%s'/></control:queryAggregates>", queryName.str());
        }
        else
            xpath.append("/>");
        
        return sendRoxieControlQuery(xpath.str(), true);
    }

    virtual IPropertyTree * retrieveRoxieMetrics(StringArray &ipList)
    {
        StringBuffer xpath;
        xpath.append("<control:metrics/>");

        if (ipList.ordinality() == 0) // no list of ips, so assume all
            return sendRoxieControlQuery(xpath.str(), true);
        else
        {
            IPropertyTree *tree = createPTree("Control");
            ForEachItemIn(idx, ipList)
            {
                StringBuffer ip(ipList.item(idx));

                if (strchr(ip.str(), ':') == 0)
                    ip.appendf(":%d", ROXIE_SERVER_PORT);

                Owned<IPropertyTree> t = sendRoxieControlQuery(xpath.str(), false, ip.str());
                if (t)
                    tree->addPropTree("Endpoint", t->getBranch("Endpoint"));
            }
            return tree;
        }
    }

    virtual IPropertyTree * listLibrariesUsedByQuery(const char *id, bool useAliasNames)
    {
        StringBuffer xpath;

        xpath.appendf("<control:getLibrariesUsedByQuery id='%s' useAliasNamesOnly='%s'/>", id, (useAliasNames) ? "1" : "0");
        return sendRoxieControlQuery(xpath, false);
    }

    virtual IPropertyTree * listQueriesUsingLibrary(const char *id)
    {
        StringBuffer xpath;

        xpath.appendf("<control:getQueriesUsingLibrary id='%s'/>", id);
        return sendRoxieControlQuery(xpath, false);
    }

    virtual IPropertyTree *retrieveQueryActivityInfo(const char *id, int activityId)
    {
        StringBuffer xpath;

        xpath.appendf("<control:retrieveActivityDetails id='%s' activityId='%d'/>", id, activityId);
        return sendRoxieControlQuery(xpath, false);
    }

    virtual IPropertyTree *getRoxieBuildVersion()
    {
        StringBuffer xpath;
        xpath.appendf("<control:getBuildVersion/>");

        return sendRoxieControlQuery(xpath.str(), false);
    }

};



IRoxieCommunicationClient* createRoxieCommunicationClient(const SocketEndpoint &ep, unsigned roxieTimeout)
{
    return new CRoxieCommunicationClient(ep, roxieTimeout);
}

























