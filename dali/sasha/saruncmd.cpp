#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "mpbase.hpp"
#include "sacmd.hpp"
#include "saruncmd.hpp"

class CSashaCmdExecutor : public CInterfaceOf<ISashaCmdExecutor>
{
    Owned<INode> node;
    unsigned defaultTimeoutMs = 60;
    StringBuffer nodeText;
    mutable StringBuffer lastServerMessage;
    bool restoreWorkunit(const char *wuid, bool dfu) const
    {
        lastServerMessage.clear();
        Owned<ISashaCommand> cmd = createSashaCommand();
        cmd->setAction(SCA_RESTORE);
        if (dfu)
            cmd->setDFU(true);
        cmd->addId(wuid);
        if (!cmd->send(node, defaultTimeoutMs))
            throw makeStringExceptionV(-1, "Could not connect to Sasha server on %s", nodeText.str());
        if (cmd->numIds()==0)
        {
            // nothing restored
            return false;
        }
        cmd->getId(0, lastServerMessage);
        return true;
    }
    bool archiveWorkunit(const char *wuid, bool dfu, bool deleteAfterArchiving) const
    {
        lastServerMessage.clear();
        Owned<ISashaCommand> cmd = createSashaCommand();
        cmd->setAction(SCA_ARCHIVE);
        if (dfu)
            cmd->setDFU(true);
        cmd->addId(wuid);
        if (!cmd->send(node, defaultTimeoutMs))
            throw makeStringExceptionV(-1, "Could not connect to Sasha server on %s", nodeText.str());
        if (cmd->numIds()==0)
        {
            // nothing archived
            return false;
        }
        cmd->getId(0, lastServerMessage);
        return true;
    }
    bool listWorkunit(ListWURequests *req, bool dfu) const
    {
        lastServerMessage.clear();
        Owned<ISashaCommand> cmd = createSashaCommand();
        cmd->setAction(req->includeDT ? SCA_LISTDT : SCA_LIST);
        if (dfu)
            cmd->setDFU(true);
        cmd->setArchived(req->archived);
        cmd->setOnline(req->online);
        if (!req->outputFields.isEmpty())
            cmd->setOutputFormat(req->outputFields);

        cmd->addId(req->wuid.isEmpty() ? "*" : req->wuid.get());
        if (!req->cluster.isEmpty())
            cmd->setCluster(req->cluster.get());
        if (!req->owner.isEmpty())
            cmd->setOwner(req->owner.get());
        if (!req->jobName.isEmpty())
            cmd->setJobName(req->jobName.get());
        if (!req->state.isEmpty())
            cmd->setState(req->state.get());

        if (!req->fromDate.isEmpty())
            cmd->setAfter(req->fromDate.str()); //In CArchivedWUReader, the After is used as fromDate.
        if (!req->toDate.isEmpty())
            cmd->setBefore(req->toDate.str()); //In CArchivedWUReader, the Before is used as toDate.

        if (!req->beforeWU.isEmpty())
            cmd->setBeforeWU(req->beforeWU.get());
        if (!req->afterWU.isEmpty())
            cmd->setAfterWU(req->afterWU.get());
        cmd->setLimit(req->maxNumberWUs);
        cmd->setSortDescending(req->descending);
        if (!cmd->send(node, defaultTimeoutMs))
            throw makeStringExceptionV(-1, "Could not connect to Sasha server on %s", nodeText.str());

        unsigned n = cmd->numIds();
        if (n == 0)
        {
            // no workunit found
            return false;
        }

        for (unsigned i = 0; i < n; i++)
        {
            cmd->getId(i, lastServerMessage);
            if (req->includeDT)
            {
                CDateTime dt;
                cmd->getDT(dt, i);
                lastServerMessage.append(",");
                dt.getString(lastServerMessage);
            }
            lastServerMessage.append("\n");
        }
        lastServerMessage.appendf("%d WUID%s returned\n", n, (n == 1) ? "" : "s");
        return true;
    }

public:
    CSashaCmdExecutor(const SocketEndpoint &ep, unsigned _defaultTimeoutSecs = 60)
    {
        ep.getEndpointHostText(nodeText);
        node.setown(createINode(ep));
        defaultTimeoutMs = _defaultTimeoutSecs * 1000;
    }
    virtual StringBuffer &getVersion(StringBuffer &version) const override
    {
        Owned<ISashaCommand> cmd = createSashaCommand();
        cmd->setAction(SCA_GETVERSION);
        if (!cmd->send(node, defaultTimeoutMs))
            throw makeStringExceptionV(-1, "Could not connect to Sasha server on %s", nodeText.str());
        if (!cmd->getId(0, version))
            throw makeStringExceptionV(-1, "Sasha server[%s]: Protocol error", nodeText.str());
        return version;
    }
    virtual StringBuffer &getLastServerMessage(StringBuffer &message) const override
    {
        message.append(lastServerMessage);
        return message;
    }
    virtual bool restoreECLWorkUnit(const char *wuid) const override
    {
        return restoreWorkunit(wuid, false);
    }
    virtual bool restoreDFUWorkUnit(const char *wuid) const override
    {
        return restoreWorkunit(wuid, true);
    }
    virtual bool archiveECLWorkUnit(const char *wuid) const override
    {
        return archiveWorkunit(wuid, false, true);
    }
    virtual bool archiveDFUWorkUnit(const char *wuid) const override
    {
        return archiveWorkunit(wuid, true, true);
    }
    virtual bool backupECLWorkUnit(const char *wuid) const override
    {
        return archiveWorkunit(wuid, false, false);
    }
    virtual bool backupDFUWorkUnit(const char *wuid) const override
    {
        return archiveWorkunit(wuid, true, false);
    }
    virtual bool listECLWorkUnit(ListWURequests *req) const override
    {
        return listWorkunit(req, false);
    }
    virtual bool listDFUWorkUnit(ListWURequests *req) const override
    {
        return listWorkunit(req, true);
    }
};

ISashaCmdExecutor *createSashaCmdExecutor(const SocketEndpoint &ep, unsigned defaultTimeoutSecs)
{
    return new CSashaCmdExecutor(ep, defaultTimeoutSecs);
}
