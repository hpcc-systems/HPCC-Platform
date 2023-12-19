#include "platform.h"
#include "jlib.hpp"
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
};

ISashaCmdExecutor *createSashaCmdExecutor(const SocketEndpoint &ep, unsigned defaultTimeoutSecs)
{
    return new CSashaCmdExecutor(ep, defaultTimeoutSecs);
}
