#include "platform.h"
#include "jlib.hpp"
#include "mpbase.hpp"
#include "sacmd.hpp"
#include "saruncmd.hpp"

static void getVersion(ISashaCommand *cmd, INode *node, StringBuffer &outBuffer)
{
    StringBuffer host;
    node->endpoint().getHostText(host);
    if (!cmd->send(node))
        throw makeStringExceptionV(-1, "Could not connect to Sasha server on %s", host.str());

    StringBuffer id;
    if (!cmd->getId(0, id))
        throw makeStringExceptionV(-1, "Sasha server[%s]: Protocol error", host.str());

    outBuffer.appendf("Sasha server[%s]: Version %s", host.str(), id.str());
}

extern SARUNCMD_API void runSashaCommand(SashaCommandAction action, INode *node, StringBuffer &outBuffer)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(action);
    switch (action)
    {
        case SCA_GETVERSION:
            getVersion(cmd, node, outBuffer);
            break;
        default:
            throwUnexpected();
    }
}
