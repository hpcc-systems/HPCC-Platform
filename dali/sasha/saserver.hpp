#ifndef SASERVER_HPP
#define SASERVER_HPP

#include "jiface.hpp"
#include "mpbase.hpp"

//#define SASH_SNMP_ENABLED

interface ISashaServer: extends IInterface  // for all coven based servers
{
    virtual void start() = 0;
    virtual void ready() = 0; // called after all servers started
    virtual void stop() = 0;
};

extern Owned<IPropertyTree> serverConfig; //configuration properties

extern void setMsgLevel(unsigned level);

extern void requestStop(IException *e);

extern void coalesceDatastore(IPropertyTree *config, bool force);

extern const char *sashaProgramName;

class CSuspendAutoStop
{
public:
    CSuspendAutoStop();
    ~CSuspendAutoStop();
};


#endif
