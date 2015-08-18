#ifndef _SCHEMA_NODE_OBSERVER_IPP_
#define _SCHEMA_NODE_OBSERVER_IPP_

#include "jobserve.hpp"

interface ISchemaNodeObserver : extends IObserver
{
public:

    enum SN_EVENT_TYPES { SN_CREATE_EVENT = 0x1,
                          SN_UPDATE_EVENT,
                          SN_OTHER_EVENT = 0xFF };

    virtual enum SN_EVENT_TYPES getEventType() = 0;
};

#endif // _SCHEMA_NODE_OBSERVER_IPP_
