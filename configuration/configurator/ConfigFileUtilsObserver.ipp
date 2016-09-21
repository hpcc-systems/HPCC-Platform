#ifndef _CONFIG_FILE_UTILS_OBSERVER_IPP_
#define _CONFIG_FILE_UTILS_OBSERVER_IPP_

#include "jobserve.hpp"

namespace CONFIGURATOR
{

interface IConfigFileUtilsObserver : extends IObserver
{
public:

    enum CF_EVENT_TYPES { CF_FILE_OPEN_EVENT = 0x1,
                          CF_FILE_CLOSE_EVENT,
                          CF_FILE_WRITE_EVENT,
                          CF_FILE_DELETE_EVENT,
                          CF_FILE_CREATE_EVENT,
                          CF_FILE_WRITE_NO_CHECK,
                          CF_FILE_ANY_EVENT,
                          CF_FILE_OTHER_EVENT = 0xFF };

    virtual enum CF_EVENT_TYPES getEventType() = 0;
};

}
#endif // _CONFIG_FILE_UTILS_OBSERVER_IPP_
