#ifndef _CONFIG_FILE_UTILS_OBSERVALBE_HPP_
#define _CONFIG_FILE_UTILS_OBSERVALBE_HPP_

#include "ConfigFileUtilsObserver.ipp"
#include "jlib.hpp"
#include "jqueue.tpp"
#include "jmutex.hpp"


class CConfigFileUtilsObservable : public CInterface, implements IObservable
{
public:

    IMPLEMENT_IINTERFACE

    virtual void addObserver(IConfigFileUtilsObserver& observer);
    virtual void removeObserver(IConfigFileUtilsObserver& observer);

    void enableNotifications(bool bEnable)
    {
        m_bEnableNotifications = bEnable;
    }

    bool getNotificationsEnabled() const
    {
        return m_bEnableNotifications;
    }

protected:

    virtual void notify( enum IConfigFileUtilsObserver::CF_EVENT_TYPES eventType);

private:

    QueueOf<IConfigFileUtilsObserver,false> m_qObservers;
    CriticalSection m_critsecObserverQueue;
    bool m_bEnableNotifications;
};


#endif // _CONFIG_FILE_UTILS_OBSERVALBE_HPP_
