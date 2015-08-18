#ifndef _CONFIG_NOTIFICATIONS_HPP_
#define _CONFIG_NOTIFICATIONS_HPP_

#include "jiface.hpp"
#include "jhash.ipp"
#include "jutil.hpp"

enum ENotificationType
{
    eInformational = 1,
    eAlert,
    eWarning,
    eError,
    eCritical,
    eUnknown // needs to be last
};

static const char NotificationTypeNames[eUnknown][1024] = {     "Informational",
                                                                "Alert",
                                                                "Warning",
                                                                "Error",
                                                                "Critical",
                                                                "Unknown" };

class CNotification;
class CNotificationManager;

class CNotificationManager : public InterfaceImpl
{
public:

    //IMPLEMENT_IINTERFACE;

    static CNotificationManager* getInstance();

    virtual ~CNotificationManager(){};

    int getNumberOfNotificationTypes() const
    {
        return eUnknown-1;
    }

    const char* getNotificationTypeName(int idx)
    {
        if (idx >= 0 && idx < eUnknown)
        {
            return NotificationTypeNames[idx];
        }
        else
        {
            return NULL;
        }
    }

    int getNumberOfNotifications(enum ENotificationType eNotifyType) const;

    const char* getNotification(enum ENotificationType eNotifyType, int idx) const;

    void setNotification(const CXSDNodeBase *pNodeBase, enum ENotificationType eNotifyType, const char* pNotificationText);
    void resetNotifications(const CXSDNodeBase *pNodeBase);

protected:

    CNotificationManager();

    typedef MapStringToMyClass<CNotification> MapStringToCNotification;
    MapStringToCNotification m_EnvXPathToNotification;

private:

    static CNotificationManager *s_pNotificationManager;

};

class CNotification : public InterfaceImpl
{
    friend class CNotificationManager;

public:

    CNotification()
    {
    }

    virtual ~CNotification()
    {
    }

    StringArray m_NotificationMessagesArrays[eUnknown];
};



#endif // _CONFIG_NOTIFICATIONS_HPP_
