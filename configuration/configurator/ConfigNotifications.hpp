/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#ifndef _CONFIG_NOTIFICATIONS_HPP_
#define _CONFIG_NOTIFICATIONS_HPP_

#include "jiface.hpp"
#include "jhash.ipp"
#include "jutil.hpp"

namespace CONFIGURATOR
{

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

    static CNotificationManager* getInstance();

    virtual ~CNotificationManager(){}

    int getNumberOfNotificationTypes() const
    {
        return eUnknown-1;
    }

    const char* getNotificationTypeName(int idx)
    {
        if (idx >= 0 && idx < eUnknown)
            return NotificationTypeNames[idx];
        else
            return NULL;
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

}
#endif // _CONFIG_NOTIFICATIONS_HPP_
