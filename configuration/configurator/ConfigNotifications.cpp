#include "SchemaCommon.hpp"
#include "ConfigNotifications.hpp"
#include "jlib.hpp"
#include "jhash.ipp"
#include "jhash.hpp"
#include "jstring.hpp"

CNotificationManager* CNotificationManager::s_pNotificationManager = NULL;

CNotificationManager::CNotificationManager()
{
}

CNotificationManager *CNotificationManager::getInstance()
{
    if (s_pNotificationManager == NULL)
    {
        s_pNotificationManager = new CNotificationManager();
    }
    return s_pNotificationManager;
}

int CNotificationManager::getNumberOfNotifications(enum ENotificationType eNotifyType) const
{
    int nCount = 0;

    HashIterator iterHash(m_EnvXPathToNotification);

    ForEach(iterHash)
    {
        CNotification *pNotification = m_EnvXPathToNotification.mapToValue(&iterHash.query());

        for (int i = 0; i < eUnknown; i++)
        {
            nCount += pNotification->m_NotificationMessagesArrays[i].length();
        }
    }

    return nCount;
}

void CNotificationManager::setNotification(const CXSDNodeBase *pNodeBase, enum ENotificationType eNotifyType, const char* pNotificationText)
{
    assert (pNodeBase != NULL);
    assert (pNotificationText != NULL);

    if (pNodeBase != NULL)
    {
        const char* pEnvXPath = pNodeBase->getEnvXPath();

        assert(pEnvXPath != NULL && *pEnvXPath != 0);

        if (pEnvXPath != NULL)
        {
            CNotification *pNotification = m_EnvXPathToNotification.getValue(pEnvXPath);

            if (pNotification == NULL)
            {
                pNotification = new CNotification();

                m_EnvXPathToNotification.setValue(pEnvXPath, pNotification);
            }

            pNotification->m_NotificationMessagesArrays[eNotifyType].append(pNotificationText);
        }
    }
}

void CNotificationManager::resetNotifications(const CXSDNodeBase *pNodeBase)
{
    assert(pNodeBase != NULL);

    if (pNodeBase != NULL)
    {
        const char* pEnvXPath = pNodeBase->getEnvXPath();

        assert(pEnvXPath != NULL && *pEnvXPath != 0);

        if (pEnvXPath != NULL && *pEnvXPath != 0)
        {
            CNotification *pNotification = m_EnvXPathToNotification.getValue(pEnvXPath);

            if (pNotification != NULL)
            {
                m_EnvXPathToNotification.remove(pEnvXPath);
                delete pNotification;
            }
        }
    }
}

const char* CNotificationManager::getNotification(enum ENotificationType eNotifyType, int idx) const
{
 /*   assert(idx >= 0);

    if (pNotification->m_NotificationMessagesArrays[eNotifyType].length() < idx)
    {
        return pNotification->m_NotificationMessagesArrays[eNotifyType].item(idx);
    }
    else
    {
        return NULL;
    }
    */

    return NULL;
}
