#include "ConfigFileUtilsObservable.hpp"

void CConfigFileUtilsObservable::addObserver(IConfigFileUtilsObserver& observer)
{
    CriticalBlock block(m_critsecObserverQueue);

   //allow observers to register only once
   if (m_qObservers.find(&observer) == (unsigned)-1)
   {
     m_qObservers.enqueue(&observer);
   }
};

void CConfigFileUtilsObservable::removeObserver(IConfigFileUtilsObserver& observer)
{
    CriticalBlock block(m_critsecObserverQueue);

    m_qObservers.dequeue(&observer);
};

void CConfigFileUtilsObservable::notify(enum IConfigFileUtilsObserver::CF_EVENT_TYPES eventType)
{
    CriticalBlock block(m_critsecObserverQueue);

    for (unsigned int idxObservers = 0; idxObservers < m_qObservers.ordinality(); idxObservers++)
    {
        IConfigFileUtilsObserver *pConfigFileUtilsObserver = m_qObservers.query(idxObservers);

        if (pConfigFileUtilsObserver->getEventType() == eventType || pConfigFileUtilsObserver->getEventType() == IConfigFileUtilsObserver::CF_FILE_ANY_EVENT)
        {
            //m_qObservers.query(idxObservers)->onNotify(eventType);
        }
    }
};
