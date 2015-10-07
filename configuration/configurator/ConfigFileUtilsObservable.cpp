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

#include "ConfigFileUtilsObservable.hpp"

void CConfigFileUtilsObservable::addObserver(IConfigFileUtilsObserver& observer)
{
   CriticalBlock block(m_critsecObserverQueue);

   //allow observers to register only once
   if (m_qObservers.find(&observer) == (unsigned)-1)
     m_qObservers.enqueue(&observer);
}

void CConfigFileUtilsObservable::removeObserver(IConfigFileUtilsObserver& observer)
{
    CriticalBlock block(m_critsecObserverQueue);
    m_qObservers.dequeue(&observer);
}

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
}
