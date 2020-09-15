/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "PeriodicTrigger.hpp"
#include <thread>

using namespace hpccMetrics;

extern "C" IMetricsReportTrigger* getTriggerInstance(const IPropertyTree *pSettingsTree)
{
    IMetricsReportTrigger *pTrigger = new PeriodicTrigger(pSettingsTree);
    return pTrigger;
}

PeriodicTrigger::PeriodicTrigger(const IPropertyTree *pSettingsTree) :
    periodSeconds{std::chrono::seconds(60)}
{
    if (pSettingsTree->hasProp("period"))
    {
        unsigned seconds = pSettingsTree->getPropInt("period");
        periodSeconds = std::chrono::seconds(seconds);
    }
}

PeriodicTrigger::~PeriodicTrigger()
{
    if (collectionStarted)
    {
        stopCollection();
    }
}

void PeriodicTrigger::start()
{
    collectThread = std::thread(&PeriodicTrigger::collectionThread, this, this);
    //collectThread = std::thread(collectionThread, this);
}

void PeriodicTrigger::stop()
{
    stopCollection();
}

void PeriodicTrigger::stopCollection()
{
    stopCollectionFlag = true;
    collectThread.join();
}

void PeriodicTrigger::collectionThread(PeriodicTrigger *pReportTrigger) const
{
    while (!stopCollectionFlag)
    {
        std::this_thread::sleep_for(pReportTrigger->periodSeconds);
        pReportTrigger->doReport("", nullptr);
    }
}
