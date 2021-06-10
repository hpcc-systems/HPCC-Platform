/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#pragma once

#include "jmetrics.hpp"
using namespace hpccMetrics;

class PeriodicTestSink : public PeriodicMetricSink
{
    public:
        explicit PeriodicTestSink(const char *name, const IPropertyTree *pSettingsTree) :
                PeriodicMetricSink(name, "test", pSettingsTree) { }

        ~PeriodicTestSink() = default;

        bool isPreparedCalled() const { return prepareCalled; }
        bool isCollectionStopped() const { return stopCollectionFlag; }
        bool isCollectionStoppedCalled() const { return stopCollectionCalled; }
        bool isCurrentlyCollectiing() const { return isCollecting; }
        int getNumCollections() const { return numCollections; }

    protected:
        virtual void prepareToStartCollecting() override
        {
            prepareCalled = true;
            numCollections = 0;
        }

        virtual void collectingHasStopped() override
        {
            stopCollectionCalled = true;
        }

        void doCollection() override
        {
            numCollections++;
        }


    protected:

        bool prepareCalled = false;
        bool stopCollectionCalled = false;
        int numCollections = 0;
};
