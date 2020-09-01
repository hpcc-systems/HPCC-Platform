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

#pragma once

#include <chrono>

namespace hpccMetrics
{

class MetricsReportContext
{
public:
    explicit MetricsReportContext(unsigned bufferSize = 0)
    {
        timestamp = std::chrono::seconds(std::time(nullptr)).count();
        allocateBuffer(bufferSize);
    }

    virtual ~MetricsReportContext()
    {
        freeBuffer();
    }

    uint8_t *allocateBuffer(unsigned size)
    {
        freeBuffer();
        if (size != 0)
        {
            bufferSize = size;
            buffer = new uint8_t[bufferSize];
        }
        return buffer;
    }

    void freeBuffer()
    {
        if (buffer != nullptr)
        {
            delete [] buffer;
            buffer = nullptr;
            bufferSize = 0;
        }
    }

    uint8_t *getBuffer()
    {
        return buffer;
    }

    unsigned getBufferSize() const
    {
        return bufferSize;
    }

    long getTimestamp() const
    {
        return timestamp;
    }

private:
    long timestamp;
    uint8_t *buffer = nullptr;
    unsigned bufferSize = 0;
};

}
