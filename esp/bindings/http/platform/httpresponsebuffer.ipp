/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#ifndef _HTTPRESPONSEBUFFER_IPP__
#define _HTTPRESPONSEBUFFER_IPP__

#include "http/platform/httptransport.ipp"
#include "rtlformat.hpp"

static constexpr unsigned defaultResponseFlushThresholdBytes = 8000;
static constexpr unsigned closingResponseFlushThresholdBytes = 1;

class CFlushingHttpResponseBuffer : public CInterfaceOf<IXmlStreamFlusher>
{
    CHttpResponse* response = nullptr;
    const size32_t responseFlushThreshold = defaultResponseFlushThresholdBytes;
    StringBuffer responseBuffer;

public:
    CFlushingHttpResponseBuffer(CHttpResponse* _response, size32_t _flushThreshold) :
        response(_response),  responseFlushThreshold(_flushThreshold)
    {
        if (!response)
            throw makeStringException(-1, "HttpResponse not specified for CFlushingWUFileBuffer");
    };
    ~CFlushingHttpResponseBuffer()
    {
        try
        {
            if (!responseBuffer.isEmpty())
                response->sendChunk(responseBuffer);
        }
        catch (IException* e)
        {
            // Ignore any socket errors that we get at termination - nothing we can do about them anyway...
            EXCLOG(e, "In ~CFlushingHttpResponseBuffer()");
            e->Release();
        }
    }
    void flushXML(StringBuffer& current, bool closing)
    {
        responseBuffer.append(current);
        current.clear();

        //When closing, no flushing is needed if responseBuffer.length() < closingResponseFlushThresholdBytes.
        const size32_t threshold = closing ? closingResponseFlushThresholdBytes : responseFlushThreshold;
        if (responseBuffer.length() < threshold)
            return;

        response->sendChunk(responseBuffer);
        responseBuffer.clear();
    }
};

#endif
