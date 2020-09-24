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

#ifndef _ESPWIZ_ws_wuresult_HPP__
#define _ESPWIZ_ws_wuresult_HPP__

#include "fvdatasource.hpp"
#include "fvresultset.ipp"
#include "fileview.hpp"
#include "rtlformat.hpp"

static const unsigned defaultWUResultDownloadFlushThreshold = 10000; 

INewResultSet* createFilteredResultSet(INewResultSet* result, IArrayOf<IConstNamedValue>* filterBy);
void getCSVHeaders(const IResultSetMetaData& metaIn, CommonCSVWriter* writer, unsigned& layer);
void getWorkunitCluster(IEspContext& context, const char* wuid, SCMStringBuffer& cluster, bool checkArchiveWUs);

enum WUResultOutFormat
{
    WUResultOutXML  = 0,
    WUResultOutJSON = 1,
    WUResultOutCSV  = 2,
    WUResultOutZIP  = 3,
    WUResultOutGZIP = 4
};

class FlushingWUResultBuffer : implements IXmlStreamFlusher, public CInterface
{
    CHttpResponse* response = nullptr;
    IFileIOStream* outIOS = nullptr; //Streamming to a file before zip/gzip
    size32_t wuResultDownloadFlushThreshold = defaultWUResultDownloadFlushThreshold;
    StringBuffer s;

public:
    IMPLEMENT_IINTERFACE;

    FlushingWUResultBuffer(CHttpResponse* _response, IFileIOStream* _outIOS, size32_t _flushThreshold) :
        response(_response), outIOS(_outIOS), wuResultDownloadFlushThreshold(_flushThreshold)
    {
        if (!response && !outIOS)
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Output not specified for FlushingWUResultBuffer");
    };
    ~FlushingWUResultBuffer();

    virtual void flushXML(StringBuffer& current, bool isClosing) override;
};

class CWsWuResultOutHelper
{
    IEspContext* context = nullptr;
    IProperties* reqParams = nullptr;
    CHttpResponse* response = nullptr;

    WUResultOutFormat outFormat = WUResultOutXML;

    StringBuffer wuid;
    StringAttr resultName, logicalName, cluster;
    IArrayOf<IConstNamedValue> filterBy;
    unsigned sequence = 0, count = 0;
    __int64 start = 0;

    StringBuffer workingFolder, resultFileNameWithPath;
    OwnedIFileIOStream resultIOS; //Streamming to a file before zip/gzip
    unsigned downloadFlushThreshold = defaultWUResultDownloadFlushThreshold;

    bool canStream();
    void readReq();
    void readWUIDReq();
    void readOutFormatReq();
    void readFilterByReq();
    void createResultIOS();
    void addCustomerHeader();
    void startStreaming();
    void finalXMLStreaming();
    void getWsWuResult(const char* resultName, const char* logicalName, unsigned sequence);
    void getFileResults();
    void filterAndAppendResultSet(INewResultSet* result, const char* resultName, const IProperties* xmlns);
    void appendResultSetStreaming(INewResultSet* result, const char* resultName, const IProperties* xmlns);
    unsigned getResultCSVStreaming(INewResultSet* result, const char* resultName, IXmlStreamFlusher* flusher);
    unsigned getResultJSONStreaming(INewResultSet* result, const char* resultName, const char* schemaName, IXmlStreamFlusher* flusher);
    unsigned getResultXmlStreaming(INewResultSet* result, const char* resultName, const char* schemaName, const IProperties* xmlns, IXmlStreamFlusher* flusher);
    void zipResultFile(const char* zipFileNameWithPath);

public:
    CWsWuResultOutHelper() { };

    bool getWUResultStreaming(CHttpRequest* request, CHttpResponse* response, unsigned flushThreshold);
};

#endif
