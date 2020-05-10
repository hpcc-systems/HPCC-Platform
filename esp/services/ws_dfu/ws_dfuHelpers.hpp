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

#ifndef _WSDFU_HELPERS_HPP__
#define _WSDFU_HELPERS_HPP__

#include "dadfs.hpp"

#include "jstring.hpp"
#include "exception_util.hpp"
#include "ws_dfu_common_esp.ipp"

namespace WsDFUHelpers
{
    bool addDFUQueryFilter(DFUQResultField* filters, unsigned short& count, MemoryBuffer& buff, const char* value, DFUQResultField name);
    void appendDFUQueryFilter(const char*name, DFUQFilterType type, const char* value, StringBuffer& filterBuf);
    void appendDFUQueryFilter(const char*name, DFUQFilterType type, const char* value, const char* valueHigh, StringBuffer& filterBuf);
    const char* getPrefixFromLogicalName(const char* logicalName, StringBuffer& prefix);
    bool addToLogicalFileList(IPropertyTree& file, const char* nodeGroup, double version, IArrayOf<IEspDFULogicalFile>& logicalFiles);
};

#endif
