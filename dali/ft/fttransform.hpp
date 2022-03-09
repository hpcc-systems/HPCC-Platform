/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef FTTRANSFORM_HPP
#define FTTRANSFORM_HPP
#include "rmtfile.hpp"

#include "ftbase.hpp"

interface ITransformer : public IInterface
{
public:
    virtual void beginTransform(IFileIOStream * out) = 0;
    virtual void endTransform(IFileIOStream * out) = 0;
    virtual size32_t getBlock(IFileIOStream * out) = 0;
    virtual bool getInputCRC(crc32_t & value) = 0;
    virtual void setInputCRC(crc32_t value) = 0;
    virtual bool setPartition(RemoteFilename & remoteInputName, offset_t _startOffset, offset_t _length, bool compressedInput, const char *decryptKey) = 0;
    virtual offset_t tell() = 0;
    virtual unsigned __int64 getStatistic(StatisticKind kind) = 0;
};


#endif
