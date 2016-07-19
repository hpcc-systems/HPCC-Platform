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

#ifndef THORHELPER_HPP
#define THORHELPER_HPP

#include "jiface.hpp"
#include "jptree.hpp"

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

interface IXmlToRawTransformer : extends IInterface
{
    virtual IDataVal & transform(IDataVal & result, size32_t len, const void * text, bool isDataset) = 0;
    virtual IDataVal & transformTree(IDataVal & result, IPropertyTree &tree, bool isDataset) = 0;
};

interface ICsvToRawTransformer : extends IInterface
{
    virtual IDataVal & transform(IDataVal & result, size32_t len, const void * text, bool isDataset) = 0;
};

enum ThorReplyCodes { DAMP_THOR_ACK, DAMP_THOR_REPLY_GOOD, DAMP_THOR_REPLY_ERROR, DAMP_THOR_REPLY_ABORT, DAMP_THOR_REPLY_PAUSED };

#endif // THORHELPER_HPP
