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

#ifndef WUATTR_HPP
#define WUATTR_HPP

#include "jlib.hpp"
#include "jstatcodes.h"

#ifdef WORKUNIT_EXPORTS
    #define WORKUNIT_API DECL_EXPORT
#else
    #define WORKUNIT_API DECL_IMPORT
#endif

//The wuattribute values start from a high value - so that they do not overlap with StXXX
enum WuAttr : unsigned
{
    WANone = 0x80000000,
    WAAll,
    WAKind,
    WASource,
    WATarget,
    WASourceIndex,
    WATargetIndex,
    WALabel,
    WAIsDependency,
    WAIsChildGraph,
    WADefinition,
    WAEclName,
    WAMax
};

extern WORKUNIT_API const char * queryWuAttributeName(WuAttr kind);
extern WORKUNIT_API WuAttr queryWuAttribute(const char * kind);
extern WORKUNIT_API const char * queryAttributeValue(IPropertyTree & src, WuAttr kind);
extern WORKUNIT_API WuAttr queryGraphAttrToWuAttr(const char * name);
extern WORKUNIT_API WuAttr queryGraphChildAttToWuAttr(const char * name);

extern WORKUNIT_API void setAttributeValue(IPropertyTree & tgt, WuAttr kind, const char * value);

#endif
