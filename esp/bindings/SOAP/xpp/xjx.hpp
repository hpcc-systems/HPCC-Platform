/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef _XJX_HPP__
#define _XJX_HPP__
#include "jlib.hpp"
namespace xpp
{
class EndTag;
class StartTag;
interface XJXPullParser {
public:
    virtual ~XJXPullParser() {}
    virtual void setInput(const char* buf, int bufSize) = 0;
    virtual int next() = 0;
    virtual const char* readContent()  = 0;
    virtual void readEndTag(EndTag& etag) = 0;
    virtual void readStartTag(StartTag& stag) = 0;
};
}
#endif
