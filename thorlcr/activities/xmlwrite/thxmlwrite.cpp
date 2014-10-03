/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "jiface.hpp"
#include "jtime.hpp"
#include "dadfs.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "thxmlwrite.ipp"

class CXmlWriteActivityMaster : public CWriteMasterBase
{
    IHThorXmlWriteArg *helper;
    ThorActivityKind kind;
    unsigned headerLength;
    unsigned footerLength;

public:
    CXmlWriteActivityMaster(CMasterGraphElement *info, ThorActivityKind _kind) : CWriteMasterBase(info), kind(_kind), headerLength(0), footerLength(0)
    {
        helper = (IHThorXmlWriteArg *)queryHelper();
    }
    virtual void init()
    {
        CWriteMasterBase::init();
        assertex(!(helper->getFlags() & TDWextend));

        IPropertyTree &props = fileDesc->queryProperties();
        StringBuffer rowTag;
        OwnedRoxieString xmlpath(helper->getXmlIteratorPath());
        if (!xmlpath)
            rowTag.append("Row");
        else
        {
            const char *path = xmlpath;
            if (*path == '/') path++;
            if (strchr(path, '/')) UNIMPLEMENTED;
            rowTag.append(path);
        }
        props.setProp("@rowTag", rowTag.str());
        props.setProp("@format", "utf8n");
        props.setProp("@kind", (kind==TAKjsonwrite) ? "json" : "xml");

        if (kind==TAKjsonwrite)
        {
            StringBuffer s;
            OwnedRoxieString supplied(helper->getHeader());
            props.setPropInt("@headerLength", buildJsonHeader(s, supplied, rowTag).length());

            supplied.set(helper->getFooter());
            props.setPropInt("@footerLength", buildJsonFooter(s.clear(), supplied, rowTag).length());
        }
    }
};

CActivityBase *createXmlWriteActivityMaster(CMasterGraphElement *container, ThorActivityKind kind)
{
    return new CXmlWriteActivityMaster(container, kind);
}

