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

#include "wuattr.hpp"
#include "jptree.hpp"

struct WuAttrInfo
{
public:
    WuAttr kind;
    StatisticMeasure measure;
    const char * name;
    const char * graphPath;
    const char * childPath;
    const char * dft;
};

#define CHILDPATH(x) "att[@name='" x "']/@value"
#define ATTR(kind, measure, path)           { WA ## kind, measure, #kind, path, nullptr, nullptr }
#define CHILD(kind, measure, path)    { WA ## kind, measure, #kind, CHILDPATH(path), path, nullptr }
#define CHILD_D(kind, measure, path, dft)    { WA ## kind, measure, #kind, CHILDPATH(path), path, dft }


const static WuAttrInfo attrInfo[] = {
    { WANone, SMeasureNone, "none", nullptr, nullptr, nullptr },
    { WAAll, SMeasureNone, "all", nullptr, nullptr, nullptr },
    CHILD(Kind, SMeasureEnum, "_kind"),
    ATTR(Source, SMeasureText, "@source"),
    ATTR(Target, SMeasureText, "@target"),
    CHILD_D(SourceIndex, SMeasureText, "_sourceIndex", "0"),
    CHILD_D(TargetIndex, SMeasureText, "_targetIndex", "0"),
    ATTR(Label, SMeasureText, "@label"),
    CHILD(IsDependency, SMeasureBool, "_dependsOn"),
    CHILD(IsChildGraph, SMeasureBool, "_childGraph"),
    CHILD(Definition, SMeasureText, "definition"),
    CHILD(EclName, SMeasureText, "name"),
    { WAMax, SMeasureNone, nullptr, nullptr, nullptr, nullptr }
};


MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    static_assert(_elements_in(attrInfo) >= (WAMax-WANone)+1, "Elements missing from attrInfo[]");
    static_assert(_elements_in(attrInfo) <= (WAMax-WANone)+1, "Extra elements in attrInfo[]");
    for (unsigned i=0; i < _elements_in(attrInfo); i++)
    {
        assertex(attrInfo[i].kind == WANone + i);
    }
    return true;
}


const char * queryWuAttributeName(WuAttr kind)
{
    if ((kind >= WANone) && (kind < WAMax))
        return attrInfo[kind-WANone].name;
    return nullptr;
}

WuAttr queryWuAttribute(const char * kind)
{
    //MORE: This needs to use a hash table
    for (unsigned i=WANone; i < WAMax; i++)
    {
        if (strieq(kind, attrInfo[i-WANone].name))
            return (WuAttr)i;
    }
    return WANone;
}

extern WORKUNIT_API const char * queryAttributeValue(IPropertyTree & src, WuAttr kind)
{
    if ((kind <= WANone) || (kind >= WAMax))
        return nullptr;

    const WuAttrInfo & info = attrInfo[kind-WANone];
    const char * path = info.graphPath;
    const char * value = src.queryProp(path);
    if (!value && info.dft)
        value = info.dft;
    return value;
}

extern WORKUNIT_API WuAttr queryGraphAttrToWuAttr(const char * name)
{
    //MORE: Create a hash table to implement this mapping efficiently
    for(unsigned i=1; i < WAMax-WANone; i++)
    {
        const WuAttrInfo & info = attrInfo[i];
        const char * path = info.graphPath;
        if (path[0] == '@' && strieq(name, path+1))
            return (WuAttr)(i+WANone);
    }
    return WANone;
}

extern WORKUNIT_API WuAttr queryGraphChildAttToWuAttr(const char * name)
{
    //MORE: Create a hash table to implement this mapping efficiently
    for(unsigned i=1; i < WAMax-WANone; i++)
    {
        const WuAttrInfo & info = attrInfo[i];
        const char * childPath = info.childPath;
        if (childPath && strieq(name, childPath))
            return (WuAttr)(i+WANone);
    }
    return WANone;
}


static IPropertyTree * addGraphAttribute(IPropertyTree * node, const char * name)
{
    IPropertyTree * att = createPTree();
    att->setProp("@name", name);
    return node->addPropTree("att", att);
}

extern WORKUNIT_API void setAttributeValue(IPropertyTree & tgt, WuAttr kind, const char * value)
{
    const WuAttrInfo & info = attrInfo[kind-WANone];
    const char * path = info.graphPath;
    if (path[0] == '@')
        tgt.setProp(path, value);
    else
        addGraphAttribute(&tgt, info.childPath)->setProp("@value", value);
}
