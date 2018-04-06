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
    WuAttr kind;                // The attribute enumeration
    StatisticMeasure measure;   // units for the measure
    const char * name;          // text version of the attribute
    const char * graphPath;     // The xpath required to extract a result from a graph node.
    const char * overridePath;  // Alternative xpath to check 1st for some overloaded attributes
    const char * childPath;     // The name of the <atr> for setting, or matching when iterating
    const char * dft;           // default value if not present
};

#define CHILDPATH(x) "att[@name='" x "']/@value"
#define CHILDMPATH(x) "att[@name='" x "'][1]/@value"
#define ATTR(kind, measure, path)           { WA ## kind, measure, #kind, path, nullptr, nullptr, nullptr }
#define ALTATTR(kind, measure, path, alt)   { WA ## kind, measure, #kind, path, alt, nullptr }
#define CHILD(kind, measure, path)          { WA ## kind, measure, #kind, CHILDPATH(path), nullptr, path, nullptr }
#define CHILD_MULTI(kind, measure, path)    { WA ## kind, measure, #kind, CHILDMPATH(path), nullptr, path, nullptr }
#define CHILD_D(kind, measure, path, dft)   { WA ## kind, measure, #kind, CHILDPATH(path), nullptr, path, dft }


const static WuAttrInfo attrInfo[] = {
    { WANone, SMeasureNone, "none", nullptr, nullptr, nullptr },
    { WAAll, SMeasureNone, "all", nullptr, nullptr, nullptr },
    CHILD(Kind, SMeasureEnum, "_kind"),
    ALTATTR(IdSource, SMeasureId, "@source", "att[@name='_sourceActivity']/@value"),
    ALTATTR(IdTarget, SMeasureId, "@target", "att[@name='_targetActivity']/@value"),
    CHILD_D(SourceIndex, SMeasureText, "_sourceIndex", "0"),
    CHILD_D(TargetIndex, SMeasureText, "_targetIndex", "0"),
    ATTR(Label, SMeasureText, "@label"),
    CHILD(IsDependency, SMeasureBool, "_dependsOn"),
    CHILD(IsChildGraph, SMeasureBool, "_childGraph"),
    CHILD_MULTI(Definition, SMeasureText, "definition"),
    CHILD_MULTI(EclName, SMeasureText, "name"),
    CHILD(EclText, SMeasureText, "ecl"),
    CHILD(RecordSize, SMeasureText, "recordSize"),
    CHILD(PredictedCount, SMeasureText, "predictedCount"),
    CHILD(Filename, SMeasureText, "_fileName"),
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

WuAttr queryWuAttribute(const char * kind, WuAttr dft)
{
    //MORE: This needs to use a hash table
    for (unsigned i=WANone; i < WAMax; i++)
    {
        if (strieq(kind, attrInfo[i-WANone].name))
            return (WuAttr)i;
    }
    return dft;
}

extern WORKUNIT_API const char * queryAttributeValue(IPropertyTree & src, WuAttr kind, StringBuffer & scratchpad)
{
    if ((kind <= WANone) || (kind >= WAMax))
        return nullptr;

    const WuAttrInfo & info = attrInfo[kind-WANone];
    const char * path = info.graphPath;
    const char * altpath = info.overridePath;
    const char * value = altpath ? src.queryProp(altpath) : nullptr;
    if (!value)
        value = src.queryProp(path);
    if (!value && info.dft)
        value = info.dft;

    //The following switch statement allows the value returned to be transformed from the value stored.
    switch (kind)
    {
    case WAIdSource:
    case WAIdTarget:
        //A bit of a hack - source and target for edges are activity ids.  Return a computed string.
        value = scratchpad.clear().append(ActivityScopePrefix).append(value).str();
        break;
    }

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
