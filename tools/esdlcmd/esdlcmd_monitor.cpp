/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "build-config.h"

#include "esdlcmd_common.hpp"
#include "esdlcmd_core.hpp"

#include "esdl2ecl.cpp"
#include "esdl-publish.cpp"
#include "xsdparser.hpp"

StringBuffer &getEsdlCmdComponentFilesPath(StringBuffer & path)
{
    if (getComponentFilesRelPathFromBin(path))
        return path;
    return path.set(COMPONENTFILES_DIR);
}

class EsdlMonitorTemplateCmd : public EsdlConvertCmd
{
public:
    EsdlMonitorTemplateCmd(){}

    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg != '-')
            {
                if (optSource.isEmpty())
                    optSource.set(arg);
                else if (optService.isEmpty())
                    optService.set(arg);
                else if (optMethod.isEmpty())
                    optMethod.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument: %s\n", arg);
                    usage();
                    return false;
                }
            }
            else
            {
                if (EsdlConvertCmd::parseCommandLineOption(iter))
                    continue;
                if (EsdlConvertCmd::matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                    return false;
            }
        }

        return true;
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optSource.isEmpty())
        {
            usage();
            throw( MakeStringException(0, "Error: Path to ESDL Source required"));
        }

        if( optService.isEmpty() )
        {
            usage();
            throw( MakeStringException(0, "An ESDL service name must be specified") );
        }

        if( optMethod.isEmpty() )
        {
            usage();
            throw( MakeStringException(0, "An ESDL method name must be specified") );
        }
        cmdHelper.verbose = optVerbose;
        return true;
    }

    void addMonitoringChildren(StringBuffer &xml, IPropertyTree *esdl, IPropertyTree *cur, unsigned indent)
    {
        const char *base_type = cur->queryProp("@base_type");
        if (base_type && *base_type)
        {
            VStringBuffer xpath("EsdlStruct[@name='%s'][1]", base_type);
            IPropertyTree *base = esdl->queryPropTree(xpath);
            if (base)
                addMonitoringChildren(xml, esdl, base, indent);
        }
        Owned<IPropertyTreeIterator> it = cur->getElements("*");
        ForEach(*it)
        {
            IPropertyTree &item = it->query();
            const char *elem = item.queryName();
            const char *name = item.queryProp("@name");
            if (streq(elem, "EsdlElement"))
            {
                const char *complex_type = item.queryProp("@complex_type");
                if (complex_type && *complex_type)
                {
                    VStringBuffer xpath("EsdlStruct[@name='%s'][1]", complex_type);
                    IPropertyTree *st = esdl->queryPropTree(xpath);
                    if (!st)
                        xml.pad(indent).appendf("<%s type='%s'/>\n", name, complex_type);
                    else
                    {
                        xml.pad(indent).appendf("<%s>\n", name);
                        addMonitoringChildren(xml, esdl, st, indent+2);
                        xml.pad(indent).appendf("</%s>\n", name);
                    }
                }
                else
                {
                    xml.pad(indent).appendf("<%s/>\n", name);
                }
            }
            else if (streq(elem, "EsdlArray"))
            {
                const char *item_name = item.queryProp("@item_tag");
                const char *type = item.queryProp("@type");
                if (!type || !*type)
                    type = "string";

                VStringBuffer xpath("EsdlStruct[@name='%s'][1]", type);
                IPropertyTree *st = esdl->queryPropTree(xpath);
                if (!st)
                {
                    if (!item_name || !*item_name)
                        item_name = "Item";
                    xml.pad(indent).appendf("<%s>\n", name);
                    xml.pad(indent+2).appendf("<%s/>\n", item_name);
                    xml.pad(indent).appendf("</%s>\n", name);
                }
                else
                {
                    if (!item_name || !*item_name)
                        item_name = type;
                    xml.pad(indent).appendf("<%s diff_match=''>\n", name);
                    xml.pad(indent+2).appendf("<%s>\n", item_name);
                    addMonitoringChildren(xml, esdl, st, indent+4);
                    xml.pad(indent+2).appendf("</%s>\n", item_name);
                    xml.pad(indent).appendf("</%s>\n", name);
                }
            }
            else if (streq(elem, "EsdlEnumRef"))
            {
                xml.pad(indent).appendf("<%s type='enum'/>\n", name);
            }
            else
            {
                xml.pad(indent).appendf("<%s unimplemented='%s'/>\n", name, elem);
            }

        }
    }

    void createMonitoringTemplate(StringBuffer &xml, IPropertyTree *esdl, const char *method)
    {
        xml.append("<ResultMonitoringTemplate>\n");
        VStringBuffer typeName("%sResponse", method);
        xml.append("  <").append(typeName).append(">\n");
        VStringBuffer xpath("*[@name='%s'][1]", typeName.str());
        IPropertyTree *responseType = esdl->queryPropTree(xpath);
        if (responseType)
            addMonitoringChildren(xml, esdl, responseType, 4);
        xml.append("  </").append(typeName).append(">\n");
        xml.append("</ResultMonitoringTemplate>");
    }

    virtual int processCMD()
    {
        loadServiceDef();

        StringBuffer xml("<esdl>");
        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService.get(), optMethod.get(), ESDLOPTLIST_DELIMITER, 0, nullptr, 0 );
        cmdHelper.defHelper->toXML(*structs, xml, 0, NULL, 0);
        xml.append("</esdl>");

        Owned<IPropertyTree> depTree = createPTreeFromXMLString(xml, ipt_ordered);
        removeEclHidden(depTree);
        toXML(depTree, xml.clear());

        StringBuffer monTemplate;
        createMonitoringTemplate(monTemplate, depTree, optMethod);

        VStringBuffer templatefile("monitor_template_%s.xml", optMethod.str());
        saveAsFile(".", templatefile, monTemplate);
        return 0;
    }

    virtual void usage()
    {
        puts("\nesdl monitor-template");
        puts("  Generates a blank Differencing template for the given ESDL based query.\n");
        puts("Usage:");
        puts("esdl monitor-template <esdlSourcePath> <serviceName> <methodName> [options]\n" );
        puts("  <esdlSourcePath> Path to the ESDL Definition file containing the ESDL service definition" );
        puts("                   to create a ECL result differencing template for." );
        puts("  <serviceName>    Name of the ESDL Service to generate the template for." );
        puts("  <methodName>     Name of the ESDL method to generate the template for." );

        EsdlConvertCmd::usage();
    }

    virtual void loadServiceDef()
    {
        cmdHelper.loadDefinition(optSource, optService, 0);
    }

public:
    EsdlCmdHelper cmdHelper;

    StringAttr optService;
    StringAttr optMethod;
};

class EsdlMonitorCmd : public EsdlConvertCmd
{
public:
    EsdlMonitorCmd() : optFlags(DEPFLAG_COLLAPSE|DEPFLAG_ARRAYOF){}

    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg != '-')
            {
                if (optSource.isEmpty())
                    optSource.set(arg);
                else if (optService.isEmpty())
                    optService.set(arg);
                else if (optMethod.isEmpty())
                    optMethod.set(arg);
                else if (diffTemplatePath.isEmpty())
                    diffTemplatePath.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument: %s\n", arg);
                    usage();
                    return false;
                }
            }
            else
            {
                if (iter.matchOption(optXsltPath, ESDLOPT_XSLT_PATH))
                    continue;
                if (EsdlConvertCmd::parseCommandLineOption(iter))
                    continue;
                if (EsdlConvertCmd::matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                    return false;
            }
        }

        return true;
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optSource.isEmpty())
        {
            usage();
            throw( MakeStringException(0, "\nError: Path to ESDL source file required\n"));
        }

        if( optService.isEmpty() )
        {
            usage();
            throw( MakeStringException(0, "\nAn ESDL service name must be specified\n") );
        }

        if( optMethod.isEmpty() )
        {
            usage();
            throw( MakeStringException(0, "\nAn ESDL method name must be specified\n") );
        }

        if (!diffTemplatePath.length())
        {
            usage();
            throw( MakeStringException(0, "\nA differencing template name must be provided\n") );
        }

        if (optXsltPath.isEmpty())
        {
            StringBuffer tmp;
            optXsltPath.set(getEsdlCmdComponentFilesPath(tmp));
        }

        diffTemplateContent.loadFile(diffTemplatePath);
        cmdHelper.verbose = optVerbose;

        return true;
    }

    class XpathTrack
    {
    public:
        XpathTrack(){}
        void push(const char *node)
        {
            size32_t len = xpath.length();
            pos.append(len);
            if (len)
                xpath.append('/');
            xpath.append(node);
        }
        void pop()
        {
            if (!pos.length())
                return;
            size32_t len = pos.popGet();
            xpath.setLength(len);
        }
        const char *str(){return xpath.str();}
    private:
        ArrayOf<size32_t> pos;
        StringBuffer xpath;
    };

    class XTrackScope
    {
    public:
        XTrackScope(XpathTrack &_xt, const char *name) : xt(_xt)
        {
            if (name && *name)
            {
                xt.push(name);
                expanded = true;
            }
        }
        ~XTrackScope()
        {
            if (expanded)
                xt.pop();
        }
        XpathTrack &xt;
        bool expanded = false;
    };

    const char *findInheritedAttribute(IPropertyTree &depTree, IPropertyTree *structType, const char *attr)
    {
        const char *s = structType->queryProp(attr);
        if (s && *s)
            return s;
        const char *base_type = structType->queryProp("@base_type");
        if (!base_type || !*base_type)
            return NULL;

        VStringBuffer xpath("EsdlStruct[@name='%s'][1]", base_type);
        IPropertyTree *baseType = depTree.queryPropTree(xpath);
        if (!baseType)
            return NULL;
        return findInheritedAttribute(depTree, baseType, attr);
    }

    IPropertyTree *findInheritedXpath(IPropertyTree &depTree, IPropertyTree *structType, const char *path)
    {
        IPropertyTree *t= structType->queryPropTree(path);
        if (t)
            return t;
        const char *base_type = structType->queryProp("@base_type");
        if (!base_type || !*base_type)
            return NULL;

        VStringBuffer xpath("EsdlStruct[@name='%s'][1]", base_type);
        IPropertyTree *baseType = depTree.queryPropTree(xpath);
        if (!baseType)
            return NULL;
        return findInheritedXpath(depTree, baseType, path);
    }

    void addAllDiffIdElementsToMap(IPropertyTree &depTree, IPropertyTree *st, IPropertyTree *map)
    {
        const char *base_type = st->queryProp("@base_type");
        if (base_type && *base_type)
        {
            VStringBuffer xpath("EsdlStruct[@name='%s'][1]", base_type);
            IPropertyTree *baseType = depTree.queryPropTree(xpath);
            if (baseType)
                addAllDiffIdElementsToMap(depTree, baseType, map);
        }
        Owned<IPropertyTreeIterator> children = st->getElements("EsdlElement");
        ForEach(*children)
        {
            IPropertyTree &child = children->query();
            if (!child.hasProp("@complex_type"))
            {
                addDiffIdPartToMap(depTree, st, map, child.queryProp("@name"));
            }
            else
            {
                IPropertyTree *partMap = ensurePTree(map, child.queryProp("@name"));
                VStringBuffer xpath("EsdlStruct[@name='%s']", child.queryProp("@complex_type"));
                IPropertyTree *structType = depTree.queryPropTree(xpath);
                if (!structType)
                    map->setProp("@ftype", "str"); //ECL compiler will find the error
                else
                    addDiffIdPartToMap(depTree, structType, partMap, NULL);
            }
        }
    }
    void addDiffIdPartToMap(IPropertyTree &depTree, IPropertyTree *parent, IPropertyTree *map, const char *id)
    {
        StringBuffer part;
        const char *finger = nullptr;
        if (id && *id)
        {
            finger = strchr(id, '.');
            if (finger)
                part.append(finger-id, id);
            else
                part.append(id);
            part.trim();
        }
        if (!part.length()) //short hand for compare the entire struct
        {
            addAllDiffIdElementsToMap(depTree, parent, map);
        }
        else
        {
            VStringBuffer xpath("EsdlElement[@name='%s']", part.str());
            IPropertyTree *partElement = parent->queryPropTree(xpath);
            if (!partElement)
            {
                StringBuffer idpath(id);
                idpath.replace(' ','_').replace('.', '/');
                IPropertyTree *partMap = ensurePTree(map, idpath.str());
                partMap->setProp("@ftype", "string"); //let ecl compiler complain
            }
            else
            {
                IPropertyTree *partMap = ensurePTree(map, part.str());
                if (!partElement->hasProp("@complex_type")) //simple or none
                {
                    EsdlBasicElementType et = esdlSimpleType(partElement->queryProp("@type"));
                    switch (et)
                    {
                    case ESDLT_INT8:
                    case ESDLT_INT16:
                    case ESDLT_INT32:
                    case ESDLT_INT64:
                    case ESDLT_UINT8:
                    case ESDLT_UINT16:
                    case ESDLT_UINT32:
                    case ESDLT_UINT64:
                    case ESDLT_BYTE:
                    case ESDLT_UBYTE:
                        partMap->setProp("@ftype", "number");
                        break;
                    case ESDLT_BOOL:
                        partMap->setProp("@ftype", "bool");
                        break;
                    case ESDLT_FLOAT:
                    case ESDLT_DOUBLE:
                        partMap->setProp("@ftype", "float");
                        break;
                    case ESDLT_UNKOWN:
                    case ESDLT_STRING:
                    default:
                        partMap->setProp("@ftype", "string");
                        break;
                    }
                }
                else
                {
                    xpath.setf("EsdlStruct[@name='%s']", partElement->queryProp("@complex_type"));
                    IPropertyTree *structType = depTree.queryPropTree(xpath);
                    if (!structType)
                    {
                        partMap->setProp("@ftype", "str"); //ECL compiler will find the error
                        return;
                    }
                    else
                    {
                        addDiffIdPartToMap(depTree, structType, partMap, finger ? finger+1 : NULL);
                    }
                }
            }
        }
    }
    IPropertyTree *createDiffIdTypeMap(IPropertyTree &depTree, IPropertyTree *parent, StringArray &idparts)
    {
        Owned<IPropertyTree> map = createPTree();
        ForEachItemIn(i1, idparts)
            addDiffIdPartToMap(depTree, parent, map, idparts.item(i1));
        return map.getClear();
    }
    void flattenDiffIdTypeMap(IPropertyTree &map, StringArray &flat, XpathTrack &xtrack, const char *name)
    {
        XTrackScope xscope(xtrack, name);

        Owned<IPropertyTreeIterator> children = map.getElements("*");
        ForEach(*children)
        {
            IPropertyTree &child = children->query();
            if (child.hasProp("@ftype"))
            {
                XTrackScope xscope(xtrack, child.queryName());
                StringBuffer s(xtrack.str());
                s.replace('/', '.');
                VStringBuffer xml("<part ftype='%s' name='%s'/>", child.queryProp("@ftype"), s.str());
                flat.append(xml);
            }
            else
            {
                flattenDiffIdTypeMap(child, flat, xtrack, child.queryName());
            }
        }
    }
    IPropertyTree *createDiffIdTree(IPropertyTree &depTree, IPropertyTree *parent, const char *diff_match, const char *item_tag, StringBuffer &idname)
    {
        StringArray idparts;
        idparts.appendListUniq(diff_match, "+");
        idparts.sortAscii(true);

        VStringBuffer itemRef("%s.", item_tag ? item_tag : "");
        ForEachItemIn(i1, idparts)
        {
            StringBuffer s(idparts.item(i1));
            if (strncmp(s, itemRef, itemRef.length())==0)
            {
                s.remove(0, itemRef.length());
                idparts.replace(s, i1);
            }
            idname.append(s.trim());
        }

        Owned<IPropertyTree> map = createDiffIdTypeMap(depTree, parent, idparts);

        Owned<IPropertyTree> diffIdTree = createPTree("diff_match");
        diffIdTree->setProp("@name", idname.toLowerCase());
        XpathTrack xt;
        StringArray flat;
        flattenDiffIdTypeMap(*map, flat, xt, nullptr);

        ForEachItemIn(i2, flat)
            diffIdTree->addPropTree("part", createPTreeFromXMLString(flat.item(i2)));
        return diffIdTree.getClear();
    }
    bool getMonFirstDiffAttributeBool(IPropertyTree *tmplate, XpathTrack &xtrack, const char *attribute, bool def)
    {
        VStringBuffer xpath("%s[1]/%s", xtrack.str(), attribute);
        return tmplate->getPropBool(xpath, def);
    }
    const char *queryMonFirstDiffAttribute(IPropertyTree *tmplate, XpathTrack &xtrack, const char *attribute, const char *def, bool allowEmpty)
    {
        VStringBuffer xpath("%s[1]/%s", xtrack.str(), attribute);
        const char *s = tmplate->queryProp(xpath);
        return (s && (allowEmpty || *s)) ? s : def;
    }
    const char *getArrayTemplateItemName(IPropertyTree *tmplate, XpathTrack &xtrack, IPropertyTree &arrayDef)
    {
        const char *name = arrayDef.queryProp("@item_tag");
        if (name && *name)
        {
            XTrackScope scope(xtrack, name);
            if (tmplate->hasProp(xtrack.str()))
                return name;
        }
        const char *type = arrayDef.queryProp("@type");
        if (type && *type)
        {
            XTrackScope scope(xtrack, type);
            if (tmplate->hasProp(xtrack.str()))
                return type;
        }
        XTrackScope scope(xtrack, "Item");
        if (tmplate->hasProp(xtrack.str()))
            return "Item";
        return nullptr;
    }
    void setBaseTypesPropBool(IPropertyTree &depTree, IPropertyTree *st, const char *attr, bool value)
    {
        const char *base_type = st->queryProp("@base_type");
        if (base_type && *base_type)
        {
            VStringBuffer xpath("EsdlStruct[@name='%s'][1]", base_type);
            IPropertyTree *baseType = depTree.queryPropTree(xpath);
            if (baseType)
            {
                setBaseTypesPropBool(depTree, baseType, attr, value);
                baseType->setPropBool(attr, value);
            }

        }
    }

    void addCtxDiffSelectors(IPropertyTree *ctx, IPropertyTree *ctxSelectors, IPropertyTree *stSelectors)
    {
        if (!stSelectors)
            return;
        Owned<IPropertyTreeIterator> selectors = stSelectors->getElements("selector");
        ForEach(*selectors)
        {
            const char *name = selectors->query().queryProp(nullptr);
            if (!name || !*name)
                continue;
            VStringBuffer xpath("selector[@name='%s']", name);
            VStringBuffer flagName("_CheckSelector_%s", name);
            ctx->setPropBool(flagName, ctxSelectors && ctxSelectors->hasProp(xpath));
        }
    }
    IPropertyTree *getBaseDepType(IPropertyTree &depTree, IPropertyTree *st)
    {
        if (!st->hasProp("@base_type"))
            return nullptr;
        VStringBuffer xpath("EsdlStruct[@name='%s']", st->queryProp("@base_type"));
        return depTree.queryPropTree(xpath);
    }
    bool noteMonitoredArrayItem(IPropertyTree &depTree, IPropertyTree *st)
    {
        if (!st)
            return false;
        IPropertyTree *baseType = getBaseDepType(depTree, st);
        if (baseType && noteMonitoredArrayItem(depTree, baseType))
            return true;
        if (st->getPropBool("@diff_monitor") && st->getPropBool("@_usedInArray"))
        {
            st->setPropBool("@diff_ordinal", true);
            return true;
        }
        return false;
    }
    void noteAllMonitoredArrayItems(IPropertyTree &depTree)
    {
        Owned<IPropertyTreeIterator> structs = depTree.getElements("EsdlStruct");
        ForEach(*structs)
        {
            IPropertyTree &st = structs->query();
            noteMonitoredArrayItem(depTree, &st);
        }
    }
    void buildOptionalFieldList(IPropertyTree *origCtx, IPropertyTree &depTree, IPropertyTree *st, const char *_name, XpathTrack &fieldtrack, StringArray &optionalFields)
    {
        XTrackScope scope(fieldtrack, _name);
        IPropertyTree *baseType = getBaseDepType(depTree, st);
        if (baseType)
            buildOptionalFieldList(origCtx, depTree, baseType, NULL, fieldtrack, optionalFields);
        Owned<IPropertyTreeIterator> children = st->getElements("*");
        ForEach(*children)
        {
            IPropertyTree &child = children->query();
            if (strieq(child.queryName(),"_diff_selectors"))
            {
                addCtxDiffSelectors(origCtx, origCtx->queryPropTree("_diff_selectors"), &child);
                continue;
            }
            const char *childname = child.queryProp("@name");
            if (child.getPropBool("@_mon") && child.getPropBool("@_nomon")) //having both makes it context sensitive
            {
                VStringBuffer flagName("_CheckField_%s", childname);
                if (origCtx->getPropBool(flagName))
                {
                    XTrackScope childscope(fieldtrack, childname);
                    optionalFields.appendUniq(fieldtrack.str());
                }
            }
            IPropertyTree *childCtx = origCtx->queryPropTree(childname);
            if (!childCtx)
                continue;
            const char *typeName = nullptr;
            if (child.hasProp("@complex_type"))
                typeName = child.queryProp("@complex_type");
            else if (strieq(child.queryName(), "EsdlArray"))
                typeName = child.queryProp("@type");

            IPropertyTree *complexType = nullptr;
            VStringBuffer xpath("EsdlStruct[@name='%s']", typeName);
            complexType = depTree.queryPropTree(xpath);
            if (complexType)
                buildOptionalFieldList(childCtx, depTree, complexType, childname, fieldtrack, optionalFields);
        }

    }
    bool expandDiffTrees(IPropertyTree *ctxLocal, IPropertyTree &depTree, IPropertyTree *st, XpathTrack &xtrack, const char *name, bool monitored, bool inMonSection, StringArray &allSelectors)
    {
        bool mon_child = false; //we have monitored descendants
        XTrackScope xscope(xtrack, name);
        if (monitored)
            st->setPropBool("@diff_monitor", true);

        StringBuffer xpath;
        const char *base_type = st->queryProp("@base_type");
        if (base_type && *base_type)
        {
            xpath.setf("EsdlStruct[@name='%s'][1]", base_type);
            IPropertyTree *baseType = depTree.queryPropTree(xpath);
            if (baseType)
            {
                baseType->setPropBool("@_base", true);
                mon_child = expandDiffTrees(ctxLocal, depTree, baseType, xtrack, NULL, monitored, inMonSection, allSelectors); //walk the type info
            }
        }

        //check for global settings for this structure
        IPropertyTree *globalStructDef = globals->queryPropTree(st->queryProp("@name"));
        {
            Owned<IPropertyTreeIterator> children = st->getElements("*");
            ForEach(*children)
            {
                IPropertyTree &child = children->query();
                const char *name = child.queryProp("@name");
                if (!name || !*name)
                    continue;

                if (globalStructDef && globalStructDef->hasProp(name))
                {
                    IPropertyTree *globalChildDef = globalStructDef->queryPropTree(name);
                    mergePTree(&child, globalChildDef);
                }
                bool childMonSection = inMonSection;
                const char *selectorList = child.queryProp("@diff_monitor"); //esdl-xml over rides parent
                XTrackScope xscope(xtrack, child.queryProp("@name"));
                const char *compare = queryMonFirstDiffAttribute(respTemplate, xtrack, "@diff_comp", nullptr, false);
                if (compare)
                {
                    Owned<IPropertyTree> compareNode = createPTree("diff_comp");
                    compareNode->setProp("@xpath", xtrack.str());
                    compareNode->setProp("@compare", compare);
                    child.addPropTree("diff_compare", compareNode.getClear());
                }
                selectorList = queryMonFirstDiffAttribute(respTemplate, xtrack, "@diff_monitor", selectorList, true);
                bool childMonitored = monitored;
                if (selectorList)
                {
                    childMonitored = *selectorList!=0;
                    StringArray selectors;
                    selectors.appendListUniq(selectorList, "|");
                    allSelectors.appendListUniq(selectorList, "|");
                    IPropertyTree *selectorsTree = ensurePTree(&child, "diff_selectors");
                    IPropertyTree *pathTree = selectorsTree->addPropTree("entry", createPTree());
                    pathTree->setProp("@path", xtrack.str());
                    if (!selectors.length())
                        pathTree->addProp("category", "false");
                    ForEachItemIn(i1, selectors)
                    {
                        StringBuffer s("Monitor");
                        pathTree->addProp("category", s.append(selectors.item(i1)).str());
                    }
                }

                if (childMonitored) //structure can be reused in different locations.  track yes and no monitoring separately so we know whether we need to check context
                    child.setPropBool("@_mon", true);
                else if (monitored)
                    child.setPropBool("@_nomon", true);
                VStringBuffer ctxMonFlag("_CheckField_%s", name);
                ctxLocal->setPropBool(ctxMonFlag, childMonitored);

                const char *childElementName =child.queryName();
                if (strieq(childElementName, "EsdlElement"))
                {
                    const char *complex_type = child.queryProp("@complex_type");
                    if (complex_type && *complex_type)
                    {
                        xpath.setf("EsdlStruct[@name='%s'][1]", complex_type);
                        IPropertyTree *childType = depTree.queryPropTree(xpath);
                        if (childType)
                        {
                            childType->setPropBool("@_used", true);
                            IPropertyTree *ctxChild = ensurePTree(ctxLocal, name);
                            bool mon_elem = expandDiffTrees(ctxChild, depTree, childType, xtrack, NULL, childMonitored, childMonSection, allSelectors); //walk the type info
                            if (!monitored && mon_elem)
                            {
                                child.setPropBool("@mon_child", true);
                                if (!mon_child)
                                    mon_child = true;
                            }
                        }
                    }
                }
                else
                {
                    if (strieq(childElementName, "EsdlArray"))
                    {
                        const char *item_type = child.queryProp("@type");
                        if (item_type && *item_type)
                        {
                            xpath.setf("EsdlStruct[@name='%s'][1]", item_type);
                            IPropertyTree *childType = depTree.queryPropTree(xpath);
                            if (childType)
                            {
                                childType->setPropBool("@_used", true);
                                childType->setPropBool("@_usedInArray", true);
                                const char *diff_match = queryMonFirstDiffAttribute(respTemplate, xtrack, "@diff_match", nullptr, false);
                                if (!diff_match || !*diff_match)
                                    diff_match = child.queryProp("@diff_match");
                                if (!diff_match || !*diff_match)
                                    diff_match = findInheritedAttribute(depTree, childType, "@diff_match");
                                if (diff_match && *diff_match)
                                {
                                    StringBuffer idname;
                                    Owned<IPropertyTree> diffIdTree = createDiffIdTree(depTree, childType, diff_match, child.queryProp("@item_tag"), idname);
                                    xpath.setf("diff_match[@name='%s']", idname.toLowerCase().str());

                                    IPropertyTree *arrayDiffKeys = child.queryPropTree("DiffMatchs");
                                    if (!arrayDiffKeys)
                                        arrayDiffKeys = child.addPropTree("DiffMatchs", createPTree("DiffMatchs"));
                                    if (arrayDiffKeys && !arrayDiffKeys->hasProp(xpath))
                                        arrayDiffKeys->addPropTree("diff_match", LINK(diffIdTree));


                                    IPropertyTree *itemTypeDiffKeys = childType->queryPropTree("DiffMatchs");
                                    if (!itemTypeDiffKeys)
                                        itemTypeDiffKeys = childType->addPropTree("DiffMatchs", createPTree("DiffMatchs"));
                                    if (itemTypeDiffKeys && !itemTypeDiffKeys->hasProp(xpath))
                                        itemTypeDiffKeys->addPropTree("diff_match", LINK(diffIdTree));
                                }

                                IPropertyTree *ctxChild = ensurePTree(ctxLocal, name);
                                const char *item_tag = getArrayTemplateItemName(respTemplate, xtrack, child);
                                bool mon_elem = expandDiffTrees(ctxChild, depTree, childType, xtrack, item_tag, childMonitored, childMonSection, allSelectors); //walk the type info
                                if (!monitored && mon_elem)
                                {
                                    child.setPropBool("@mon_child", true);
                                    if (!mon_child)
                                        mon_child = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        if (monitored)
            return true;
        if (mon_child)
        {
            setBaseTypesPropBool(depTree, st, "@mon_child_base", true);
            st->setPropBool("@mon_child", true);
            return true;
        }
        return false;
    }

    IPropertyTree *checkExtractNestedResponseType(IPropertyTree *depTree, IPropertyTree *respType, StringBuffer &respTypeName)
    {
        const char *altTypeName = respType->queryProp("EsdlElement[@name='response']/@complex_type");
        if (altTypeName && *altTypeName)
        {
            VStringBuffer xpath("EsdlStruct[@name='%s']", altTypeName);
            IPropertyTree *altType = depTree->queryPropTree(xpath);
            if (altType)
            {
                respType = altType;
                respTypeName.set(altTypeName);
            }
        }
        return respType;
    }

    virtual int processCMD()
    {
        cmdHelper.loadDefinition(optSource, optService, 0);

        Owned<IEsdlDefObjectIterator> responseEsdl = cmdHelper.esdlDef->getDependencies(optService, optMethod, 0, nullptr, DEPFLAG_INCLUDE_RESPONSE | DEPFLAG_INCLUDE_METHOD | DEPFLAG_ECL_ONLY);
        Owned<IEsdlDefObjectIterator> requestEsdl = cmdHelper.esdlDef->getDependencies(optService, optMethod, 0, nullptr, DEPFLAG_INCLUDE_REQUEST | DEPFLAG_ECL_ONLY);
        Owned<IEsdlDefinitionHelper> defHelper = createEsdlDefinitionHelper();

        StringBuffer xml("<esxdl>\n");
        xml.append("<RequestInfo>\n");
        defHelper->toXML(*requestEsdl, xml, 0, nullptr, 0);
        xml.append("</RequestInfo>\n");

        defHelper->toXML(*responseEsdl, xml, 0, nullptr, 0);

        xml.append("\n</esxdl>");

        Owned<IPropertyTree> depTree = createPTreeFromXMLString(xml, ipt_ordered);

        monitoringTemplate.setown(createPTreeFromXMLString(diffTemplateContent, ipt_ordered));
        globals = ensurePTree(monitoringTemplate, "Globals");

        removeEclHidden(depTree);
        removeEclHidden(depTree->getPropTree("RequestInfo"));

        VStringBuffer xpath("EsdlMethod[@name='%s']/@response_type", optMethod.str());
        StringBuffer resp_type = depTree->queryProp(xpath);
        if (!resp_type.length())
            throw( MakeStringException(0, "Esdl Method not found %s", optMethod.str()));

        if (resp_type.length()>2 && resp_type.charAt(resp_type.length()-2)=='E' && resp_type.charAt(resp_type.length()-1)=='x')
            resp_type.setLength(resp_type.length()-2);

        respTemplate = monitoringTemplate->queryPropTree(resp_type);

        IPropertyTree *respTree = depTree->queryPropTree(xpath.setf("EsdlResponse[@name='%s']", resp_type.str()));
        if (!respTree)
            respTree = depTree->queryPropTree(xpath.setf("EsdlStruct[@name='%s']", resp_type.str()));

        if (!respTree)
            throw( MakeStringException(0, "Esdl Response type '%s' definition not found", resp_type.str()));

        respTree = checkExtractNestedResponseType(depTree, respTree, resp_type);

        XpathTrack xtrack;
        Owned<IPropertyTree> ctxTree = createPTree();
        StringArray allSelectors;
        expandDiffTrees(ctxTree, *depTree, respTree, xtrack, NULL, false, false, allSelectors);

        noteAllMonitoredArrayItems(*depTree);

        XpathTrack fieldtrack;
        StringArray optionalFields;
        buildOptionalFieldList(ctxTree, *depTree, respTree, NULL, fieldtrack, optionalFields);
        if (optionalFields.length())
        {
            IPropertyTree *optionalTree = depTree->addPropTree("OptionalFields", LINK(createPTree()));
            ForEachItemIn(i1, optionalFields)
                optionalTree->addProp("Path", optionalFields.item(i1));
        }
        if (allSelectors.length())
        {
            IPropertyTree *selectorTree = depTree->addPropTree("Selectors", LINK(createPTree()));
            ForEachItemIn(i2, allSelectors)
                selectorTree->addProp("Selector", allSelectors.item(i2));
        }

        IPropertyTree *eclSection = monitoringTemplate->queryPropTree("ECL");
        if (eclSection)
            depTree->addPropTree("ECL", LINK(eclSection));

        depTree->addPropTree("Template", LINK(monitoringTemplate)); //make available to xslt

        toXML(depTree, xml.clear()); //refresh changes

        VStringBuffer filename("%s_preprocess.xml", optMethod.str());
        saveAsFile(".", filename, xml);

        Owned<IXslProcessor> xslp = getXslProcessor();
        Owned<IXslTransform> xform = xslp->createXslTransform();
        StringBuffer fullXsltPath(optXsltPath);
        fullXsltPath.append("/xslt/esdl2monitor.xslt");
        xform->loadXslFromFile(fullXsltPath);
        xform->setXmlSource(xml.str(), xml.length());

        StringBuffer stringvar;
        xform->setParameter("requestType", stringvar.setf("'%s'", depTree->queryProp(xpath.setf("EsdlMethod[@name='%s']/@request_type", optMethod.str()))));
        xform->setParameter("responseType", stringvar.setf("'%s'", resp_type.str()));
        xform->setParameter("queryName", stringvar.setf("'%s'", monitoringTemplate->queryProp("@queryName")));

        StringBuffer ecl;

        xform->setParameter("diffmode", "'Monitor'");
        xform->setParameter("diffaction", "'Create'");

        xform->setParameter("platform", "'roxie'");
        xform->transform(ecl);
        filename.setf("MonitorRoxie_create_%s.ecl", optMethod.str());
        saveAsFile(".", filename, ecl);

        xform->setParameter("platform", "'esp'");
        xform->transform(ecl.clear());
        filename.setf("MonitorESP_create_%s.ecl", optMethod.str());
        saveAsFile(".", filename, ecl);

        xform->setParameter("diffaction", "'Run'");

        xform->setParameter("platform", "'roxie'");
        xform->transform(ecl.clear());
        filename.setf("MonitorRoxie_run_%s.ecl", optMethod.str());
        saveAsFile(".", filename, ecl);

        xform->setParameter("platform", "'esp'");
        xform->transform(ecl.clear());
        filename.setf("MonitorESP_run_%s.ecl", optMethod.str());
        saveAsFile(".", filename, ecl);

        xform->setParameter("diffmode", "'Compare'");
        xform->transform(ecl.clear());
        filename.setf("Compare_%s.ecl", optMethod.str());
        saveAsFile(".", filename, ecl);

        return 0;
    }

    virtual void usage()
    {
        puts("\nesdl monitor");
        puts("  Generates ECL code to perform a comparison between two ECL results from the specified");
        puts("  ESDL based query.");
        puts("Usage:");
        puts("esdl monitor <esdlSourcePath> <serviceName> <methodName> <diffTemplate> [options]\n");
        puts("  <esdlSourcePath> Path to the ESDL Definition file containing the ESDL service definition");
        puts("                   to perform result differencing on.");
        puts("  <serviceName>    Name of the service to use, as defined in the specified ESDL source file.");
        puts("  <methodName>     Name of method to use as defined in the specified service.");
        puts("  <diffTemplate>   The template that specifies the differencing and monitoring rules usedto generate the result");
        puts("                   differencing and monitoring ECL code for the given service method.\n" );

        EsdlConvertCmd::usage();
    }

    void setFlag( unsigned f ) { optFlags |= f; }
    void unsetFlag( unsigned f ) { optFlags &= ~f; }

public:
    EsdlCmdHelper cmdHelper;
    Owned<IPropertyTree> monitoringTemplate;
    IPropertyTree *respTemplate = nullptr;
    IPropertyTree *globals;

    StringBuffer diffTemplatePath;
    StringBuffer diffTemplateContent;
    StringAttr optService;
    StringAttr optXsltPath;
    StringAttr optMethod;
    unsigned optFlags;
};


IEsdlCommand *createEsdlMonitorCommand(const char *cmdname)
{
    if (strieq(cmdname, "MONITOR-TEMPLATE"))
        return new EsdlMonitorTemplateCmd();
    if (strieq(cmdname, "MONITOR"))
        return new EsdlMonitorCmd();

    return nullptr;
}
