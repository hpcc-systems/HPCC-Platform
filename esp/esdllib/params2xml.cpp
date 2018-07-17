/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#pragma warning(disable : 4786)

#include "jliball.hpp"
#include "esdl_def.hpp"
#include "params2xml.hpp"


void child2xml(IEsdlDefinition *esdl, IEsdlDefObject &child, StringBuffer &flexpath, IProperties *params, StringBuffer &xmlstr, StringBuffer &path, unsigned flags, double ver, int flexpath_setpoint);

void paramsBaseStruct2xml(IEsdlDefinition *esdl, IEsdlDefStruct *est, IProperties *params, StringBuffer &xmlstr, StringBuffer &path, StringBuffer &flexpath, unsigned flexpath_setpoint, unsigned flags, double ver)
{
    if (esdl && est && est->queryProp("base_type"))
    {
        IEsdlDefStruct *base = esdl->queryStruct(est->queryProp("base_type"));
        if (base)
        {
            if (base->queryProp("base_type"))
                paramsBaseStruct2xml(esdl, base, params, xmlstr, path, flexpath, flexpath_setpoint, flags, ver);

            Owned<IEsdlDefObjectIterator> bt = base->getChildren();
            ForEach(*bt)
                child2xml(esdl, bt->query(), flexpath, params, xmlstr, path, flags, ver, flexpath_setpoint);
        }
    }
}

void paramsStruct2xml(IEsdlDefinition *esdl, IEsdlDefStruct *est, const char *tagname, IProperties *params, StringBuffer &xmlstr, StringBuffer &path, unsigned flags, double ver, bool isroot)
{
    xmlstr.append('<').append(tagname).append('>');
    unsigned xml_setpoint=xmlstr.length();

    StringBuffer flexpath(path);
    if (flexpath.length())
        flexpath.append(".");
    unsigned flexpath_setpoint = flexpath.length();

    paramsBaseStruct2xml(esdl, est, params, xmlstr, path, flexpath, flexpath_setpoint, flags, ver);

    Owned<IEsdlDefObjectIterator> it = est->getChildren();
    ForEach(*it)
        child2xml(esdl, it->query(), flexpath, params, xmlstr, path, flags, ver, flexpath_setpoint);

    if (isroot || xml_setpoint<xmlstr.length())
        xmlstr.append("</").append(tagname).append('>');
    else
        xmlstr.setLength(xml_setpoint - strlen(tagname) - 2);
}


esdl_decl void params2xml(IEsdlDefinition *def, const char *structname, IProperties *params, StringBuffer &xmlstr, unsigned flags, double ver)
{
    IEsdlDefStruct *est = def->queryStruct(structname);
    if (!est)
        throw MakeStringException(-1, "parms2xml: ESDL Struct %s not found", structname);

    StringBuffer path;
    paramsStruct2xml(def, est, structname, params, xmlstr, path, flags, ver, true);
}



esdl_decl void params2xml(IEsdlDefinition *def, const char *service, const char *method, EsdlDefTypeId esdltype, IProperties *params, StringBuffer &xmlstr, unsigned flags, double ver)
{
    if (esdltype!=EsdlTypeRequest && esdltype!=EsdlTypeResponse)
        throw MakeStringException(-1, "parms2xml: Only ESDL request and response types supported");
    IEsdlDefService *srv = def->queryService(service);
    if (!srv)
        throw MakeStringException(-1, "parms2xml: ESDL Service %s not found", service);
    IEsdlDefMethod *mth = srv->queryMethodByName(method);
    if (!mth)
        throw MakeStringException(-1, "parms2xml: ESDL Method %s not found", method);
    if (esdltype==EsdlTypeRequest)
        params2xml(def, mth->queryRequestType(), params, xmlstr, flags, ver);
    else if (esdltype==EsdlTypeResponse)
        params2xml(def, mth->queryResponseType(), params, xmlstr, flags, ver);
}

void child2xml(IEsdlDefinition *esdl, IEsdlDefObject &child, StringBuffer &flexpath, IProperties *params, StringBuffer &xmlstr, StringBuffer &path, unsigned flags, double ver, int flexpath_setpoint)
{
    if (child.checkVersion(ver))
    {
        const char *name = child.queryName();
        const char *xml_tag = child.queryProp("xml_tag");
        const char *tagname;
        if (xml_tag && *xml_tag && flags & PARAMS2XML_OUTPUT_XML_TAG_NAME)
            tagname = xml_tag;
        else
            tagname = name;

        if (xml_tag && *xml_tag && flags & PARAMS2XML_INPUT_XML_TAG_NAME)
            name = xml_tag;

        switch (child.getEsdlType())
        {
            case EsdlTypeElement:
            {
                flexpath.append(name);
                const char *complex_type = child.queryProp("complex_type");
                if (complex_type)
                {
                    IEsdlDefStruct *cst = esdl->queryStruct(complex_type);
                    if (cst)
                        paramsStruct2xml(esdl, cst, tagname, params, xmlstr, flexpath, flags, ver, false);
                }
                else
                {
                    const char *val = params->queryProp(flexpath.str());
                    if (val && *val)
                    {
                        xmlstr.append('<').append(tagname).append('>');
                        encodeUtf8XML(val, xmlstr);
                        xmlstr.append("</").append(tagname).append('>');
                    }
                }
                flexpath.setLength(flexpath_setpoint);
                break;
            }
            case EsdlTypeEnumRef:
            {
                flexpath.append(name);
                const char *val = params->queryProp(flexpath.str());
                if (val && *val)
                {
                    xmlstr.append('<').append(tagname).append('>');
                    encodeUtf8XML(val, xmlstr);
                    xmlstr.append("</").append(tagname).append('>');
                }
                flexpath.setLength(flexpath_setpoint);
                break;
            }

            case EsdlTypeArray:
            {
                const char *item_tag = child.queryProp("item_tag");
                if (!item_tag || !*item_tag)
                    item_tag = "item";
                const char *artype = child.queryProp("type");
                IEsdlDefStruct *cst = esdl->queryStruct(artype);
                IEsdlDefArray* defArray = dynamic_cast<IEsdlDefArray*>(&child);
                bool isEsdlList = defArray->checkIsEsdlList();

                flexpath.append(name);

                if (cst)
                {
                    flexpath.append('.').append(artype);
                    unsigned arpath_setpoint = flexpath.length();

                    flexpath.append('.').append("itemcount");
                    const char *countstr = params->queryProp(flexpath.str());
                    flexpath.setLength(arpath_setpoint);

                    int itemcount = (countstr) ? atoi(countstr) : 0;
                    if (itemcount)
                    {
                        if (isEsdlList)
                            item_tag = name;
                        else
                            xmlstr.append('<').append(name).append('>');
                        unsigned arxml_setpoint = xmlstr.length();
                        for (int pos = 0; pos < itemcount; pos++)
                        {
                            flexpath.append('.').append(pos);
                            paramsStruct2xml(esdl, cst, item_tag, params, xmlstr, flexpath, flags, ver, false);
                            flexpath.setLength(arpath_setpoint);
                        }
                        if (!isEsdlList)
                        {
                            if (xmlstr.length()>arxml_setpoint)
                                xmlstr.append("</").append(name).append('>');
                            else
                                xmlstr.setLength(arxml_setpoint - strlen(name) - 2);
                        }
                    }
                }
                else
                {
                    const char *val = params->queryProp(flexpath.str());
                    if (val && *val)
                    {
                        if (isEsdlList)
                            item_tag = name;
                        else
                            xmlstr.append('<').append(name).append('>');
                        unsigned arxml_setpoint = xmlstr.length();
                        StringBuffer itemval;
                        for (const char *finger=val; *finger; finger++)
                        {
                            if (strchr("\n\r", *finger))
                            {
                                if (itemval.length())
                                {
                                    xmlstr.append('<').append(item_tag).append('>');
                                    encodeUtf8XML(itemval.str(), xmlstr);
                                    xmlstr.append("</").append(item_tag).append('>');
                                }
                                itemval.clear();
                            }
                            else
                                itemval.append(*finger);
                        }
                        if (!itemval.isEmpty()) //Last item has not been added yet!
                        {
                            xmlstr.append('<').append(item_tag).append('>');
                            encodeUtf8XML(itemval.str(), xmlstr);
                            xmlstr.append("</").append(item_tag).append('>');
                        }
                        if (!isEsdlList)
                        {
                            if (xmlstr.length()>arxml_setpoint)
                                xmlstr.append("</").append(name).append('>');
                            else
                                xmlstr.setLength(arxml_setpoint - strlen(name) - 2);
                        }
                    }
                }
                flexpath.setLength(flexpath_setpoint);
                break;
            }
            default:
                break;
        };
    }
}
