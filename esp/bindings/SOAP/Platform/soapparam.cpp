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

#include "esphttp.hpp"

#include "jstring.hpp"
#include "jprop.hpp"

#include "soapesp.hpp"
#include "soapmessage.hpp"
#include "soapparam.hpp"

void BaseEspParam::toJSON(IEspContext *ctx, StringBuffer &s, const char *tagname, bool encode)
{
    if (isNil && nilBH==nilRemove)
        return;
    appendJSONName(s, tagname);
    toJSONValue(s, encode);
}

void BaseEspParam::toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode)
{
    if (isNil && nilBH==nilRemove)
        return;
    appendXMLOpenTag(s, tagname, prefix);
    toXMLValue(s, encode);
    appendXMLCloseTag(s, tagname, prefix);
}

void BaseEspParam::toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *basepath, bool encode, const char *xsdtype, const char *prefix)
{
    if (isNil && nilBH!=nilIgnore)
        return;

    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return toJSON(ctx, s, tagname, true);
    toXML(ctx, s, tagname, prefix, encode);
}

void BaseEspParam::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix)
{
    addRpcValue(rpc_call, tagname, basepath, xsdtype, prefix, NULL);
}

bool BaseEspParam::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    StringBuffer path(basepath);
    if (basepath!=NULL && basepath[0]!=0)
        path.append("/");
    path.append(tagname);

    if (updateValue(rpc_call, path.str()))
    {
        if (optGroup && rpc_call.queryContext())
            rpc_call.queryContext()->addOptGroup(optGroup);
        isNil = false;
    }
    return isNil;
}

bool BaseEspParam::unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup)
{
    if (updateValue(soapval, tagname))
    {
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
        isNil = false;
    }
    return isNil;
}

bool BaseEspParam::unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    StringBuffer path;
    if (basepath && *basepath)
        path.append(basepath).append('.');
    path.append(tagname);

    if (updateValue(params.queryProp(path.str())))
    {
        isNil = false;
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
    }
    return isNil;
}

void SoapStringParam::toXMLValue(StringBuffer &s, bool encode)
{
    if (!encode)
        s.append(value);
    else
        encodeUtf8XML(value.str(), s, getEncodeNewlines() ? ENCODE_NEWLINES : 0);
}

void SoapStringParam::toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *basepath, bool encode, const char *xsdtype, const char *prefix, bool encodeJSON)
{
    if (isNil && nilBH!=nilIgnore)
        return;

    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        toJSON(ctx, s, tagname, encodeJSON);
    else
        toXML(ctx, s, tagname, prefix, encode);
}

void SoapStringParam::toJSONValue(StringBuffer &s, bool encode)
{
    appendJSONStringValue(s, NULL, (isNil) ? NULL : value.str(), encode, encode);
}

void SoapStringParam::addRpcValue(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix, bool *encodex)
{
    if (!isNil || nilBH==nilIgnore)
        rpc_call.add_value(basepath, prefix, tagname, xsdtype, value.str(), (!encodex) ? rpc_call.getEncodeXml() : *encodex);
}

bool SoapStringParam::updateValue(IRpcMessage &rpc_call, const char *path)
{
    StringBuffer tmp;
    if (rpc_call.get_value(path, tmp))
    {
        value.set(tmp.str());
        return true;
    }
    return false;
}

bool SoapStringParam::updateValue(CSoapValue &soapval, const char *tagname)
{
    return soapval.get_value(tagname, value.clear());
}

bool SoapStringParam::updateValue(const char *s)
{
    if (!s)
        return false;
    return !esp_convert(s, value.clear());
}

bool SoapStringParam::unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    if(attachments)
    {
        StringBuffer key;
        if (basepath && *basepath)
            key.append(basepath).append(".");
        key.append(tagname);

        StringBuffer* data = attachments->getValue(key.str());
        if (data)
        {
            StringBuffer path;
            if (basepath && *basepath)
                path.append(basepath).append(".");
            path.append(tagname);
            isNil=false;
            value.clear().swapWith(*data);
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            return true;
        }
    }
    return false;
}

void BaseEspStruct::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix)
{
    StringBuffer xml;
    Owned<IProperties> props;
    serializeContent(rpc_call.queryContext(), xml, props);

    if (xml.length() || nilBH==nilIgnore || props)
        rpc_call.add_value(basepath, prefix, tagname, xsdtype, xml.str(), false);
    if (props)
        rpc_call.add_attr(basepath, tagname, NULL, *props);
}

void BaseEspStruct::toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname)
{
    size32_t start = s.length();
    appendJSONName(s, tagname);
    s.append('{');
    size32_t check = s.length();
    serializeContent(ctx, s);
    if (s.length()==check)
        s.setLength(start);
    else
        s.append('}');
}

void BaseEspStruct::toXML(IEspContext *ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode)
{
    size32_t start = s.length();
    appendXMLOpenTag(s, tagname, prefix, false);
    size32_t check = s.length();
    serializeAttributes(ctx, s);
    s.append('>');
    serializeContent(ctx, s);
    if (nilBH==nilIgnore || s.length()!=check+1)
        appendXMLCloseTag(s, tagname, prefix);
    else
        s.setLength(start);
}

void BaseEspStruct::toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *basepath, bool encode, const char *xsdtype, const char *prefix)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return toJSON(ctx, s, tagname);
    toXML(ctx, s, tagname, prefix, encode);
}

bool BaseEspStruct::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    StringBuffer path;
    if (basepath && *basepath)
    {
        path.append(basepath);
        if (path.charAt(path.length()-1)!='/')
            path.append("/");
    }
    path.append(tagname);
    if (updateValue(rpc_call, path.str()))
    {
        if (optGroup && rpc_call.queryContext())
            rpc_call.queryContext()->addOptGroup(optGroup);
        return true;
    }
    return false;
}

bool BaseEspStruct::unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup)
{
    CSoapValue *sv = soapval.get_value(tagname);
    if (sv)
    {
        updateValue(ctx, *sv);
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
        return true;
    }
    return false;
}

bool BaseEspStruct::unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
   StringBuffer path;
   if (basepath && *basepath)
       path.append(basepath).append(".");
   path.append(tagname);

   if (updateValue(ctx, params, attachments, path.str()))
   {
       if (ctx && optGroup)
           ctx->addOptGroup(optGroup);
       return true;
   }
   return false;
}

void SoapParamBinary::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix)
{
    StringBuffer sb64;
    JBASE64_Encode(value.toByteArray(), value.length(), sb64);
    rpc_call.add_value(basepath, prefix, tagname, xsdtype, sb64);
}

void SoapParamBinary::toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname)
{
    delimitJSON(s);
    if (tagname && *tagname)
        s.append('"').append(tagname).append("\": ");
    s.append('"');
    JBASE64_Encode(value.toByteArray(), value.length(), s);
    s.append('"');
    return;
}

void SoapParamBinary::toXML(IEspContext *ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode)
{
    appendXMLOpenTag(s, tagname, prefix);
    JBASE64_Encode(value.toByteArray(), value.length(), s);
    appendXMLCloseTag(s, tagname, prefix);
}

void SoapParamBinary::toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *basepath, bool encode, const char *xsdtype, const char *prefix)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return toJSON(ctx, s, tagname);
    toXML(ctx, s, tagname, prefix, encode);
}

bool SoapParamBinary::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    StringBuffer path(basepath);
    if (basepath!=NULL && basepath[0]!=0)
        path.append("/");
    path.append(tagname);

    StringBuffer sb64;
    bool ret = rpc_call.get_value(path.str(), sb64);
    if (ret && optGroup && rpc_call.queryContext())
        rpc_call.queryContext()->addOptGroup(optGroup);

    if(sb64.length())
        JBASE64_Decode(sb64.str(), value);

    return ret;
}

bool SoapParamBinary::unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup)
{
    return false;
}

bool SoapParamBinary::unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    StringBuffer path;
    if (basepath && *basepath)
        path.append(basepath).append(".");
    path.append(tagname);

    const char* val = params.queryProp(path.str());
    if(val)
    {
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
        JBASE64_Decode(params.queryProp(path.str()), value);
        return true;
    }
    return false;
}


void SoapAttachString::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix)
{
    rpc_call.add_value(basepath, prefix, tagname, "Attachment", value);
}

void SoapAttachString::toJSON(IEspContext *ctx, StringBuffer &s, const char *tagname)
{
    appendJSONValue(s, tagname, value);
}

void SoapAttachString::toXML(IEspContext *ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode)
{
    appendXMLTag(s, tagname, value, prefix, ENCODE_NONE);
}

void SoapAttachString::toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *basepath, bool encode, const char *xsdtype, const char *prefix)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return toJSON(ctx, s, tagname);
    toXML(ctx, s, tagname, prefix, encode);
}

bool SoapAttachString::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup,const char *xsdtype, const char *prefix)
{
    StringBuffer path(basepath);
    if (basepath!=NULL && basepath[0]!=0)
        path.append("/");
    path.append(tagname);

    if (rpc_call.get_value(path.str(), value)) {
        if (optGroup && rpc_call.queryContext())
            rpc_call.queryContext()->addOptGroup(optGroup);
        return true;
    }
    return false;
}

bool SoapAttachString::unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup)
{
    return false;
}

bool SoapAttachString::unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix)
{
    return false;
}

StringBuffer &buildVarPath(StringBuffer &path, const char *tagname, const char *basepath, const char *item, const char *tail, int *idx)
{
    path.clear();
    if (basepath)
        path.append(basepath).append(".");
    path.append(tagname);
    if (item)
        path.append(".").append(item);
    if (tail)
        path.append(".").append(tail);
    if (idx)
        path.append(".").append(*idx);
    return path;
}

void EspBaseArrayParam::toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *itemname)
{
    unsigned count = getLength();
    if (!count)
    {
        if (nilBH!=nilRemove)
        {
            delimitJSON(s);
            if (tagname && *tagname)
                s.append('\"').append(tagname).append("\": ");
            s.append("[]");
        }
        return;
    }

    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        appendJSONName(s, tagname);
        if (tagname && *tagname && itemname && *itemname)
            s.append('{');
        if (!itemname || !*itemname)
            itemname = getItemTag(itemname);
        appendJSONName(s, itemname);
        s.append('[');
        for (unsigned  i=0; i<count; i++)
            toStrItem(ctx, s, i, itemname);
        s.append(']');
        if (tagname && *tagname && itemname && *itemname)
            s.append('}');
    }
}

void EspBaseArrayParam::toXML(IEspContext *ctx, StringBuffer &s, const char *tagname, const char *itemname, const char *prefix, bool encode)
{
    itemname = getItemTag(itemname);
    unsigned count = getLength();
    if (!count)
    {
        if (nilBH != nilRemove)
            appendXMLOpenTag(s, tagname, prefix, true, true);
        return;
    }

    appendXMLOpenTag(s, tagname, prefix);
    for (unsigned  i=0; i<count; i++)
        toStrItem(ctx, s, i, itemname);
    appendXMLCloseTag(s, tagname, prefix);
}

void EspBaseArrayParam::toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *itemname, const char *elementtype, const char *basepath, const char *prefix)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return toJSON(ctx, s, tagname, itemname);
    toXML(ctx, s, tagname, itemname, prefix, true);
}

bool EspBaseArrayParam::unmarshallItems(IEspContext* ctx, CSoapValue *sv, const char *itemname, const char *optGroup)
{
    if (!sv)
        return false;

    SoapValueArray* children = sv->query_children();
    if (children)
    {
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
        ForEachItemIn(i, *children)
        {
            CSoapValue &child = children->item(i);
            child.setEncodeXml(false);
            append(ctx, child);
        }
        return true;
    }
    return false;
}

bool EspBaseArrayParam::unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup, const char *itemname)
{
    return unmarshallItems(ctx, soapval.get_value(tagname), itemname, optGroup);
}

bool EspBaseArrayParam::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup, const char *prefix)
{
    StringBuffer path(basepath);
    if (basepath!=NULL && basepath[0]!=0)
        path.append("/");
    path.append(tagname);

    return unmarshallItems(rpc_call.queryContext(), dynamic_cast<CRpcMessage *>(&rpc_call)->get_value(path), NULL, optGroup);
}

bool EspBaseArrayParam::unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    bool hasValue = false;
    if(attachments)
    {
        StringBuffer path;
        buildVarPath(path, tagname, basepath, NULL, "itemlist", NULL);
        if (params.hasProp(path.str()))
        {
            hasValue = true;
            //sparse array encoding
            char *itemlist = strdup(params.queryProp(path.str()));
            char *delim=NULL;
            if (itemlist)
            {
                for(char *finger=itemlist; finger; finger=(delim) ? delim+1 : NULL)
                {
                    if ((delim=strchr(finger, '+')))
                        *delim=0;
                    if (*finger)
                    {
                        buildVarPath(path, tagname, basepath, finger, NULL, NULL);
                        StringBuffer* data = attachments->getValue(path.str());
                        if (data)
                            append(data->str());
                    }
                }
                free(itemlist);
            }
        }
        else
        {
            buildVarPath(path, tagname, basepath, NULL, "itemcount", NULL);
            int count=params.getPropInt(path.str(), -1);
            if (count>0)
            {
                hasValue = true;
                for (int idx=0; idx<count; idx++)
                {
                    buildVarPath(path, tagname, basepath, NULL, NULL, &idx);
                    StringBuffer* data = attachments->getValue(path.str());
                    if (data)
                        append(data->str());
                }
            }
        }
    }

    if (hasValue && ctx && optGroup)
        ctx->addOptGroup(optGroup);

    return hasValue;
}

bool EspBaseArrayParam::unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix)
{
    if (unmarshallRawArray(params, tagname, basepath))
    {
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
        return true;
    }

    StringBuffer path;
    if (basepath && *basepath)
       path.append(basepath).append(".");
    path.append(tagname);
    const char *pathstr=path.str();

    bool hasValue = false;
    Owned<IPropertyIterator> iter = params.getIterator();

    if (pathstr && *pathstr && iter && iter->first())
    {
        int taglen = strlen(pathstr);
        while (iter->isValid())
        {
            const char *keyname=iter->getPropKey();
            if (strncmp(keyname, pathstr, taglen)==0)
            {
                if (strlen(keyname)==taglen || !strncmp(keyname+taglen, "_rd_", 4))
                {
                    const char *finger = params.queryProp(iter->getPropKey());
                    StringBuffer itemval;
                    while (*finger)
                    {
                        if (*finger=='\r')
                            finger++;

                        if (*finger=='\n')
                        {
                            if (itemval.length())
                                append(itemval.str());
                            itemval.clear();
                        }
                        else
                        {
                            itemval.append(*finger);
                        }
                        finger++;
                    }
                    if (itemval.length())
                    {
                        append(itemval.str());
                        hasValue = true;
                    }
                }
                else if (strncmp(keyname+taglen, "_v", 2)==0)
                {
                    if (params.getPropInt(keyname)) {
                        append(keyname+taglen+2);
                        hasValue = true;
                    }
                }
                else if (strncmp(keyname+taglen, "_i", 2)==0)
                {
                    append(params.queryProp(iter->getPropKey()));
                    hasValue = true;
                }
            }

            iter->next();
        }
    }

    if (hasValue && ctx && optGroup)
        ctx->addOptGroup(optGroup);

    return hasValue;
}

bool EspBaseArrayParam::unmarshallRawArray(IProperties &params, const char *tagname, const char *basepath)
{
    StringBuffer path;
    buildVarPath(path, tagname, basepath, NULL, "itemlist", NULL);
    if (params.hasProp(path.str()))
    {
        //sparse array encoding
        char *itemlist=strdup(params.queryProp(path.str()));
        char *delim=NULL;
        if (itemlist)
        {
            for(char *finger=itemlist; finger; finger=(delim) ? delim+1 : NULL)
            {
                if ((delim=strchr(finger, '+')))
                    *delim=0;
                if (*finger)
                {
                    buildVarPath(path, tagname, basepath, finger, NULL, NULL);
                    append(params.queryProp(path));
                }
            }
            free(itemlist);
            return true;
        }
    }
    else
    {
        buildVarPath(path, tagname, basepath, NULL, "itemcount", NULL);
        int count=params.getPropInt(path.str(), -1);
        if (count>0)
        {
            for (int idx=0; idx<count; idx++)
            {
                buildVarPath(path, tagname, basepath, NULL, NULL, &idx);
                append(params.queryProp(path));
            }
            return true;
        }
    }

    return false;
}
