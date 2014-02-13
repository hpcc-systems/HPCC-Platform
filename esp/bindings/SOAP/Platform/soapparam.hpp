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

#ifndef _SOAP_PARAM_H_
#define _SOAP_PARAM_H_

#include "esphttp.hpp"

inline bool esp_convert(const char* sv, StringAttr& value){value.set(sv); return (!sv);}
inline bool esp_convert(const char* sv, StringBuffer& value){value.clear().append(sv); return (!sv);}
inline bool esp_convert(const char* sv, int& value){value = (sv!=NULL) ? atoi(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, unsigned long& value){value = (sv!=NULL) ? (unsigned long) atol(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, unsigned char& value){value = (sv!=NULL) ? (unsigned char) atol(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, long& value){value = (sv!=NULL) ? atol(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, __int64& value){value = (sv!=NULL) ? _atoi64(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, unsigned int& value){value = (sv!=NULL) ? (unsigned int) atoi(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, unsigned short& value){value = (sv!=NULL) ? (unsigned short) atoi(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, short& value){value = (sv!=NULL) ? (short) atoi(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, double& value){value = (sv!=NULL) ? (double) atof(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, float& value){value = (sv!=NULL) ? (float) atof(sv) : 0; return (!sv);}
inline bool esp_convert(const char* sv, bool& value)
{
    value = streq(sv,"1") || streq(sv,"true") || streq(sv,"on");
    return false;
}

typedef enum nilBehavior_
{
    nilIgnore,
    nilRemove
} nilBehavior;

// workaround for StringBuffer.appendlong()
template <typename type>
inline void appendStringBuffer(StringBuffer& s, type value)
{   s.append(value); }

// specialization
template <>
inline void appendStringBuffer(StringBuffer& s, long value)
{   s.appendlong(value); }

template <>
inline void appendStringBuffer(StringBuffer& s, unsigned long value)
{   s.appendulong(value); }

class ESPHTTP_API BaseEspParam
{
public:
   bool isNil;
   nilBehavior nilBH;
   bool allowHttpNil;
public:
    BaseEspParam(nilBehavior nb, bool httpNil) : isNil(true), nilBH(nb), allowHttpNil(httpNil){}
    virtual ~BaseEspParam(){}

    void Nil(){isNil=true;}
    bool is_nil(){return isNil;}

    void copy(BaseEspParam &from)
    {
        isNil=from.isNil;
        nilBH=from.nilBH;
    }

    virtual bool updateValue(IRpcMessage &rpc_call, const char *path)=0;
    virtual bool updateValue(CSoapValue &soapval, const char *tagname)=0;
    virtual bool updateValue(const char *s)=0;
    virtual void addRpcValue(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix, bool *encodex)=0;

    virtual void toXMLValue(StringBuffer &s, bool encode)=0;
    virtual void toJSONValue(StringBuffer &s, bool encode)=0;
    virtual void toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode);
    virtual void toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname, bool encode);
    void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *prefix="");

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="");

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");
    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL);
    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

template <class cpptype, class inittype=cpptype> class SoapParam : public BaseEspParam
{
private:
   cpptype value;

public:
    SoapParam(nilBehavior nb=nilIgnore, bool httpNil=true) : BaseEspParam(nb, httpNil), value(0){}
    SoapParam(inittype val, nilBehavior nb=nilIgnore, bool httpNil=true) : BaseEspParam(nb, httpNil)
    {
        set(val);
    }

    cpptype getValue() const {return value;}
    operator cpptype () {return value;}
    const cpptype * operator ->() const {return &value;}
    void set(cpptype val) { value = val; isNil=false;};
    void operator=(cpptype val) { set(val); };

    void copy(SoapParam<cpptype, inittype> &from)
    {
        value=from.value;
        BaseEspParam::copy(from);
    }

    virtual void toXMLValue(StringBuffer &s, bool encode)
    {
        appendStringBuffer(s, value);
    }

    virtual void toJSONValue(StringBuffer &s, bool encode)
    {
        if (!isNil)
            appendJSONValue(s, NULL, value);
        else
            appendJSONValue(s, NULL, (const char *)NULL);
    }

    void addRpcValue(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="", bool *encodex=NULL)
    {
        if (!isNil || nilBH==nilIgnore)
            rpc_call.add_value(basepath, prefix, tagname, xsdtype, value);
    }

    virtual bool updateValue(IRpcMessage &rpc_call, const char *path)
    {
        return (rpc_call.get_value(path, value));
    }

    virtual bool updateValue(CSoapValue &soapval, const char *tagname)
    {
        return soapval.get_value(tagname, value);
    }

    virtual bool updateValue(const char *s)
    {
        if (!s || (!*s && allowHttpNil))
            return false;
        return !esp_convert(s, value);
    }
};

template <class cpptype, cpptype initval> class SoapParamEx : public BaseEspParam
{
private:
   cpptype value;

public:
    SoapParamEx(nilBehavior nb=nilIgnore) : BaseEspParam(nilIgnore, false), value(initval){isNil=false;}

    virtual ~SoapParamEx(){}

    const cpptype & getValue() const {return value;}

    operator cpptype () {return value;}
    const cpptype * operator ->() const {return &value;}
    cpptype * operator ->() {return &value;}
    void operator=(cpptype val) { value = val;};

    void copy(SoapParamEx<cpptype, initval> &from)
    {
        value=from.value;
        isNil = false;
    }

    void addRpcValue(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="", bool *encodex=NULL)
    {
        rpc_call.add_value(basepath, prefix, tagname, xsdtype, value);
    }

    virtual void toXMLValue(StringBuffer &s, bool encode)
    {
        s.append(value);
    }

    virtual void toJSONValue(StringBuffer &s, bool encode)
    {
        if (!isNil)
            appendJSONValue(s, NULL, value);
        else
            appendJSONValue(s, NULL, (const char *)NULL);
    }

    virtual bool updateValue(IRpcMessage &rpc_call, const char *path)
    {
        return rpc_call.get_value(path, value);
    }

    virtual bool updateValue(CSoapValue &soapval, const char *tagname)
    {
        return soapval.get_value(tagname, value);
    }

    virtual bool updateValue(const char *s)
    {
        return !esp_convert(s, value);
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

class ESPHTTP_API SoapStringParam : public BaseEspParam
{
private:
    StringBuffer value;
    bool encodeNewlines;

public:
    SoapStringParam(nilBehavior nb=nilIgnore) : BaseEspParam(nb, true), encodeNewlines(false) {}
    SoapStringParam(const char * val, nilBehavior nb=nilIgnore) : BaseEspParam(nb, !val), encodeNewlines(false)
    {
        set(val);
    }

    virtual ~SoapStringParam(){}

    const StringBuffer & getValue() const {return value;}
    bool is_nil(){return isNil;}
    bool getEncodeNewlines(){return encodeNewlines;}

    operator const StringBuffer &() {return value;}
    const StringBuffer * operator ->() const {return &value;}

    StringBuffer &getBuffer(){return value;}

    void copy(SoapStringParam &from)
    {
        value = from.value;
        BaseEspParam::copy(from);
    }

    void operator=(StringBuffer &val) { value = val; isNil=false;};

    void set(const char *val, bool trim=false)
    {
        value.set(val);
        if (trim)
            value.trim();
        isNil = !val;
    }
    const char *query()
    {
        return (isNil && nilBH==nilRemove) ? NULL : value.str();
    }

    void setEncodeNewlines(bool b)
    {
        encodeNewlines = b;
    }

    void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *prefix="", bool encodeJSON=true);

    virtual void toXMLValue(StringBuffer &s, bool encode);
    virtual void toJSONValue(StringBuffer &s, bool encode);

    void addRpcValue(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char *xsdtype, const char *prefix, bool *encodex);
    virtual bool updateValue(IRpcMessage &rpc_call, const char *path);
    virtual bool updateValue(CSoapValue &soapval, const char *tagname);
    virtual bool updateValue(const char *s);

    bool unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");
};

class ESPHTTP_API BaseEspStruct
{
private:
   nilBehavior nilBH;

public:
    BaseEspStruct(nilBehavior nb=nilIgnore) : nilBH(nb){}
    virtual ~BaseEspStruct(){}

    void copy(BaseEspStruct &from)
    {
        nilBH=from.nilBH;
    }

    virtual bool updateValue(IRpcMessage &rpc_call, const char *path) = 0;
    virtual bool updateValue(IEspContext *ctx, CSoapValue &soapval) = 0;
    virtual bool updateValue(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *path) = 0;

    virtual void serializeContent(IEspContext *ctx, StringBuffer &s, IProperties *props=NULL) = 0;
    virtual void serializeAttributes(IEspContext *ctx, StringBuffer &s) = 0;

    virtual void toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode);
    virtual void toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname);
    void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encode=true, const char *xsdtype="", const char *prefix="");

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="");
    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");
    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL);
    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");
};

template <class cpptype, class iftype, class inittype=cpptype> class SoapStruct : public BaseEspStruct
{
private:
   cpptype value;
   nilBehavior nilBH;

private:
    operator cpptype () {return value;}
    void operator=(cpptype val) { value = val;};

public:
    SoapStruct(const char *serviceName, nilBehavior nb=nilIgnore) : value(serviceName), nilBH(nb){}
    virtual ~SoapStruct(){}

    cpptype & getValue() {return value;}

    const cpptype * operator ->() const {return &value;}
    cpptype * operator ->() {return &value;}

    void copy(SoapStruct<cpptype, iftype, inittype> &from)
    {
        value.copy(from.value);
        nilBH=from.nilBH;
    }

    void copy(iftype &ifrom)
    {
        value.copy(ifrom);
    }

    virtual void serializeAttributes(IEspContext *ctx, StringBuffer &xml)
    {
        value.serializeAttributes(ctx, xml);
    }

    virtual void serializeContent(IEspContext *ctx, StringBuffer &xml, IProperties *props=NULL)
    {
        value.serializeContent(ctx, xml, (props) ? &props : NULL);
    }

    virtual bool updateValue(IRpcMessage &rpc_call, const char *path)
    {
        return value.unserialize(rpc_call, NULL, path);
    }

    virtual bool updateValue(IEspContext *ctx, CSoapValue &soapval)
    {
        return value.unserialize(ctx, soapval);
    }

    virtual bool updateValue(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *path)
    {
        return value.unserialize(ctx, params, attachments, path);
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

class ESPHTTP_API SoapParamBinary
{
private:
   MemoryBuffer value;

public:
    SoapParamBinary(nilBehavior nb=nilIgnore){}

    virtual ~SoapParamBinary(){}

    const MemoryBuffer & getValue() const {return value;}

    operator MemoryBuffer& () {return value;}
    const MemoryBuffer * operator ->() const {return &value;}
    MemoryBuffer * operator ->() {return &value;}

    void operator=(MemoryBuffer &val) { value.clear().append(val);};

    void copy(SoapParamBinary &from)
    {
        value.clear().append(from.value);
    }

    virtual void toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode);
    virtual void toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname);
    void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *prefix="");

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="");
    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");
    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL);
    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="");

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

class ESPHTTP_API SoapAttachString
{
private:
   StringBuffer value;

public:
    SoapAttachString(nilBehavior nb=nilIgnore){}

    virtual ~SoapAttachString(){}

    const StringBuffer & getValue() const {return value;}
    const StringBuffer * operator ->() const {return &value;}

    void copy(SoapAttachString &from)
    {
        value.clear().append(from.value);
    }

    void operator=(StringBuffer val){ value.clear().append(val);}

    void set(const char *val){ value.clear().append(val);}
    const char *query(){return value.str();}

    virtual void toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode);
    virtual void toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname);
    void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *prefix="");

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="");
    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL,const char *xsdtype="", const char *prefix="");
    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL);
    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *prefix="");

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

template <class cpptype> class SoapAttachParam
{
private:
   cpptype value;

public:
    SoapAttachParam(nilBehavior nb=nilIgnore){}

    virtual ~SoapAttachParam(){}

    const cpptype & getValue() const {return value;}

    operator cpptype () {return value;}
    const cpptype * operator ->() const {return &value;}
    cpptype * operator ->() {return &value;}
    //Applicable to primitive types, eg. unsigned short, unsigned long, etc.
    void operator=(cpptype val) { value = val;};

    void copy(SoapAttachParam &from)
    {
        value=from.value;
    }

    virtual void toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *prefix, bool encode)
    {
        appendXMLTag(s, tagname, value, prefix, ENCODE_NONE);
    }

    virtual void toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname)
    {
        appendJSONValue(s, tagname, value);
    }

    void toStr(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xsdtype="", const char *prefix="")
    {
        if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
            return toJSON(ctx, s, tagname);
        return toXML(ctx, s, tagname, prefix);
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *prefix="")
    {
        rpc_call.add_value(basepath, prefix, tagname, "Attachment", value);
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="")
    {
        StringBuffer path(basepath);
        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");
        path.append(tagname);

        if (rpc_call.get_value(path.str(), value))
        {
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *prefix="")
    {
        return false;
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

ESPHTTP_API StringBuffer &buildVarPath(StringBuffer &path, const char *tagname, const char *basepath, const char *item, const char *tail, int *idx);

class ESPHTTP_API EspBaseArrayParam
{
protected:
    nilBehavior nilBH;

public:
    EspBaseArrayParam(nilBehavior nb) : nilBH(nb) { }
    virtual ~EspBaseArrayParam() { }

    virtual unsigned getLength()=0;
    virtual const char *getItemTag(const char *in)=0;

    virtual void append(IEspContext *ctx, CSoapValue& sv)=0;
    //virtual bool append(IRpcMessage &rpc_call, const char *path)=0;
    virtual void append(const char *s)=0;

    virtual void toXML(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *itemname, const char *prefix, bool encode);
    virtual void toJSON(IEspContext* ctx, StringBuffer &s, const char *tagname, const char *itemname);
    virtual void toStr(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *itemname, const char *elementtype="", const char *basepath="", const char *prefix="");
    virtual void toStrItem(IEspContext *ctx, StringBuffer &s, unsigned index, const char *name)=0;

    virtual bool unmarshallItems(IEspContext* ctx, CSoapValue *sv, const char *itemname, const char* optGroup=NULL);
    virtual bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup, const char *itemname="Item");
    virtual bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup, const char *prefix);
    virtual bool unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix);
    virtual bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *prefix);
    virtual bool unmarshallRawArray(IProperties &params, const char *tagname, const char *basepath);
};

// generic version: works for string or other types, such as string enum
template <class arraytype>
inline void append_to_array(arraytype& array, const char* item) {  array.append(item); }

// specialization
template <> inline void append_to_array<IntArray>(IntArray& array, const char* item) { array.append(atoi(item)); }

template <> inline void append_to_array<ShortArray>(ShortArray& array, const char* item) { array.append(atoi(item)); }

template <> inline void append_to_array<BoolArray>(BoolArray& array, const char* item) { array.append(streq(item,"1") || streq(item,"true")); }

template <> inline void append_to_array<Int64Array>(Int64Array& array, const char* item) { array.append(atoi64(item)); }

template <> inline void append_to_array<FloatArray>(FloatArray& array, const char* item) { array.append((float)atof(item)); }

template <> inline void append_to_array<DoubleArray>(DoubleArray& array, const char* item) { array.append(atof(item)); }

template <class arraytype, class itemtype>
class SoapArrayParam : public EspBaseArrayParam
{
protected:
    arraytype arr;
    nilBehavior nilBH;

public:
    SoapArrayParam(nilBehavior nb=nilIgnore) : EspBaseArrayParam(nb) { }
    virtual ~SoapArrayParam() { }

    arraytype &getValue() {return arr;}

    operator arraytype () {return  arr;}
    operator arraytype &() {return  arr;}
    arraytype * operator ->() {return &arr;}

    void copy(SoapArrayParam<arraytype,itemtype> &from)
    {
        arr.kill();
        arraytype fromArray = from.getValue();
        ForEachItemIn(idx, fromArray)
          arr.append(fromArray.item(idx));
    }

    virtual unsigned getLength()
    {
        return arr.ordinality();
    }

    virtual const char *getItemTag(const char *in)
    {
        return in;
    }

    virtual void append(IEspContext *ctx, CSoapValue& sv)
    {
        itemtype itemval;
        sv.get_value("", itemval);
        arr.append(itemval);
    }

    virtual void append(const char *s)
    {
        if (s && *s)
            append_to_array(arr, s);
    }

    virtual void toStrItem(IEspContext *ctx, StringBuffer &s, unsigned index, const char *name)
    {
        s.append(arr.item(index));
    }

    inline void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *itemname, const char *elementtype="", const char *basepath="", const char *prefix="")
    {
        EspBaseArrayParam::toStr(ctx, str, tagname, itemname, elementtype, basepath, prefix);
    }

    inline void marshall(IRpcMessage &rpc_call, const char *tagname, const char *itemname, const char *elementtype, const char *basepath, const char *prefix)
    {
        rpc_call.add_value(basepath, prefix, tagname, prefix, itemname, elementtype, arr);
    }

    inline bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *prefix="")
    {  return EspBaseArrayParam::unmarshall(rpc_call,tagname,basepath,optGroup,prefix); }

    inline bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL,const char *xsdtype="", const char *prefix="")
    {  return EspBaseArrayParam::unmarshall(ctx, params, attachments, tagname, basepath, optGroup, xsdtype, prefix); }

    inline bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL, const char *itemname="Item")
    {  return EspBaseArrayParam::unmarshall(ctx, soapval, tagname, optGroup, itemname); }

    void setEncodeNewlines(bool encodenl) { UNIMPLEMENTED; }
};


//--------------------------------------------
// string array

class SoapStringArray : public SoapArrayParam<StringArray, const char*>
{
    typedef SoapArrayParam<StringArray,const char*> BASE;

public:
    SoapStringArray(nilBehavior nb) : SoapArrayParam<StringArray,const char*>(nb) { }

    virtual void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *itemname, const char *elementtype="", const char *basepath="", const char *prefix="")
    {
        BASE::toStr(ctx, str, tagname, itemname, elementtype, basepath, prefix);
    }

    void toStrItemJSON(StringBuffer &s, unsigned index)
    {
        const char *val = arr.item(index);
        if (!val)
            return;
        appendJSONValue(s, NULL, val);
    }

    virtual void toStrItem(IEspContext *ctx, StringBuffer &s, unsigned index, const char *name)
    {
        if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
            toStrItemJSON(s, index);
        else
            appendXMLTag(s, name, arr.item(index));
    }

    virtual void append(IEspContext *ctx, CSoapValue& v)
    {
        StringBuffer s;
        v.get_value("", s);
        append_to_array(arr, s.str());
    }

    virtual void marshall(IRpcMessage &rpc_call, const char *tagname, const char *itemname="Item", const char *elementtype="", const char *basepath="", const char *prefix="")
    {  BASE::marshall(rpc_call, tagname, itemname, elementtype, basepath, prefix); }

    inline bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *prefix="")
    {  return BASE::unmarshall(rpc_call,tagname,basepath,optGroup,prefix); }

    inline bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL,const char *xsdtype="", const char *prefix="")
    {  return BASE::unmarshall(ctx, params, attachments, tagname, basepath, optGroup, xsdtype, prefix); }

    inline bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL, const char *itemname="Item")
    {  return BASE::unmarshall(ctx, soapval, tagname, optGroup, itemname); }
};

//--------------------------------------------
// int array

class SoapIntArray : public SoapArrayParam<IntArray,int>
{
public:
    SoapIntArray(nilBehavior nb) : SoapArrayParam<IntArray,int>(nb) { }
};

//--------------------------------------------
// short array

class SoapShortArray : public SoapArrayParam<ShortArray,int>
{
public:
    SoapShortArray(nilBehavior nb) : SoapArrayParam<ShortArray,int>(nb) { }
};

//--------------------------------------------
// long array

class SoapInt64Array : public SoapArrayParam<Int64Array,__int64>
{
public:
    SoapInt64Array(nilBehavior nb) : SoapArrayParam<Int64Array,__int64>(nb) { }
};

//--------------------------------------------
// Float array

class SoapFloatArray : public SoapArrayParam<FloatArray,float>
{
public:
    SoapFloatArray(nilBehavior nb) : SoapArrayParam<FloatArray,float>(nb) { }
};

//--------------------------------------------
// Double array

class SoapDoubleArray : public SoapArrayParam<DoubleArray,double>
{
public:
    SoapDoubleArray(nilBehavior nb) : SoapArrayParam<DoubleArray,double>(nb) { }
};

//--------------------------------------------
// bool array

class SoapBoolArray : public SoapArrayParam<BoolArray,bool>
{
public:
    SoapBoolArray(nilBehavior nb) : SoapArrayParam<BoolArray,bool>(nb) { }
};

//----------------------------------------------------------
// struct array

template <class basetype, class cltype>
class SoapStructArrayParam : public EspBaseArrayParam
{
public:
    SoapStructArrayParam(nilBehavior nb=nilRemove) : EspBaseArrayParam(nilRemove /*Backward compatabiltiy*/){}
    virtual ~SoapStructArrayParam()
    {
    }

    IArrayOf<basetype>  &getValue(){return arr;}

    operator IArrayOf<basetype>  & () {return  arr;}
    IArrayOf<basetype>  * operator ->() {return &arr;}

    void copy(SoapStructArrayParam<basetype, cltype> &from)
    {
        arr.kill();
        int count= from.arr.ordinality();
        for (int index = 0; index<count; index++)
        {
            from.arr.item(index).Link();
            arr.append(from.arr.item(index));
        }
    }

    virtual void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *itemname, const char *elementtype="Item", const char *basepath="", const char *prefix="")
    {
        return EspBaseArrayParam::toStr(ctx, str, tagname, itemname, elementtype, basepath, prefix);
    }

    virtual void toStrItem(IEspContext *ctx, StringBuffer &str, unsigned index, const char *name)
    {
        basetype &b = arr.item(index);
        cltype *cl = dynamic_cast<cltype *>(&b);
        if (cl)
            cl->serializeItem(ctx, str, name);
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *itemname="", const char *elementtype="", const char *basepath="", const char *prefix="")
    {
        if (!arr.ordinality())
            return;
        StringBuffer s;
        toStr(rpc_call.queryContext(), s, NULL, itemname, elementtype, basepath, prefix);
        rpc_call.add_value(basepath, prefix, tagname, elementtype, s.str(), false);
    }

    virtual unsigned getLength(){return arr.ordinality();}
    virtual const char *getItemTag(const char *in)
    {
        return in;
    }

    virtual void append(const char *){ UNIMPLEMENTED; }

    virtual void append(IEspContext *ctx, CSoapValue& v)
    {
        cltype *cl = new cltype("unknown");
        cl->unserialize(ctx, v);
        arr.append(*static_cast<basetype*>(cl));
    }

    inline bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL, const char *itemname="Item")
    {
        return EspBaseArrayParam::unmarshall(ctx, soapval, tagname, optGroup, itemname);
    }

    inline bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *prefix="")
    {
        return EspBaseArrayParam::unmarshall(rpc_call, tagname, basepath, optGroup, prefix);
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *prefix="")
    {
        StringBuffer path;
        buildVarPath(path, tagname, basepath, cltype::queryXsdElementName(), "itemlist", NULL);
        bool hasValue = false;
        if (params.hasProp(path.str()))
        {
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
                        buildVarPath(path, tagname, basepath, cltype::queryXsdElementName(), finger, NULL);
                        cltype *bt = new cltype("unknown");
                        bt->unserialize(ctx,params, attachments, path.str());
                        arr.append(*static_cast<basetype*>(bt));
                        hasValue = true;
                    }
                }
                free(itemlist);
            }
        }
        else
        {
            buildVarPath(path, tagname, basepath, cltype::queryXsdElementName(), "itemcount", NULL);
            int count=params.getPropInt(path.str(), -1);
            if (count>0)
            {
                for (int idx=0; idx<count; idx++)
                {
                    buildVarPath(path, tagname, basepath, cltype::queryXsdElementName(), NULL, &idx);
                    cltype *bt = new cltype("unknown");
                    bt->unserialize(ctx,params, attachments, path.str());
                    arr.append(*static_cast<basetype*>(bt));
                    hasValue = true;
                }
            }
        }
        return hasValue;
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }

private:
    IArrayOf<basetype> arr;
};

template <class enumtype>
class SoapEnumParamNew
{
private:
   enumtype defvalue;
   enumtype value;
   int count_;
   StringAttr typeName_,baseType_;

   StringArray enumstrings;
   const char* asString() { return toString(value); }

public:
    SoapEnumParamNew(nilBehavior nb=nilIgnore)
    {
        defvalue = value = (enumtype)-1;
        count_ = 0;
    }

    SoapEnumParamNew(enumtype defvalue_)
    {
        defvalue = defvalue_;
        value = (enumtype)-1;
        count_ = 0;
    }

    void setDefaultValue(const char* s)
    {
        if (s)
        {
            int tempval = enumstrings.find(s);

            if (tempval == NotFound)
                throw MakeStringException(-1, "Invalid value for type %s: %s", typeName_.get(), s);
            else
                defvalue = (enumtype)tempval;
        }
    }

    virtual ~SoapEnumParamNew(){}

    enumtype getValue() const { return (value>=0) ? value : defvalue; }
    operator const char* ()   { return asString(); }

    operator enumtype () const {return  getValue();}
    const enumtype * operator ->() const { return (value>=0) ? &value : &defvalue;}
    enumtype * operator ->() {return (value>=0) ? &value : &defvalue;}

    const char* toString(enumtype val)
    {
       if (val<0)
       {
           if (defvalue>=0)
               return enumstrings.item((int)defvalue);
           else
               return "";
       }
       else
           return enumstrings.item((int)val);
    }

    enumtype toEnum(const char* s)
    {
        if (s)
        {
            int tempval = enumstrings.find(s);
            if (tempval != NotFound)
                return (enumtype)tempval;
        }

        throw MakeStringException(-1, "Invalid value for type %s: %s", typeName_.get(), s ? s : "");
    }

    void copy(SoapEnumParamNew<enumtype>  &from)
    {
        value=from.value;
        count_=from.count_;
        enumstrings.kill();
        ForEachItemIn(idx, from.enumstrings)
            enumstrings.append(from.enumstrings.item(idx));
    }

    void setValue(enumtype valuex)
    {
        if (0<=valuex && valuex < count_)
            value = valuex;
        else if (valuex == -1)
            value = defvalue;
        else
            throw MakeStringException(-1, "Invalid value for type %s: %d", typeName_.get(), valuex);
    }

    void setValue(const char* s)
    {
        if (s)
        {
            int tempval = enumstrings.find(s);
            if (tempval == NotFound)
                throw MakeStringException(-1, "Invalid value for type %s: %s", typeName_.get(), s);
            else
                value = (enumtype)tempval;
        }
    }

    void init(const char* typeName,const char* baseType,const char **strings)
    {
        typeName_.set(typeName);
        baseType_.set(baseType);
        value = defvalue;
        count_=0;

        if (strings!=NULL)
        {
            while(strings[count_]!=NULL)
            {
                enumstrings.append(strings[count_]);
                count_++;
            }
        }
    }

    void toStr(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true,const char *xsdtype="", const char *prefix="")
    {
        if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
            appendJSONValue(str, tagname, asString());
        else
            appendXMLTag(str, tagname, asString(), prefix);
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char* itemname, const char* elementtype="",const char *basepath="", const char *prefix="")
    {
        rpc_call.add_value(basepath, prefix, tagname, "string", enumstrings.item(value));
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *prefix="")
    {
        StringBuffer path(basepath);
        StringAttr strvalue;

        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");

        path.append(tagname);

        if (rpc_call.get_value(path.str(), strvalue)) {
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            setValue(strvalue);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        StringAttr v;
        if (soapval.get_value(tagname,v)) {
            setValue(v);
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);

        StringAttr strvalue;
        if (!esp_convert(params.queryProp(path.str()), strvalue)) {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            setValue(strvalue);
            return true;
        }
        return false;
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }

    void getXsdDefinition_(IEspContext &context, IHttpMessage *request, StringBuffer &schema, BoolHash &added, const char* descriptions[])
    {
        if (added.getValue(typeName_))
            return;

        added.setValue(typeName_, 1);

        schema.appendf("<xsd:simpleType name=\"%s\">", typeName_.get());

        if ( descriptions && ((context.queryOptions()&ESPCTX_NO_ANNOTATION) == 0) )
        {
            schema.append("<xsd:annotation><xsd:appinfo>");
            for (unsigned i=0; i<enumstrings.length(); i++)
            {
                const char* desc = descriptions[i];
                if (desc && *desc)
                    schema.appendf("<item name=\"%s\" description=\"%s\"/>", enumstrings.item(i), desc);
            }
            schema.append("</xsd:appinfo></xsd:annotation>");
        }

        schema.appendf("<xsd:restriction base=\"xsd:%s\">", baseType_.get());
        for (unsigned i=0; i<enumstrings.length(); i++)
            schema.appendf("<xsd:enumeration value=\"%s\"/>", enumstrings.item(i));
        schema.append("</xsd:restriction></xsd:simpleType>");
    }
};

template <class enumtype, class cxtype, class arraytype>
class SoapEnumArrayParam : public EspBaseArrayParam
{
public:
    SoapEnumArrayParam(nilBehavior nb=nilIgnore){}
    virtual ~SoapEnumArrayParam()
    {
    }

    arraytype  &getValue(){return arr;}
    operator arraytype  & () {return  arr;}
    arraytype  * operator ->() {return &arr;}

    virtual const char *getItemTag(const char *in)
    {
        return (in && *in) ? in : cxtype::queryXsdElementName();
    }

    virtual unsigned getLength(){return arr.ordinality();}

    virtual void append(IEspContext *ctx, CSoapValue& sv)
    {
        StringBuffer s;
        sv.serializeContent(s, NULL);
        arr.append(cxtype::enumOf(s));
    }
    //virtual bool append(IRpcMessage &rpc_call, const char *path)=0;
    virtual void append(const char *s)
    {
        arr.append(cxtype::enumOf(s));
    }

    void copy(SoapEnumArrayParam<enumtype, cxtype, arraytype> &from)
    {
        arr.kill();
        int count= from.arr.ordinality();
        for (int index = 0; index<count; index++)
            arr.append(from.arr.item(index));
    }

    virtual void toStrItem(IEspContext *ctx, StringBuffer &s, unsigned index, const char *name)
    {
        if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
            appendJSONValue(s, name, cxtype::stringOf(arr.item(index)));
        else
            appendXMLTag(s, name, cxtype::stringOf(arr.item(index)));
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *itemname="", const char *elementtype="", const char *basepath="", const char *prefix="")
    {
        int count= arr.ordinality();
        if (count > 0)
        {
            const char* itemTag = (!itemname || !*itemname) ? cxtype::queryXsdElementName() : itemname;

            StringBuffer xml;
            for (int index = 0; index<count; index++)
            {
                enumtype e = arr.item(index);
                xml.appendf("<%s>", itemTag);
                encodeUtf8XML(cxtype::stringOf(e),xml);
                xml.appendf("</%s>", itemTag);
            }
            rpc_call.add_value(basepath, prefix, tagname, elementtype, xml.str(), false);
        }
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *prefix="")
    {
        StringBuffer path;
        buildVarPath(path, tagname, basepath, cxtype::queryXsdElementName(), "itemlist", NULL);
        bool hasValue = false;
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
                        arr.append(cxtype::enumOf(finger));
                        hasValue = true;
                    }
                }
                free(itemlist);
            }
        }
        else
        {
            buildVarPath(path, tagname, basepath, cxtype::queryXsdElementName(), "itemcount", NULL);
            int count=params.getPropInt(path.str(), -1);
            if (count>0)
            {
                for (int idx=0; idx<count; idx++)
                {
                    buildVarPath(path, tagname, basepath, cxtype::queryXsdElementName(), NULL, &idx);
                    const char* val = params.queryProp(path);
                    if (val) {
                        arr.append(cxtype::enumOf(val));
                        hasValue = true;
                    }
                }
            }
        }

        if (hasValue && ctx && optGroup)
            ctx->addOptGroup(optGroup);

        return hasValue;
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }

private:
    arraytype arr;
};


#endif //_SOAP_PARAM_H_
