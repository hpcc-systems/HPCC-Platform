/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _SOAP_PARAM_H_
#define _SOAP_PARAM_H_


//return whether nil, but only use the return in the case of http url parameters (bool for one nil means false)

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
    //true if a property is sent in, but is not one of {0, false, off}
    //value=(sv!=NULL && stricmp(sv, "0") && stricmp(sv, "false") && stricmp(sv, "off"));

    //true if it is 1, true or on. The default should be false (e.g., for bad input)
    value = streq(sv,"1") || streq(sv,"true") || streq(sv,"on");
    return false;
}


typedef enum nilBehavior_
{
    nilIgnore,
    nilRemove
} nilBehavior;

inline void append_start_tag(StringBuffer &str, const char *tagname, const char *xmlns, bool close, IProperties *props)
{
    str.append('<');
    if (xmlns && *xmlns)
        str.append(xmlns).append(':');
    str.append(tagname);

    if (props)
    {
        Owned<IPropertyIterator> piter = props->getIterator();
        for (piter->first(); piter->isValid(); piter->next())
        {
            const char *keyname=piter->getPropKey();
            const char* val = props->queryProp(keyname);
            if (val && *val)
            {
                StringBuffer encoded;
                encodeXML(val,encoded);
                str.appendf(" %s=\"%s\"", keyname, encoded.str());
            }
        }
    }
    
    if (close)
        str.append('/');
    str.append('>');
}


inline void append_end_tag(StringBuffer &str, const char *tagname, const char *xmlns)
{
    str.append("</");
    if (xmlns && *xmlns)
        str.append(xmlns).append(':');
    str.append(tagname).append('>');
}

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

//Applicable to primitive types, eg. unsigned short, unsigned long, etc.
template <class cpptype, class inittype=cpptype> class SoapParam
{
private:
   cpptype value_;
   bool isNil;
   nilBehavior nilBH;
   bool allowHttpNil;

public:
    SoapParam(nilBehavior nb=nilIgnore, bool httpNil=true) : isNil(true), value_(0), nilBH(nb), allowHttpNil(httpNil){}

    SoapParam(inittype val, nilBehavior nb=nilIgnore, bool httpNil=true) : value_(val), isNil(false), nilBH(nb), allowHttpNil(httpNil){}

    virtual ~SoapParam(){}

    cpptype getValue() const {return value_;}
    operator cpptype () {return value_;}

    const cpptype * operator ->() const {return &value_;}

    void operator=(cpptype val) { value_ = val; isNil=false;};
    void Nil(){isNil=true;}
    bool is_nil(){return isNil;}

    void copy(SoapParam<cpptype, inittype> &from)
    {
        value_=from.value_;
        isNil=from.isNil;
        nilBH=from.nilBH;
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        if (!isNil || nilBH==nilIgnore)
            rpc_call.add_value(basepath, xmlns, tagname, xsdtype, value_);
    }

    void marshall(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *xmlns="")
    {
        if (!isNil || nilBH==nilIgnore)
        {
            StringBuffer temp;
            appendStringBuffer(temp,value_);
            append_start_tag(str, tagname, xmlns, false, NULL);
            if (encodeXml)
                encodeUtf8XML(temp.str(), str);
            else
                str.append(temp);
            append_end_tag(str, tagname, xmlns);
        }
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path(basepath);

        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");

        path.append(tagname);

        if (rpc_call.get_value(path.str(), value_)) {
            if (optGroup && rpc_call.queryContext()) 
                rpc_call.queryContext()->addOptGroup(optGroup);
            isNil = false;
        }
        return isNil;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        if (soapval.get_value(tagname, value_)) {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            isNil = false;
        }
        return isNil;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);

        const char *pval=params.queryProp(path.str());
        if (pval && (*pval || !allowHttpNil))
        {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            isNil = false;
            esp_convert(pval, value_);      
        }
        return isNil;
    }   

    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

class SoapStringParam
{
private:
    StringBuffer value_;
    bool isNil;
    nilBehavior nilBH;
    bool encodeNewlines;

public:
    SoapStringParam(nilBehavior nb=nilIgnore) : isNil(true), nilBH(nb), encodeNewlines(false) {}

    SoapStringParam(const char * val, nilBehavior nb=nilIgnore) : value_(val), isNil(val==NULL), nilBH(nb), encodeNewlines(false) {}

    virtual ~SoapStringParam(){}

    const StringBuffer & getValue() const {return value_;}
    bool is_nil(){return isNil;}

    operator const StringBuffer &() {return value_;}
    const StringBuffer * operator ->() const {return &value_;}

    StringBuffer &getBuffer(){return value_;}

    void copy(SoapStringParam &from)
    {
        value_.clear().append(from.value_);
        isNil=from.isNil;
        nilBH=from.nilBH;
    }
    void operator=(StringBuffer &val) 
    { 
        value_.clear().append(val); 
        isNil=false;
    };

    void set(const char *value, bool trim=false)
    {
        value_.clear().append(value); 
        if (trim)
            value_.trim();
        isNil=(value==NULL);
    }
    const char *query()
    {
        return (isNil && nilBH==nilRemove) ? NULL : value_.str();
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="", bool *encodex=NULL)
    {
        if (!isNil || nilBH==nilIgnore)
            rpc_call.add_value(basepath, xmlns, tagname, xsdtype, value_.str(),(!encodex) ? rpc_call.getEncodeXml() : *encodex);
    }

    void marshall(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *xmlns="")
    {
        if (nilBH==nilIgnore || !isNil)
        {
            append_start_tag(str, tagname, xmlns, false, NULL);
            if (encodeXml)
                encodeUtf8XML(value_.str(), str, encodeNewlines?ENCODE_NEWLINES:0);
            else
                str.append(value_);
            append_end_tag(str, tagname, xmlns);
        }
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path(basepath);

        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");

        path.append(tagname);

        StringBuffer tmp;
        if (rpc_call.get_value(path.str(), tmp))
        {
            isNil = false;
            value_.set(tmp);
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        isNil = !soapval.get_value_str(tagname, value_.clear());
        if (!isNil && ctx && optGroup) ctx->addOptGroup(optGroup);
        return !isNil;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);
        const char *pval = params.queryProp(path.str());
        if (pval)
        {
            isNil=false;
            esp_convert(pval, value_.clear());
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
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
                value_.clear().swapWith(*data);
                if (ctx && optGroup) 
                    ctx->addOptGroup(optGroup);
                return true;
            }
        }
        return false;
    }

    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        encodeNewlines = encodenl;
    }
};


template <class cpptype, class iftype, class inittype=cpptype> class SoapStruct
{
private:
   cpptype value_;
   nilBehavior nilBH;

private:
    operator cpptype () {return value_;}
    void operator=(cpptype val) { value_ = val;};

public:
    SoapStruct(const char *serviceName, nilBehavior nb=nilIgnore) : value_(serviceName), nilBH(nb){}
    virtual ~SoapStruct(){}

    cpptype & getValue() {return value_;}

    const cpptype * operator ->() const {return &value_;}
    cpptype * operator ->() {return &value_;}

    void copy(SoapStruct<cpptype, iftype, inittype> &from)
    {
        value_.copy(from.value_);
        nilBH=from.nilBH;
    }
    void copy(iftype &ifrom)
    {
        value_.copy(ifrom);
    }


    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        IProperties *props=NULL;
        StringBuffer xml;
        value_.serializeContent(rpc_call.queryContext(),xml, &props);
        //value_.serialize(*rpc_call.queryContext(),xml,"");
        //inittype::serializer(*rpc_call.queryContext(),value_,xml,false);
        if (xml.length() || nilBH==nilIgnore || props)
            rpc_call.add_value(basepath, xmlns, tagname, xsdtype, xml.str(), false);

        if (props)
            rpc_call.add_attr(basepath, tagname, NULL, *props);
        if (props)
            props->Release();
    }

    void marshall(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *basepath="", const char *xsdtype="",  bool encodeXml=true, const char *xmlns="")
    {
        StringBuffer xml;
        IProperties *props=NULL;
        value_.serializeContent(ctx,xml,&props);
        //value_.serialize(ctx,xml,"");     
        //inittype::serializer(ctx,value_,xml,false);
        if (props || xml.length() || nilBH==nilIgnore)
            append_start_tag(str, tagname, xmlns, (xml.length()==0), props);
        if (xml.length())
        {
            str.append(xml.str());
            append_end_tag(str, tagname, xmlns);
        }
        if (props)
            props->Release();
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
        {
            path.append(basepath);
            if (path.charAt(path.length()-1)!='/')
                path.append("/");
        }
        path.append(tagname);
        if (value_.unserialize(rpc_call, NULL, path.str())) {
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        CSoapValue *sv = soapval.get_value(tagname);
        if (sv) {
            value_.unserialize(ctx,*sv);
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
       StringBuffer path;
       if (basepath && *basepath)
           path.append(basepath).append(".");
       path.append(tagname);
   
       if (value_.unserialize(ctx, params, attachments, path.str())) {
           if (ctx && optGroup)
               ctx->addOptGroup(optGroup);
           return true;
       }
       return false;
    }
    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

template <class cpptype, cpptype initval> class SoapParamEx
{
private:
   cpptype value_;

public:
    SoapParamEx(nilBehavior nb=nilIgnore) : value_(initval){}

    virtual ~SoapParamEx(){}

    const cpptype & getValue() const {return value_;}

    operator cpptype () {return value_;}
    const cpptype * operator ->() const {return &value_;}
    cpptype * operator ->() {return &value_;}
    //Applicable to primitive types, eg. unsigned short, unsigned long, etc.
    void operator=(cpptype val) { value_ = val;};

    void copy(SoapParamEx<cpptype, initval> &from)
    {
        value_=from.value_;
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
       rpc_call.add_value(basepath, xmlns, tagname, xsdtype, value_);
    }

    void marshall(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *xmlns="")
    {
        append_start_tag(str, tagname, xmlns, false, NULL);
        str.append(value_);
        append_end_tag(str, tagname, xmlns);
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns="")
    {
      StringBuffer path(basepath);

      if (basepath!=NULL && basepath[0]!=0)
         path.append("/");
  
      path.append(tagname);
  
      return rpc_call.get_value(path.str(), value_);
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        if (soapval.get_value(tagname, value_)) {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            return true;
        }
        return false;
    }


    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);
        return esp_convert(params.queryProp(path.str()), value_);
    }

    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};


class SoapParamBinary
{
private:
   MemoryBuffer value_;

public:
    SoapParamBinary(nilBehavior nb=nilIgnore){}

    virtual ~SoapParamBinary(){}

    const MemoryBuffer & getValue() const {return value_;}

    operator MemoryBuffer& () {return value_;}
    const MemoryBuffer * operator ->() const {return &value_;}
    MemoryBuffer * operator ->() {return &value_;}

    void operator=(MemoryBuffer &val) { value_.clear().append(val);};

    void copy(SoapParamBinary &from)
    {
        value_.clear().append(from.value_);
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer b64value;
        JBASE64_Encode(value_.toByteArray(), value_.length(), b64value);
        rpc_call.add_value(basepath, xmlns, tagname, xsdtype, b64value);
    }

    void marshall(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *xmlns="")
    {
        append_start_tag(str, tagname, xmlns, false, NULL);
        JBASE64_Encode(value_.toByteArray(), value_.length(), str);
        append_end_tag(str, tagname, xmlns);
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path(basepath);

        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");
        path.append(tagname);

        StringBuffer b64value;
        bool ret = rpc_call.get_value(path.str(), b64value);
        if (ret && optGroup && rpc_call.queryContext())
            rpc_call.queryContext()->addOptGroup(optGroup);

        if(b64value.length() > 0) 
            JBASE64_Decode(b64value.str(), value_);

        return ret;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        //TODO: is this OK
        //assertex(false);
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);

        const char* val = params.queryProp(path.str());
        if(val) {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            JBASE64_Decode(params.queryProp(path.str()), value_);
            return true;
        }
        return false;
    }
    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

template <class cpptype> class SoapAttachParam
{
private:
   cpptype value_;

public:
    SoapAttachParam(nilBehavior nb=nilIgnore){}

    virtual ~SoapAttachParam(){}

    const cpptype & getValue() const {return value_;}

    operator cpptype () {return value_;}
    const cpptype * operator ->() const {return &value_;}
    cpptype * operator ->() {return &value_;}
    //Applicable to primitive types, eg. unsigned short, unsigned long, etc.
    void operator=(cpptype val) { value_ = val;};

    void copy(SoapAttachParam &from)
    {
        value_=from.value_;
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        rpc_call.add_value(basepath, xmlns, tagname, "Attachment", value_);
    }

    void marshall(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="",const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        append_start_tag(str, tagname, xmlns, false, NULL);
        str.append(value_);
        append_end_tag(str, tagname, xmlns);
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path(basepath);

        if (basepath!=NULL && basepath[0]!=0)
         path.append("/");

        path.append(tagname);

        if (rpc_call.get_value(path.str(), value_)) {
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        //TODO: is this ok
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns="")
    {
        //TODO: is this ok
        return false;
    }

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};


class SoapAttachString
{
private:
   StringBuffer value_;

public:
    SoapAttachString(nilBehavior nb=nilIgnore){}

    virtual ~SoapAttachString(){}

    const StringBuffer & getValue() const {return value_;}
    const StringBuffer * operator ->() const {return &value_;}

    void copy(SoapAttachString &from)
    {
        value_.clear().append(from.value_);
    }

    void operator=(StringBuffer val){ value_.clear().append(val);}

    void set(const char *val){ value_.clear().append(val);}
    const char *query(){return value_.str();}

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        rpc_call.add_value(basepath, xmlns, tagname, "Attachment", value_);
    }

    void marshall(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true, const char *xsdtype="", const char *xmlns="")
    {
        append_start_tag(str, tagname, xmlns, false, NULL);
        str.append(value_);
        append_end_tag(str, tagname, xmlns);
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL,const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path(basepath);

        if (basepath!=NULL && basepath[0]!=0)
         path.append("/");

        path.append(tagname);

        if (rpc_call.get_value(path.str(), value_)) {
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            return true;
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL) { 
        //TODO: what to do with this
        return false;
    }
    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){
        //TODO: what to do with this
        return false;
    }
    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

inline StringBuffer &buildVarPath(StringBuffer &path, const char *tagname, const char *basepath, const char *item, const char *tail, int *idx)
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

//--------------------------------------------
// soap array template

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
class SoapArrayParam
{
protected:
    arraytype array_;
    nilBehavior nilBH;

public:
    SoapArrayParam(nilBehavior nb=nilIgnore) : nilBH(nb) { }
    virtual ~SoapArrayParam() { }

    arraytype &getValue() {return array_;}

    operator arraytype () {return  array_;}
//  operator const arraytype &() const {return  array_;}
    operator arraytype &() {return  array_;}
    arraytype * operator ->() {return &array_;}

    void copy(SoapArrayParam<arraytype,itemtype> &from)
    {
        array_.kill();
        arraytype fromArray = from.getValue();
        ForEachItemIn(idx, fromArray)
          array_.append(fromArray.item(idx));
    }

    // marshall
    virtual void marshall(IRpcMessage &rpc_call, const char *tagname, const char *elementname="Item", const char *elementtype="", const char *basepath="", const char *xmlns="");
    virtual void marshall(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *elementname="Item", const char *elementtype="", const char *basepath="", const char *xmlns="");

    // unmarshall
    virtual bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL, const char *elementname="Item");
    virtual bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xmlns="");
    virtual bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="");
    virtual bool unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="");

    bool unmarshallRawArray(IProperties &params, const char *tagname, const char *basepath);

    void setEncodeNewlines(bool encodenl) { UNIMPLEMENTED; }
};

template <class arraytype, class itemtype> 
inline void SoapArrayParam<arraytype,itemtype>::marshall(IRpcMessage &rpc_call, const char *tagname, const char *elementname, const char *elementtype, const char *basepath, const char *xmlns)
{
    rpc_call.add_value(basepath, xmlns, tagname, xmlns, elementname, elementtype, array_);
}

template <class arraytype, class itemtype> 
inline void SoapArrayParam<arraytype,itemtype>::marshall(IEspContext* ctx,StringBuffer &str, const char *tagname, const char *elementname, const char *elementtype, const char *basepath, const char *xmlns)
{
    const unsigned nItems = array_.ordinality();
    if (nItems == 0) 
    {
        if (nilBH != nilRemove)
            append_start_tag(str, tagname, xmlns, true, NULL);
    }
    else
    {
        append_start_tag(str, tagname, xmlns, false, NULL);
        for (unsigned  i=0; i<nItems; i++)
        {
            itemtype val = array_.item(i);
            append_start_tag(str, elementname, xmlns, false, NULL);
            str.append(val);
            append_end_tag(str, elementname, xmlns);
        }
        append_end_tag(str, tagname, xmlns);
    }
}

template <class arraytype, class itemtype> 
inline bool SoapArrayParam<arraytype,itemtype>::unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup, const char *elementname)
{
    CSoapValue *sv= soapval.get_value(tagname);
    if (sv)
    {
        SoapValueArray* children = sv->query_children();        
        if (children)
        {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);     
            itemtype itemval;
            ForEachItemIn(x, *children)
            {
                CSoapValue& onechild = children->item(x);
                onechild.get_value("",itemval);
                array_.append(itemval);
            }               
            return true;
        }
    }
    return false;
}



template <class arraytype, class itemtype> 
inline bool SoapArrayParam<arraytype,itemtype>::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, const char* optGroup, const char *xmlns)
{
    StringBuffer path(basepath);

    if (basepath!=NULL && basepath[0]!=0)
        path.append("/");

    path.append(tagname);

    if (rpc_call.get_value(path.str(), array_))
    {
        if (optGroup && rpc_call.queryContext())
            rpc_call.queryContext()->addOptGroup(optGroup);
        return true;
    }

    return false;
}

template <class arraytype, class itemtype> 
inline bool SoapArrayParam<arraytype,itemtype>::unmarshallAttach(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *xmlns)
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
                        StringBuffer* data = attachments->getValue(path.str());
                        if (data) 
                            append_to_array(array_,data->str());
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
                        append_to_array(array_,data->str());
                }
            }
        }
    }

    if (hasValue && ctx && optGroup)
        ctx->addOptGroup(optGroup);

    return hasValue;
}

template <class arraytype, class itemtype> 
inline bool SoapArrayParam<arraytype,itemtype>::unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char* optGroup, const char *xsdtype, const char *xmlns)
{
    if (unmarshallRawArray(params, tagname, basepath)) {
        if (ctx && optGroup)
            ctx->addOptGroup(optGroup);
        return true;
    }

    //supported encodings
    //property = value
    //--------   ---------
    //tagname_vb_value = boolean
    //tagname_iv_index = value

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
                                append_to_array(array_,itemval.str());
                            itemval.clear();
                        }
                        else
                        {
                            itemval.append(*finger);
                        }
                        finger++;
                    }
                    if (itemval.length()) {
                        append_to_array(array_,itemval.str());
                        hasValue = true;
                    }
                }
                else if (strncmp(keyname+taglen, "_v", 2)==0)
                {
                    if (params.getPropInt(keyname)) {
                        append_to_array(array_,keyname+taglen+2);
                        hasValue = true;
                    }
                }
                else if (strncmp(keyname+taglen, "_i", 2)==0)
                {
                    //array_.add(name , val ); 
                    append_to_array(array_,params.queryProp(iter->getPropKey()));
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

//void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
//void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
template <class arraytype, class itemtype> 
inline bool SoapArrayParam<arraytype,itemtype>::unmarshallRawArray(IProperties &params, const char *tagname, const char *basepath)
{
    bool rt = false;

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
                    const char* val = params.queryProp(path);
                    if (val)
                        append_to_array(array_,val);
                }
            }
            free(itemlist);

            rt = true;
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
                const char* val = params.queryProp(path);
                if (val)
                    append_to_array(array_,val);
            }
            rt = true;
        }
    }

    return rt;
}

//--------------------------------------------
// string array

class SoapStringArray : public SoapArrayParam<StringArray,const char*>
{
    typedef SoapArrayParam<StringArray,const char*> BASE;
public:
    SoapStringArray(nilBehavior nb) : SoapArrayParam<StringArray,const char*>(nb) { }

    virtual void marshall(IRpcMessage &rpc_call, const char *tagname, const char *elementname="Item", const char *elementtype="", const char *basepath="", const char *xmlns="")
    { BASE::marshall(rpc_call,tagname,elementname,basepath,xmlns); }
    
    virtual void marshall(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *elementname="Item", const char *elementtype="", const char *basepath="", const char *xmlns="")
    {
        const unsigned nItems = array_.ordinality();
        if (nItems == 0) 
        {
            if (nilBH != nilRemove)
                append_start_tag(str, tagname, xmlns, true, NULL);
        }
        else
        {
            append_start_tag(str, tagname, xmlns, false, NULL);
            for (unsigned  i=0; i<nItems; i++)
            {
                const char* val = array_.item(i);               
                append_start_tag(str, elementname, xmlns, false, NULL);
                encodeUtf8XML(val,str); // string specific
                append_end_tag(str, elementname, xmlns);
            }
            append_end_tag(str, tagname, xmlns);
        }
    }

    virtual bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char *elementname="Item")
    {
        CSoapValue *sv= soapval.get_value(tagname);
        if (sv)
        {
            SoapValueArray* children = sv->query_children();
            
            if (children)
            {
                StringBuffer itemval;
                ForEachItemIn(x, *children)
                {
                    CSoapValue& onechild = children->item(x);
                    onechild.get_value_str("",itemval.clear());
                    append_to_array(array_,itemval.str());
                }
                return true;
            }
        }
        return false;
    }

    virtual bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xmlns="")
    {  return BASE::unmarshall(rpc_call,tagname,basepath,optGroup,xmlns); }

    virtual bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL,const char *xsdtype="", const char *xmlns="")
    {  return BASE::unmarshall(ctx,params, attachments,tagname,basepath,optGroup,xmlns); }
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
class SoapStructArrayParam
{
public:
    SoapStructArrayParam(nilBehavior nb=nilIgnore){}
    virtual ~SoapStructArrayParam()
    {
    }

    IArrayOf<basetype>  &getValue(){return array_;}

    operator IArrayOf<basetype>  & () {return  array_;}
    IArrayOf<basetype>  * operator ->() {return &array_;}

    void copy(SoapStructArrayParam<basetype, cltype> &from)
    {
        array_.kill();
        int count= from.array_.ordinality();
        for (int index = 0; index<count; index++)
        {
            from.array_.item(index).Link();
            array_.append(from.array_.item(index));
        }
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *elementname="", const char *elementtype="", const char *basepath="", const char *xmlns="")
    {
        int count= array_.ordinality();
        if (count > 0)
        {
            StringBuffer xml;
            IEspContext* ctx = rpc_call.queryContext();
            for (int index = 0; index<count; index++)
            {
                basetype &ifc = array_.item(index);
                cltype *cl = dynamic_cast<cltype *>(&ifc);

                if (cl)
                    cl->serialize(ctx,xml, elementname);
                else
                    //cltype::serializer(ifc, xml, elementname);
                    cltype::serializer(ctx,ifc, xml);
            }

            rpc_call.add_value(basepath, xmlns, tagname, elementtype, xml.str(), false);
        }
    }

    void marshall(IEspContext* ctx,StringBuffer &xml, const char *tagname, const char *elementname=NULL, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        int count= array_.ordinality();
        if (count > 0)
        {
            xml.appendf("<%s>", tagname);
            for (int index = 0; index<count; index++)
            {
                basetype &ifc = array_.item(index);
                cltype *cl = dynamic_cast<cltype *>(&ifc);

                if (cl)
                    cl->serialize(ctx,xml, elementname);
                else
                    //cltype::serializer(ifc, xml, elementname);
                    cltype::serializer(ctx,ifc, xml);
            }
            xml.appendf("</%s>", tagname);
        }
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        CSoapValue *sv= soapval.get_value(tagname);
        if (sv)
        {
            SoapValueArray* children = sv->query_children();
            if (children)
            {
                if (ctx && optGroup)
                    ctx->addOptGroup(optGroup);
                ForEachItemIn(x, *children)
                {
                    CSoapValue& onechild = children->item(x);
                    cltype *bt = new cltype("unknown");
                    bt->unserialize(ctx,onechild);
                    array_.append(*static_cast<basetype*>(bt));
                }
                return true;
            }
        }
        return false;
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xmlns="")
    {
        //MTimeSection timing(NULL, "SoapStructureArray Unmarshalling");
        CRpcMessage *rpcmsg=dynamic_cast<CRpcMessage *>(&rpc_call);
        if (rpcmsg)
        {
            StringBuffer path(basepath);
            if (basepath!=NULL && basepath[0]!=0)
                path.append("/");
            path.append(tagname);
            CSoapValue *soapval=rpcmsg->get_value(path.str());
            if (soapval) {
                if (optGroup && rpc_call.queryContext())
                    rpc_call.queryContext()->addOptGroup(optGroup);
                return unmarshall(rpc_call.queryContext(),*soapval, NULL);
            }
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        buildVarPath(path, tagname, basepath, cltype::queryXsdElementName(), "itemlist", NULL);
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
                        buildVarPath(path, tagname, basepath, cltype::queryXsdElementName(), finger, NULL);
                        cltype *bt = new cltype("unknown");
                        bt->unserialize(ctx,params, attachments, path.str());
                        array_.append(*static_cast<basetype*>(bt));
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
                    array_.append(*static_cast<basetype*>(bt));
                    hasValue = true;
                }
            }
        }
        return hasValue;
    }

    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }

private:
    IArrayOf<basetype> array_;
};

template <class enumtype, enumtype defvalue> 
class SoapEnumParam
{
private:
   enumtype value_;
   int count_;

   StringArray enumstrings;

public:
    SoapEnumParam(const char **strings, nilBehavior nb=nilIgnore)
    {
        setEnumStrings(strings);
    }

    SoapEnumParam(nilBehavior nb=nilIgnore)
    {
        value_ = defvalue;
        count_=0;
    }

    virtual ~SoapEnumParam(){}
    enumtype getValue() const {return value_;}
    operator enumtype () const {return  value_;}
    const enumtype * operator ->() const {return &value_;}
    enumtype * operator ->() {return &value_;}

    void copy(SoapEnumParam<enumtype, defvalue>  &from)
    {
        value_=from.value_;
        count_=from.count_;
        enumstrings.kill();
        ForEachItemIn(idx, from.enumstrings)
            enumstrings.append(from.enumstrings.item(idx));
    }

    const enumtype & operator =(enumtype &value) {value_=value; return value_;}

    void setValue(enumtype valuex)
    {
        if (valuex >= count_)
            value_ = defvalue;
        else
            value_ = valuex;
    }

    void setEnumStrings(const char **strings)
    {
        value_ = defvalue;
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

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char *xmlns="")
    {
        rpc_call.add_value(basepath, xmlns, tagname, "string", enumstrings.item(value_));
    }

    void marshall(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xmlns="")
    {
        StringBuffer path(basepath);
        StringAttr strvalue;

        if (basepath!=NULL && basepath[0]!=0)
            path.append("/");

        path.append(tagname);

        rpc_call.get_value(path.str(), strvalue);

        int tempval = defvalue;

        if (strvalue.length())
            tempval = enumstrings.find(strvalue.get());

        if (tempval != NotFound) {
            if (optGroup && rpc_call.queryContext())
                rpc_call.queryContext()->addOptGroup(optGroup);
            value_ = (enumtype) tempval;
            return true;
        }
        else {
            value_ = defvalue;
            return false;
        }
    }

    bool unmarshall(CSoapValue &soapval, const char *tagname, const char* optGroup) {
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
    {
        StringBuffer path;
        if (basepath && *basepath)
            path.append(basepath).append(".");
        path.append(tagname);

        StringAttr strvalue;
        esp_convert(params.queryProp(path.str()), strvalue);

        int tempval = defvalue;
        if (strvalue.length())
            tempval = enumstrings.find(strvalue.get());

        if (tempval != NotFound) {
            if (ctx && optGroup)
                ctx->addOptGroup(optGroup);
            value_ = (enumtype) tempval;
            return true;
        }
        else {
            value_ = defvalue;
            return false;
        }
    }
    
    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }
};

// This template is used for auto generation of ESPenum
// TODO: merge to use the orginal template
template <class enumtype> 
class SoapEnumParamNew
{
private:
   enumtype defvalue;
   enumtype value_;
   int count_;
   StringAttr typeName_,baseType_;

   StringArray enumstrings;
   const char* asString() { return toString(value_); }

public:
    SoapEnumParamNew(nilBehavior nb=nilIgnore)
    {
        defvalue = value_ = (enumtype)-1; 
        count_ = 0;
    }

    SoapEnumParamNew(enumtype defvalue_)
    {
        defvalue = defvalue_;
        value_ = (enumtype)-1; 
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

    enumtype getValue() const { return (value_>=0) ? value_ : defvalue; }
    operator const char* ()   { return asString(); }

    operator enumtype () const {return  getValue();}
    const enumtype * operator ->() const { return (value_>=0) ? &value_ : &defvalue;}
    enumtype * operator ->() {return (value_>=0) ? &value_ : &defvalue;}

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
        value_=from.value_;
        count_=from.count_;
        enumstrings.kill();
        ForEachItemIn(idx, from.enumstrings)
            enumstrings.append(from.enumstrings.item(idx));
    }

    void setValue(enumtype valuex)
    {
        if (0<=valuex && valuex < count_)
            value_ = valuex;
        else if (valuex == -1)
            value_ = defvalue;
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
                value_ = (enumtype)tempval; 
        }
    }

    void init(const char* typeName,const char* baseType,const char **strings)
    {
        typeName_.set(typeName);
        baseType_.set(baseType);
        value_ = defvalue;
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

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char* elementname, const char* elementtype="",const char *basepath="", const char *xmlns="")
    {
        rpc_call.add_value(basepath, xmlns, tagname, "string", enumstrings.item(value_));
    }

    void marshall(IEspContext* ctx, StringBuffer &str, const char *tagname, const char *basepath="", bool encodeXml=true,const char *xsdtype="", const char *xmlns="")
    {
        append_start_tag(str, tagname, xmlns, false, NULL);
        str.append(asString());
        append_end_tag(str, tagname, xmlns);
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xmlns="")
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

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
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
    
    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }

    void getXsdDefinition_(IEspContext &context, IHttpMessage *request, StringBuffer &schema, 
        BoolHash &added, const char* descriptions[])
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

/*
inline CGradeLevel Array__Member2Param(CGradeLevel &src)              { return src; }
inline void Array__Assign(CGradeLevel & dest, CGradeLevel const & src){ dest = src; }
inline bool Array__Equal(CGradeLevel const & m, CGradeLevel const p)  { return m==p; }
inline CGradeLevel Array__Empty(CGradeLevel *)                        { return GradeLevel_Undefined; }
inline void Array__Destroy(CGradeLevel & ) { }
MAKEArrayOf(CGradeLevel, CGradeLevel, GradeLevelArray)
*/

template <class enumtype, class cxtype, class arraytype> 
class SoapEnumArrayParam
{
public:
    SoapEnumArrayParam(nilBehavior nb=nilIgnore){}
    virtual ~SoapEnumArrayParam()
    {
    }

    arraytype  &getValue(){return array_;}

    operator arraytype  & () {return  array_;}
    arraytype  * operator ->() {return &array_;}

    void copy(SoapEnumArrayParam<enumtype, cxtype, arraytype> &from)
    {
        array_.kill();
        int count= from.array_.ordinality();
        for (int index = 0; index<count; index++)
            array_.append(from.array_.item(index));
    }

    void marshall(IRpcMessage &rpc_call, const char *tagname, const char *elementname="", const char *elementtype="", const char *basepath="", const char *xmlns="")
    {
        int count= array_.ordinality();
        if (count > 0)
        {           
            const char* itemTag = (!elementname || !*elementname) ? cxtype::queryXsdElementName() : elementname;

            StringBuffer xml;
            for (int index = 0; index<count; index++)
            {
                enumtype e = array_.item(index);
                xml.appendf("<%s>", itemTag);
                encodeUtf8XML(cxtype::stringOf(e),xml);
                xml.appendf("</%s>", itemTag);
            }

            rpc_call.add_value(basepath, xmlns, tagname, elementtype, xml.str(), false);
        }
    }

    void marshall(IEspContext* ctx,StringBuffer &xml, const char *tagname, const char *elementname=NULL, const char *basepath="", const char *xsdtype="", const char *xmlns="")
    {
        int count= array_.ordinality();
        if (count > 0)
        {
            const char* itemTag = (!elementname || !*elementname) ? cxtype::queryXsdElementName() : elementname;
            xml.appendf("<%s>", tagname);

            for (int index = 0; index<count; index++)
            {
                enumtype e = array_.item(index);
                xml.appendf("<%s>", itemTag);
                encodeUtf8XML(cxtype::stringOf(e),xml);
                xml.appendf("</%s>", itemTag);
            }
            xml.appendf("</%s>", tagname);
        }
    }

    bool unmarshall(IEspContext* ctx, CSoapValue &soapval, const char *tagname, const char* optGroup=NULL)
    {
        CSoapValue *sv= soapval.get_value(tagname);
        if (sv)
        {
            SoapValueArray* children = sv->query_children();
            if (children)
            {
                if (ctx && optGroup)
                    ctx->addOptGroup(optGroup);
                ForEachItemIn(x, *children)
                {
                    CSoapValue& onechild = children->item(x);
                    StringBuffer s;
                    onechild.serializeContent(s,NULL);
                    array_.append(cxtype::enumOf(s));
                }
                return true;
            }
        }
        return false;
    }

    bool unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath="", const char* optGroup=NULL, const char *xmlns="")
    {
        CRpcMessage *rpcmsg=dynamic_cast<CRpcMessage *>(&rpc_call);
        if (rpcmsg)
        {
            StringBuffer path(basepath);
            if (basepath!=NULL && basepath[0]!=0)
                path.append("/");
            path.append(tagname);
            CSoapValue *soapval=rpcmsg->get_value(path.str());
            if (soapval) {
                if (optGroup && rpc_call.queryContext())
                    rpc_call.queryContext()->addOptGroup(optGroup);
                unmarshall(rpc_call.queryContext(), *soapval, NULL);
                return true;
            }
        }
        return false;
    }

    bool unmarshall(IEspContext* ctx, IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath=NULL, const char* optGroup=NULL, const char *xsdtype="", const char *xmlns="")
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
                        //buildVarPath(path, tagname, basepath, cxtype::queryXsdElementName(), finger, NULL);
                        array_.append(cxtype::enumOf(finger));
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
                        array_.append(cxtype::enumOf(val));
                        hasValue = true;
                    }
                }
            }
        }

        if (hasValue && ctx && optGroup)
            ctx->addOptGroup(optGroup);

        return hasValue;
    }

    //void unmarshall(const char * msg, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}
    //void unmarshall(IPropertyTree * localnode, const char *tagname, const char *basepath=NULL, const char *xsdtype="", const char *xmlns=""){}

    void setEncodeNewlines(bool encodenl)
    {
        UNIMPLEMENTED;
    }

private:
    arraytype array_;
};


#endif //_SOAP_PARAM_H_
