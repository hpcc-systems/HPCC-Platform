<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:http="http://schemas.xmlsoap.org/wsdl/http/" xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/" xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
<xsl:output method="text" omit-xml-declaration="yes" indent="no"></xsl:output>
<xsl:template match="esxdl">
<xsl:text>#ifndef PRIMITIVE_TYPES_HPP__
#define PRIMITIVE_TYPES_HPP__

#include "jlib.hpp"
#include "jstring.hpp"
#include &lt;string&gt;

using namespace std;

namespace cppplugin
{

class Integer : public CInterface, implements IInterface
{
private:
    int v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Integer(int v_)
    {
        v = v_;
        s.appendf("%d", v_);
    }

    int val()
    {
        return v;
    }

    StringBuffer&amp; str(StringBuffer&amp; buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer&amp; str()
    {
        return s;
    }
};

class Integer64 : public CInterface, implements IInterface
{
private:
    int64_t v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Integer64(int64_t v_)
    {
        v = v_;
        s.appendf("%ld", v_);
    }

    int64_t val()
    {
        return v;
    }

    StringBuffer&amp; str(StringBuffer&amp; buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer&amp; str()
    {
        return s;
    }
};

class Float : public CInterface, implements IInterface
{
private:
    float v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Float(float v_)
    {
        v = v_;
        s.appendf("%f", v_);
    }

    float val()
    {
        return v;
    }

    StringBuffer&amp; str(StringBuffer&amp; buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer&amp; str()
    {
        return s;
    }
};

class Double : public CInterface, implements IInterface
{
private:
    double v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Double(double v_)
    {
        v = v_;
        s.appendf("%f", v_);
    }

    double val()
    {
        return v;
    }

    StringBuffer&amp; str(StringBuffer&amp; buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer&amp; str()
    {
        return s;
    }
};

class Boolean : public CInterface, implements IInterface
{
private:
    bool v;
    StringBuffer s;
public:
    IMPLEMENT_IINTERFACE;

    Boolean(bool v_)
    {
        v = v_;
        s.appendf("%s", v_?"true":"false");
    }

    bool val()
    {
        return v;
    }

    StringBuffer&amp; str(StringBuffer&amp; buf)
    {
        buf.append(s);
        return buf;
    }

    StringBuffer&amp; str()
    {
        return s;
    }
};

class PString : public CInterface, implements IInterface
{
private:
    StringBuffer v;
public:
    IMPLEMENT_IINTERFACE;

    PString(const char* v_)
    {
        v.append(v_);
    }

    PString(size32_t len, const char* v_)
    {
        v.append(len, v_);
    }

    const char* val()
    {
        return v.str();
    }

    StringBuffer&amp; str(StringBuffer&amp; buf)
    {
        buf.append(v);
        return buf;
    }

    StringBuffer&amp; str()
    {
        return v;
    }
};
}
#endif</xsl:text>
</xsl:template>
</xsl:stylesheet>
