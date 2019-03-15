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
    <xsl:output method="text" omit-xml-declaration="yes" indent="no"/>
    <xsl:template match="esxdl">
<xsl:variable name="servicename"><xsl:value-of select="EsdlService/@name"/></xsl:variable>
<xsl:variable name="servicebase"><xsl:value-of select="$servicename"/>ServiceBase</xsl:variable>
#include "<xsl:value-of select="$servicename"/>ServiceBase.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

<xsl:apply-templates select="EsdlService"/>
</xsl:template>

<xsl:template name="outputCppPrimitive">
        <xsl:param name="typename"/>
        <xsl:choose>
            <xsl:when test="$typename='bool'"><xsl:value-of select="'Boolean'"/></xsl:when>
            <xsl:when test="$typename='boolean'"><xsl:value-of select="'Boolean'"/></xsl:when>
            <xsl:when test="$typename='decimal'"><xsl:value-of select="'Double'"/></xsl:when>
            <xsl:when test="$typename='float'"><xsl:value-of select="'Float'"/></xsl:when>
            <xsl:when test="$typename='double'"><xsl:value-of select="'Double'"/></xsl:when>
            <xsl:when test="$typename='integer'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='int64'"><xsl:value-of select="'Integer64'"/></xsl:when>
            <xsl:when test="$typename='uint64'"><xsl:value-of select="'Integer64'"/></xsl:when>
            <xsl:when test="$typename='long'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='int'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='short'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='nonPositiveInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='negativeInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='nonNegativeInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsigned'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedLong'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedInt'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedShort'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedByte'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='positiveInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='base64Binary'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='string'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='xsdString'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='normalizedString'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='binary'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$typename"/></xsl:otherwise>
        </xsl:choose>
</xsl:template>

<xsl:template match="EsdlService">
    <xsl:variable name="servicename" select="@name"/>
    <xsl:variable name="servicebase"><xsl:value-of select="$servicename"/>ServiceBase</xsl:variable>
    <xsl:for-each select="EsdlMethod">
    <xsl:text>extern "C" int on</xsl:text><xsl:value-of select="../@name"/><xsl:value-of select="@name"/>(const char* CtxStr, const char* ReqStr, StringBuffer&amp; RespStr)
{
    Owned&lt;<xsl:value-of select="$servicebase"/>&gt; service = create<xsl:value-of select="$servicename"/>ServiceObj();
    return service-&gt;<xsl:value-of select="@name"/><xsl:text>(CtxStr, ReqStr, RespStr);
}

</xsl:text>
        </xsl:for-each>

<xsl:text>
class </xsl:text><xsl:value-of select="$servicename"/>UnSerializer : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    <xsl:value-of select="$servicename"/>UnSerializer()
    {
    }

    int unserialize(EsdlContext* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        if (ptree->hasProp("username"))
            obj->username.setown(new PString(ptree->queryProp("username")));
        if (ptree->hasProp("clientMajorVersion"))
            obj->clientMajorVersion.setown(new Integer(ptree->getPropInt("clientMajorVersion")));
        if (ptree->hasProp("clientMinorVersion"))
            obj->clientMinorVersion.setown(new Integer(ptree->getPropInt("clientMinorVersion")));
    }<xsl:text>
</xsl:text>

    <xsl:for-each select="/esxdl/EsdlRequest|/esxdl/EsdlResponse|/esxdl/EsdlStruct">
        <xsl:call-template name="generateUnserializeForStruct"/>
    </xsl:for-each>

    <xsl:for-each select="/esxdl/EsdlRequest|/esxdl/EsdlResponse|/esxdl/EsdlStruct">
        <xsl:call-template name="generateSerializeForStruct"/>
    </xsl:for-each>
<xsl:text>
};
</xsl:text>
    <xsl:for-each select="EsdlMethod">
    <xsl:text>int </xsl:text><xsl:value-of select="$servicebase"/>::<xsl:value-of select="@name"/><xsl:text>(const char* CtxStr, const char* ReqStr, StringBuffer&amp; RespStr)
{</xsl:text>
    Owned&lt;<xsl:value-of select="$servicename"/>UnSerializer&gt; UnSe = new <xsl:value-of select="$servicename"/>UnSerializer();
    Owned&lt;EsdlContext&gt; ctx = new EsdlContext();
    if (CtxStr &amp;&amp; *CtxStr)
    {
        Owned&lt;IPropertyTree&gt; ptree = createPTreeFromXMLString(CtxStr);
        UnSe-&gt;unserialize(ctx.get(), ptree.get());
    }
    Owned&lt;<xsl:value-of select="$servicebase"/>&gt; service = create<xsl:value-of select="$servicename"/>ServiceObj();
    Owned&lt;<xsl:value-of select="@request_type"/>&gt; req = new <xsl:value-of select="@request_type"/>();
    Owned&lt;IPropertyTree&gt; ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned&lt;<xsl:value-of select="@response_type"/>&gt; resp = service-&gt;<xsl:value-of select="@name"/>(ctx.get(), req.get());
    RespStr = "&lt;Response&gt;&lt;Results&gt;&lt;Result&gt;&lt;Dataset name=\"<xsl:value-of select="@response_type"/>\">&lt;Row&gt;";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("&lt;/Row&gt;&lt;/Dataset&gt;&lt;/Result&gt;&lt;/Results&gt;&lt;/Response&gt;");

    return 0;
}<xsl:text>
</xsl:text>
    </xsl:for-each>
</xsl:template>

<xsl:template name="generateUnserializeForStruct">
    int unserialize(<xsl:value-of select="@name"/>* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;<xsl:text>

</xsl:text>
        <xsl:for-each select="*">
        <xsl:variable name="fieldclass" select="name(.)"/>
        <xsl:variable name="fieldname"  select="@name"/>
        <xsl:variable name="fieldvar"  select="concat('m_', $fieldname)"/>
        <xsl:choose>
            <xsl:when test="$fieldclass='EsdlElement'">
            <xsl:choose>
                <xsl:when test="@complex_type">
        subtree = ptree->queryPropTree("<xsl:value-of select="$fieldname"/>");
        if (subtree != nullptr)
        {
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new <xsl:value-of select="@complex_type"/>());
            unserialize(obj-&gt;<xsl:value-of select="$fieldvar"/>.get(), subtree);
        }<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:variable name="primitive">
                        <xsl:call-template name="outputCppPrimitive">
                           <xsl:with-param name="typename">
                              <xsl:value-of select="@type"/>
                           </xsl:with-param>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:choose>
                    <xsl:when test="$primitive='PString'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new PString(ptree-&gt;queryProp("<xsl:value-of select="$fieldname"/>")));<xsl:text>
</xsl:text>
                    </xsl:when>
                    <xsl:when test="$primitive='Integer'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new Integer(ptree-&gt;getPropInt("<xsl:value-of select="$fieldname"/>")));<xsl:text>
</xsl:text>
                    </xsl:when>
                    <xsl:when test="$primitive='Integer64'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new Integer64(ptree-&gt;getPropInt64("<xsl:value-of select="$fieldname"/>")));<xsl:text>
</xsl:text>
                    </xsl:when>
                    <xsl:when test="$primitive='Boolean'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new Boolean(ptree-&gt;getPropBool("<xsl:value-of select="$fieldname"/>")));<xsl:text>
</xsl:text>
                    </xsl:when>
                    <xsl:when test="$primitive='Float'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new Float(atof(ptree-&gt;queryProp("<xsl:value-of select="$fieldname"/>"))));<xsl:text>
</xsl:text>
                    </xsl:when>
                    <xsl:when test="$primitive='Double'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/>.setown(new Double(atof(ptree-&gt;queryProp("<xsl:value-of select="$fieldname"/>"))));<xsl:text>
</xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                    </xsl:otherwise>
                    </xsl:choose>
                </xsl:otherwise>
            </xsl:choose>
            </xsl:when>
            <xsl:when test="$fieldclass='EsdlArray'">
        subtree = ptree->queryPropTree("<xsl:value-of select="$fieldname"/>");
        if (subtree != nullptr)
        {
            Owned&lt;IPropertyTreeIterator&gt; itr = subtree->getElements("<xsl:value-of select="@item_tag"/>");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &amp;itr->query();<xsl:text>
</xsl:text>
            <xsl:variable name="primitive">
                <xsl:call-template name="outputCppPrimitive">
                   <xsl:with-param name="typename">
                      <xsl:value-of select="@type"/>
                   </xsl:with-param>
                </xsl:call-template>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="$primitive=@type">
                Owned&lt;<xsl:value-of select="@type"/>&gt; oneobj(new <xsl:value-of select="@type"/>());
                unserialize(oneobj.get(), onetree);
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*oneobj.getClear());<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:when test="$primitive='PString'">
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*(new PString(onetree->queryProp("."))));<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:when test="$primitive='Integer'">
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*(new Integer(onetree->getPropInt("."))));<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:when test="$primitive='Integer64'">
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*(new Integer64(onetree->getPropInt64("."))));<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:when test="$primitive='Boolean'">
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*(new Boolean(onetree->getPropBool("."))));<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:when test="$primitive='Float'">
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*(new Float(atof(onetree->queryProp(".")))));<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:when test="$primitive='Double'">
                obj-&gt;<xsl:value-of select="$fieldvar"/>.append(*(new Double(atof(onetree->queryProp(".")))));<xsl:text>
</xsl:text>
                </xsl:when>
            </xsl:choose>
            }<xsl:text>
</xsl:text>
         }<xsl:text>
</xsl:text>
            </xsl:when>
            <xsl:when test="$fieldclass='EsdlEnum'">
        if (ptree-&gt;hasProp("<xsl:value-of select="$fieldname"/>"))
            obj-&gt;<xsl:value-of select="$fieldvar"/> = EnumHandler<xsl:value-of select="@enum_type"/>::fromString(ptree-&gt;queryProp("<xsl:value-of select="$fieldname"/>"));<xsl:text>
</xsl:text>
            </xsl:when>
            <xsl:otherwise>
            </xsl:otherwise>
        </xsl:choose>
        </xsl:for-each>
        return 0;
    }
</xsl:template>

<xsl:template name="generateSerializeForStruct">
    int serialize(<xsl:value-of select="@name"/>* obj, StringBuffer&amp; buf)
    {
        if (!obj)
            return 0;<xsl:text>
</xsl:text>
        <xsl:for-each select="*">
        <xsl:variable name="fieldclass" select="name(.)"/>
        <xsl:variable name="fieldname"  select="@name"/>
        <xsl:variable name="fieldvar"  select="concat('m_', $fieldname)"/>
        <xsl:choose>
            <xsl:when test="$fieldclass='EsdlElement'">
        if (obj-&gt;<xsl:value-of select="$fieldvar"/>)
        {
            buf.append("&lt;<xsl:value-of select="$fieldname"/>&gt;");<xsl:text>
</xsl:text>
            <xsl:choose>
                <xsl:when test="@complex_type">
<xsl:text>            </xsl:text>serialize(obj-&gt;<xsl:value-of select="$fieldvar"/>, buf);<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:variable name="primitive">
                        <xsl:call-template name="outputCppPrimitive">
                           <xsl:with-param name="typename">
                              <xsl:value-of select="@type"/>
                           </xsl:with-param>
                        </xsl:call-template>
                    </xsl:variable>
<xsl:text>            </xsl:text>buf.append(obj-&gt;<xsl:value-of select="$fieldvar"/>-&gt;str());<xsl:text>
</xsl:text>
                </xsl:otherwise>
            </xsl:choose>
<xsl:text>            </xsl:text>buf.append("&lt;/<xsl:value-of select="$fieldname"/>&gt;");<xsl:text>
        }
</xsl:text>
            </xsl:when>
            <xsl:when test="$fieldclass='EsdlArray'">
        if (obj-&gt;<xsl:value-of select="$fieldvar"/>.length() > 0)
        {
            buf.append("&lt;<xsl:value-of select="$fieldname"/>&gt;");
            ForEachItemIn(i, obj-&gt;<xsl:value-of select="$fieldvar"/>)
            {<xsl:text>
</xsl:text>
            <xsl:variable name="primitive">
                <xsl:call-template name="outputCppPrimitive">
                   <xsl:with-param name="typename">
                      <xsl:value-of select="@type"/>
                   </xsl:with-param>
                </xsl:call-template>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="$primitive=@type">
                <xsl:text>                    </xsl:text><xsl:value-of select="@type"/>&amp; oneitem = obj-&gt;<xsl:value-of select="$fieldvar"/>.item(i);
                buf.append("&lt;<xsl:value-of select="@item_tag"/>&gt;");
                serialize(&amp;oneitem, buf);
                buf.append("&lt;/<xsl:value-of select="@item_tag"/>&gt;");<xsl:text>
</xsl:text>
                </xsl:when>
                <xsl:otherwise>
                <xsl:text>            </xsl:text><xsl:value-of select="$primitive"/>&amp; oneitem = obj-&gt;<xsl:value-of select="$fieldvar"/>.item(i);
                buf.append("&lt;<xsl:value-of select="@item_tag"/>&gt;").append(oneitem.str()).append("&lt;/<xsl:value-of select="@item_tag"/>&gt;");<xsl:text>
</xsl:text>
                </xsl:otherwise>
            </xsl:choose>
            }
            buf.append("&lt;/<xsl:value-of select="$fieldname"/>&gt;");
        }<xsl:text>
</xsl:text>
            </xsl:when>
            <xsl:when test="$fieldclass='EsdlEnum'">
        if (obj-&gt;<xsl:value-of select="$fieldvar"/> != <xsl:value-of select="@enum_type"/>::UNSET)
            buf.append("&lt;<xsl:value-of select="$fieldname"/>&gt;").append(EnumHandler<xsl:value-of select="@enum_type"/>::toString(obj-&gt;<xsl:value-of select="$fieldvar"/>)).append("&lt;/<xsl:value-of select="$fieldname"/>&gt;");<xsl:text>
</xsl:text>
            </xsl:when>
            <xsl:otherwise>
            </xsl:otherwise>
        </xsl:choose>
        </xsl:for-each>
        return 0;
    }<xsl:text>
</xsl:text>
</xsl:template>

</xsl:stylesheet>
