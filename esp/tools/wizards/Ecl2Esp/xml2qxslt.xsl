<?xml version="1.0" encoding="UTF-8"?>
<!--

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
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default">

<xsl:output method="text"/>
<xsl:param name="method" select="'EspMethod'"/>
<!--by default, this stylesheet translates response from a Roxie
    Set the following to 0 to generate request to be sent to 3rd party server-->
<xsl:param name="response" select="1"/>
<!--the following generates END_HANDLE[_KEEPTAG]_ORDERED instead of END_HANDLE[_KEEPTAG]-->
<xsl:param name="orderedRequest" select="0"/>
<!--the following is suffix used for names of lists of elements like 'List' would group 
    Address elements under AddressList (corresponding to arrays in SCM) -->
<xsl:param name="listSuffix" select="'s'"/>

<xsl:variable name="trueValue" select="'true'"/>
<xsl:variable name="falseValue" select="'false'"/>

<xsl:include href="xml2common.xsl"/>


<xsl:variable name="ReqRespSuffix">
    <xsl:choose>
        <xsl:when test="$response=0">Request</xsl:when>
        <xsl:otherwise>Response</xsl:otherwise>
    </xsl:choose>
</xsl:variable>

<xsl:key name="k" match="*" use="concat(name(..), ../@name, name(), @name)"/>

<xsl:template match="/">
    <xsl:if test="$response=0">//======================================================================
// <xsl:value-of select="$method"/>
//======================================================================
#pragma warning(disable:4786)
#include "rxcommon.h"
#include "wsm_<xsl:value-of select="$method"/>.hpp"
</xsl:if>

//======================================================================
// <xsl:value-of select="concat($method, $ReqRespSuffix)"/>
//======================================================================
    <xsl:apply-templates select="$root">
        <xsl:with-param name="defineFunction" select="1"/>
    </xsl:apply-templates>
    <xsl:for-each select="$root">
        <xsl:choose>
            <xsl:when test="$response=0">
                <xsl:call-template name="generateRequest"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="generateResponse"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:for-each>
</xsl:template>


<xsl:template name="generateRequest">
<xsl:text disable-output-escaping="yes"><![CDATA[
static QxHandle* createRequestHandle()
{
    QxHandle* null0H = createNullHandle();
    BEGIN_HANDLE(Options)
]]></xsl:text>
        <xsl:if test="MaxResultsThisTime">      { "ReturnCount", "MaxResultsThisTime", createIntHandle(true)},
</xsl:if>
        <xsl:if test="SkipRecords">     { "StartingRecord", "SkipRecords", createStartingRecordHandle()},
</xsl:if>
        <xsl:if test="PhoneticMatch">       { "UsePhonetics", "PhoneticMatch", createBoolHandle() },
</xsl:if>
        <xsl:if test="AllowNickNames">      { "UseNicknames", "AllowNicknames", createBoolHandle() },
</xsl:if>
        <xsl:if test="NoDeepDive">      { "IncludeAlsoFound", "NoDeepDive", createAlwaysOutputBoolHandle("NoDeepDive", "0", "1")},
</xsl:if>
        <xsl:for-each select="$root/*">
            <xsl:variable name="name">
                <xsl:call-template name="getNodeName"/>
            </xsl:variable>
            <xsl:variable name="n" select="translate($name, $upper, $lower)"/>
            <xsl:if test="$response=0 and substring($n, 1, 7)='include' and $n!='include_schemas_' and $n!='includealsofound'">
                <xsl:variable name="outName">
                    <xsl:call-template name="getOutNameNode"/>
                </xsl:variable>
                <xsl:text disable-output-escaping="yes">      { "</xsl:text>
                <xsl:value-of select="$outName"/>", <xsl:choose>
                    <xsl:when test="$outName!=name()">
                        <xsl:value-of select="concat($quote, name(), $quote)"/>
                    </xsl:when>
                    <xsl:otherwise>NULL</xsl:otherwise>
                </xsl:choose>
                <xsl:value-of select="concat(', createBoolHandle() },', $eol)"/>
            </xsl:if>
        </xsl:for-each>     { "Blind", NULL, null0H },
        { "Encrypt", NULL, null0H },
    END_HANDLE_KEEPTAG(Options)
    <xsl:apply-templates select="$root"/>
    BEGIN_HANDLE(request)
        { "User", "",  createRxReqUserHandle() },
        { "Options", "", OptionsH },
        { "<xsl:value-of select="$by"/>", "", <xsl:value-of select="$by"/>H },
        REQUEST_COMMON
    END_HANDLE_REQ(request)
    
    return requestH;
}

static Owned&lt;QxHandlePool&gt; g_reqHandlePool(createHandlePool(createRequestHandle));

QUICKXSLT_CAPI int Qx<xsl:value-of select="$method"/>Request( IEspContext&amp; ctx, const char* soapRequest,
                        StringBuffer&amp; out, const char* rootTag, void* parms)
{
    return g_reqHandlePool->transform(ctx, out/*output xml*/, soapRequest/*input xml*/, 
        "<xsl:value-of select="$method"/>Request"/*root in*/, "<xsl:value-of select="name($root)"/>"/*root out*/, parms, 
        NULL/*recCountXPath*/, NULL/*reqExtra*/, false/*ignore namespaces*/, false/*throw exceptions*/);
}
</xsl:template>


<xsl:template name="generateResponse">
static QxHandle* createResponseHandle()<xsl:text>
{</xsl:text>
    <xsl:apply-templates select="$root/*"/>
    <xsl:variable name="row" select="name($root/*[1])"/>
    <xsl:variable name="outName">
        <xsl:for-each select="$root/*[1]">
            <xsl:call-template name="getOutNameNode"/>
        </xsl:for-each>
    </xsl:variable>
    BEGIN_HANDLE(Dataset)
        {"<xsl:value-of select="$row"/>", "Record", <xsl:value-of select="$outName"/>H},
    END_HANDLE_RESP_KEEPTAG(Dataset)
    <xsl:text disable-output-escaping="yes">
    
    PARENTSUB_HANDLE2(Header, HeaderH)
    
    // exceptions
    BEGIN_HANDLE(Exception)
        {"Source"},
        {"Message"},
        {"Code", NULL, createIntHandle()},
        {"Location"},
    END_HANDLE_RESP_KEEPTAG(Exception)

    BEGIN_HANDLE(Exceptions)
        {"Row", "Item", ExceptionH},
    END_HANDLE_RESP_KEEPTAG2(Exceptions, "Exceptions")
    
    QxValueInspector* recCountInH = new QxValueInspector;
    QxHandle* recCountOutH = createNodeRelocator(recCountInH, "RecordCount");

    BEGIN_HANDLE(response)
        { "Dataset[@name='Results']", "Records", DatasetH},
        { "Dataset[@name='RecordsAvailable']", "", createRoxieRecordCount(recCountInH)},
        { "Dataset[@name='Exception']", "",  HeaderH, ExceptionsH },
        { "Exception", "", HeaderH, createRoxyExceptionHandle() },
        { "RecordCount", NULL, recCountOutH },
        { "XmlSchema", NULL, createNullHandle() },
    END_HANDLE_RESP(response)
        
    return responseH;
}
</xsl:text>

static Owned&lt;QxHandlePool&gt; g_respHandlePool(createHandlePool(createResponseHandle));

QUICKXSLT_CAPI int Rx<xsl:value-of select="$method"/>Resp(IEspContext&amp; ctx, const char* soapResponse,
                             StringBuffer&amp; out, const char* rootTag, void* parms)
{
    return g_respHandlePool->transform(ctx, out, soapResponse, "Result", rootTag, parms, NULL, NULL, true);
}
</xsl:template>


<xsl:template match="*">
<xsl:param name="defineFunction" select="0"/>
    <xsl:variable name="parent" select=".."/>
    <xsl:variable name="nodes" select="key('k', concat(name(..), ../@name, name(), @name))"/>
    <xsl:variable name="snodes" select="$nodes[count(.. | $parent) = 1]"/>  
    <xsl:variable name="n" select="count($nodes)"/>
    <xsl:variable name="ns" select="count($snodes)"/>
    <xsl:variable name="firstSibling" select="count(. | $snodes[1]) = 1"/><!--first sibling with this name-->
    <xsl:variable name="numberSuffix">
        <xsl:call-template name="getNumberSuffixForNode"/>
    </xsl:variable>
    
    <!--is this node unique for parent or first one of repeting nodes of same name under parent-->
    <xsl:if test="(not($snodes[2]) or $firstSibling) and ($numberSuffix='' or $numberSuffix='1')">
        <xsl:variable name="name" select="name()"/>
        <xsl:variable name="outName">
            <xsl:call-template name="getOutName">
                <xsl:with-param name="inName" select="substring($name, 1, string-length($name) - string-length($numberSuffix))"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:apply-templates select="*">
            <xsl:with-param name="defineFunction" select="$defineFunction"/>
        </xsl:apply-templates>
        <!--xsl:variable name="firstInDoc" select="$ns = $n or count(.|$nodes[1])=1"/-->
        <xsl:variable name="firstInDoc" select="count(.|$nodes[1])=1"/>
        <xsl:variable name="firstChildName" select="name(*[1])"/>
        <xsl:variable name="isArray" select="(*[2]) and not(*[name()!=$firstChildName])"/>
        <xsl:if test="$firstInDoc and *[1] and not($isArray)">
            <xsl:choose>
                <xsl:when test="$defineFunction and ($ns &lt; $n)">
                    <xsl:call-template name="printFunctionHeader">
                        <xsl:with-param name="fnName" select="concat('create', $outName, 'Handle()')"/>
                    </xsl:call-template>
                    <xsl:call-template name="map"/>
                </xsl:when><!--defineFunction-->
                <xsl:when test="$defineFunction=0 and $ns=$n">
                    <xsl:call-template name="map"/>
                </xsl:when>
            </xsl:choose>
        </xsl:if>
        <xsl:if test="$snodes[2] and $firstSibling">
            <xsl:if test="$firstInDoc and (($defineFunction and ($ns &lt; $n)) or ($defineFunction=0 and $ns=$n))">
                <xsl:variable name="listName" select="concat($outName, $listSuffix)"/>
                <xsl:if test="$defineFunction and ($ns &lt; $n)">
                    <xsl:if test="*[1]">
                        <xsl:value-of select="concat($eol, '    return ', $outName, 'H;', $eol, '}', $eol)"/>
                    </xsl:if>
                    <xsl:if test="$response=0">             
                        <xsl:call-template name="printFunctionHeader">
                            <xsl:with-param name="fnName" select="concat('create', $listName, 'Handle()')"/>
                        </xsl:call-template>
                    </xsl:if>
                </xsl:if>
                <xsl:choose>
                    <xsl:when test="$response=1">
                        <xsl:if test="count(../*) &gt; count($snodes)">
    PARENTSUB_HANDLE2(<xsl:value-of 
    select="concat($outName, $listSuffix, ', ', $listName, 'H )', $eol)"/></xsl:if>
                    </xsl:when><!--response-->
                    <xsl:otherwise><!--request-->
    BEGIN_HANDLE(<xsl:value-of select="concat($listName, ')')"/>
    { <xsl:value-of select="concat($quote, $outName, $quote, ', ', $quote, $name, $quote)"/>
    <xsl:if test="*[1]">
        <xsl:choose>
            <xsl:when test="$defineFunction and ($ns &lt; $n)">
                <xsl:value-of select="concat(', create', $outName, 'Handle()')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="concat(', ', $outName, 'H')"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:if>
    <xsl:text>}, </xsl:text>
    END_HANDLE_KEEPTAG<xsl:if test="$orderedRequest">_ORDERED</xsl:if>(<xsl:value-of select="concat($listName, ')', $eol)"/>
                    </xsl:otherwise><!--request-->
                </xsl:choose>
            </xsl:if>
        </xsl:if>
        <xsl:if test="$response=0 and $firstInDoc and ($defineFunction and ($ns &lt; $n)) and (*[1] or $ns &gt; 1)">
            <xsl:value-of select="concat($eol, '    return ', $outName)"/>
            <xsl:if test="$ns &gt; 1"><xsl:value-of select="$listSuffix"/></xsl:if>
            <xsl:value-of select="concat('H;', $eol, '}', $eol)"/>
        </xsl:if>
    </xsl:if>   
</xsl:template>

<xsl:template name="map">
<xsl:param name="headerFooter" select="1"/>
    <xsl:variable name="name" select="name()"/>
    <xsl:variable name="outName">
        <xsl:choose>
            <xsl:when test="$response=0 and count(..|/)=1">
                <xsl:value-of select="$by"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:variable name="temp">
                    <xsl:call-template name="getOutNameNode"/>
                </xsl:variable>
                <xsl:variable name="numberSuffix">
                    <xsl:call-template name="getNumberSuffixForNode"/>
                </xsl:variable>
                <xsl:value-of select="substring($temp, 1, string-length($temp) - string-length($numberSuffix))"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    
    <xsl:if test="$headerFooter">
        <xsl:variable name="p" select="count(preceding::*[*[1]] | ancestor::*)"/>
        <xsl:variable name="nullH">null<xsl:if test="$p!=0">
            <xsl:value-of select="$p + 1"/></xsl:if>
            <xsl:text>H</xsl:text>
        </xsl:variable>
    QxHandle* <xsl:value-of select="$nullH"/> = createNullHandle();
<xsl:call-template name="groupKnownElements"/>  
    BEGIN_HANDLE(<xsl:value-of select="$outName"/>)
        { "*", NULL, <xsl:value-of select="$nullH"/>}, // igore all unhandled fields
</xsl:if>
    <xsl:for-each select="*">
        <xsl:choose>
            <xsl:when test="$response=0"><!--processing request-->
                <xsl:choose>
                    <xsl:when test="name()='LastName'">     { "Name", "", createRxReqNameHandle()},
</xsl:when>
                    <xsl:when test="name()='Addr'">     { "Address", "", createRxReqAddrHandle()},
</xsl:when>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise><!--processing response-->
                <xsl:variable name="cname">
                    <xsl:call-template name="getNodeName"/>
                </xsl:variable>
                <xsl:variable name="lcName" select="translate($cname, $upper_, $lower)"/>
                <xsl:variable name="siblingNames">
                    <xsl:text> </xsl:text>
                    <xsl:for-each select="../*">
                        <xsl:value-of select="concat(name(), ' ')"/>
                    </xsl:for-each>
                </xsl:variable>
                <xsl:variable name="lcSiblingNames" select="translate($siblingNames, $upper_, $lower)"/>
                <xsl:variable name="namePart">
                    <xsl:variable name="hasLastNameSibling">
                        <xsl:call-template name="searchMultiplePatterns">
                            <xsl:with-param name="s" select="$lcSiblingNames"/>
                            <xsl:with-param name="patterns" select="'lname|lastname'"/>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:if test="$hasLastNameSibling=1">
                        <xsl:choose>
                            <xsl:when test="$lcName='lastname' or $lcName='lname'">Last</xsl:when>
                            <xsl:when test="$lcName='firstname' or $lcName='fname'">First</xsl:when>
                            <xsl:when test="$lcName='middlename' or $lcName='mname'">Middle</xsl:when>
                            <xsl:when test="$lcName='title'">Prefix</xsl:when>
                            <xsl:when test="$lcName='name'">Full</xsl:when>
                            <xsl:when test="$lcName='namesuffix'">Suffix</xsl:when>
                        </xsl:choose>
                    </xsl:if>
                </xsl:variable>
                <xsl:variable name="addressPart">
                    <xsl:variable name="hasAddressSibling">
                        <xsl:variable name="lc1SiblingNames" select="translate($lcSiblingNames, '1', '')"/>
                        <xsl:call-template name="searchMultiplePatterns">
                            <xsl:with-param name="s" select="$lc1SiblingNames"/>
                            <xsl:with-param name="patterns" select="'addr|address|streetaddress'"/>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:if test="$hasAddressSibling=1">
                        <xsl:variable name="lcdName" select="translate($lcName, $digits, '')"/>
                        <xsl:choose>
                            <xsl:when test="$lcName='predir'">StreetPreDirection</xsl:when>
                            <xsl:when test="$lcName='primrange'">StreetNumber</xsl:when>
                            <xsl:when test="$lcName='primname'">StreetName</xsl:when>
                            <xsl:when test="$lcName='postdir'">StreetPostDirection</xsl:when>
                            <xsl:when test="$lcName='suffix' or $lcName='addrsuffix'">StreetSuffix</xsl:when>
                            <xsl:when test="$lcName='unitdesig'">UnitDesignation</xsl:when>
                            <xsl:when test="$lcName='secrange'">UnitNumber</xsl:when>
                            <xsl:when test="$lcName='vcityname'">City</xsl:when>
                            <xsl:when test="$lcName='st'">State</xsl:when>
                            <xsl:when test="$lcName='zip5' or $lcName='zip'">Zip5</xsl:when>
                            <xsl:when test="$lcName='zip4'">Zip4</xsl:when>
                            <xsl:when test="$lcName='countyname'">County</xsl:when>
                            <xsl:when test="$lcdName='addr' or $lcdName='address' or $lcdName='streetaddress'">StreetAddress1</xsl:when>
                            <xsl:when test="$lcName='csz'">StateCityZip</xsl:when>
                        </xsl:choose>
                    </xsl:if>
                </xsl:variable>
                <xsl:choose>
                    <xsl:when test="$namePart!=''">
                        <xsl:value-of 
                        select="concat($tab, $tab, '{ ', $quote, $cname, $quote, ', ', $quote, $namePart, $quote, ', NameH },', $eol)"/>
                    </xsl:when>
                    <xsl:when test="$addressPart!=''">
                        <xsl:value-of select="concat($tab, $tab, '{ ', $quote, $cname, $quote, ', ', $quote, $addressPart, $quote, ', AddressH },', $eol)"/>
                    </xsl:when>
                    <xsl:when test="$lcName='isdeepdive'">      { "isdeepdive", "AlsoFound", createBoolHandle("true", "false") },
</xsl:when>
                </xsl:choose>
        </xsl:otherwise>
        </xsl:choose>
    </xsl:for-each>
    
    <xsl:variable name="parent" select="."/>
    <xsl:for-each select="*">
        <xsl:variable name="skip">
            <xsl:call-template name="shouldSkipElement">
                <xsl:with-param name="generatingScm" select="0"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="name2" select="name()"/>
        <xsl:variable name="nodes" select="key('k', concat(name(..), ../@name, $name2, @name))"/>
        <xsl:variable name="snodes" select="$nodes[count(.. | $parent) = 1]"/>  
        <xsl:variable name="n" select="count($nodes)"/>
        <xsl:variable name="ns" select="count($snodes)"/>
        <xsl:if test="$skip=0 and count(. | $snodes[1]) = 1">
            <xsl:variable name="num_snodes" select="count($snodes)"/>
            <xsl:call-template name="mapEntry">
                <xsl:with-param name="group" select="number($num_snodes &gt; 1 and (count(../*) &gt; $num_snodes))"/>
                <xsl:with-param name="createHandleFunction" select="$n - $ns"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:for-each>
    <xsl:if test="$headerFooter">
        <xsl:choose>
            <xsl:when test="$response"> END_HANDLE_RESP_KEEPTAG</xsl:when>
            <xsl:otherwise> END_HANDLE_KEEPTAG</xsl:otherwise>
        </xsl:choose>
        <xsl:if test="$orderedRequest">_ORDERED</xsl:if>
        <xsl:value-of select="concat('(', $outName, ')', $eol)"/>
    </xsl:if><!--headerFooter-->
</xsl:template>
    

<xsl:template name="mapEntry">
<xsl:param name="group" select="0"/>
<xsl:param name="createHandleFunction" select="0"/>
    <xsl:variable name="outName">
        <xsl:choose>
            <xsl:when test="$response=0 and count(..|/)=1">
                <xsl:value-of select="$by"/>
            </xsl:when>
            <xsl:when test="name()!=''">
                <xsl:call-template name="getOutNameNode"/>
            </xsl:when>
            <xsl:otherwise>Value</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:variable name="name2" select="name()"/>
    <xsl:variable name="isDate">
        <xsl:choose>
            <xsl:when test="$outName = $by or $outName = 'Value'">0</xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="isDate">
                    <xsl:with-param name="s" select="name()"/>
                </xsl:call-template>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:variable name="hName">
        <xsl:choose>
            <xsl:when test="$createHandleFunction">
                <xsl:value-of select="concat(', create', $outName, 'Handle()')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="concat(', ', $outName, 'H')"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:variable name="listHName">
        <xsl:choose>
            <xsl:when test="$createHandleFunction">
                <xsl:value-of select="concat('create', $outName, $listSuffix, 'Handle()')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="concat($outName, $listSuffix, 'H')"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:choose>
        <xsl:when test="$response">
            <xsl:variable name="firstChild" select="*[1]"/>
            <xsl:variable name="isArray" select="(*[2]) and not($firstChild/following-sibling::*[name()!=name($firstChild)])"/>
            <xsl:variable name="numberSuffix">
                <xsl:call-template name="getNumberSuffixForNode"/>
            </xsl:variable> 
            <xsl:text disable-output-escaping="yes">      { "</xsl:text>
            <xsl:value-of select="$name2"/>
            <xsl:if test="@name">[@name='<xsl:value-of select="@name"/>']</xsl:if>
            <xsl:value-of select="$quote"/>
            <xsl:if test="$outName != name() or *[1] or $group or $isArray">
                <xsl:choose>
                    <xsl:when test="$numberSuffix!=''">
                        <xsl:variable name="baseName">
                            <xsl:variable name="len" select="string-length($outName)"/>
                            <xsl:variable name="lsuf" select="string-length($numberSuffix)"/>
                            <xsl:value-of select="substring($outName, 1, $len - $lsuf)"/>
                        </xsl:variable>
                        <xsl:value-of select="concat(', ', $quote, $baseName, $quote, ', ', $baseName, $listSuffix, 'H')"/>
                    </xsl:when>
                    <xsl:when test="$isArray">
                        <xsl:text>, </xsl:text>
                        <xsl:choose>
                            <xsl:when test="$outName != name()">
                                <xsl:value-of select="concat($quote, $outName, $quote)"/>
                            </xsl:when>
                            <xsl:otherwise>NULL</xsl:otherwise>
                        </xsl:choose>
                        <xsl:variable name="childName">
                            <xsl:value-of select="name($firstChild)"/>
                        </xsl:variable>
                        <xsl:variable name="outChildName">
                            <xsl:for-each select="$firstChild">
                                <xsl:call-template name="getOutNameNode"/>
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:variable name="parent" select="."/>
                        <xsl:value-of select="concat(', createArrayHandle(', $quote, $childName, $quote)"/>
                        <xsl:for-each select="$firstChild">
                            <xsl:variable name="cnodes" select="key('k', concat(name(..), ../@name, $childName, @name))"/>
                            <xsl:variable name="csnodes" select="$cnodes[count(.. | $parent) = 1]"/>    
                            <xsl:variable name="cn" select="count($cnodes)"/>
                            <xsl:variable name="cns" select="count($csnodes)"/>
                            <xsl:call-template name="outputRespHandleInfo">
                                <xsl:with-param name="hName">
                                    <xsl:choose>
                                        <xsl:when test="$cn - $cns">
                                            <xsl:value-of select="concat(', create', $outChildName, 'Handle()')"/>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="concat(', ', $outChildName, 'H')"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:with-param>
                                <xsl:with-param name="outName" select="$outChildName"/>
                                <xsl:with-param name="isDate">
                                    <xsl:call-template name="isDate">
                                        <xsl:with-param name="s" select="name($firstChild)"/>
                                    </xsl:call-template>
                                </xsl:with-param>
                            </xsl:call-template>
                        </xsl:for-each>
                        <xsl:if test="$childName != $outChildName">
                            <xsl:value-of select="concat(', ', $quote, $outChildName, $quote)"/>
                        </xsl:if>
                        <xsl:text>)</xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:text>, </xsl:text>
                        <xsl:choose>
                            <xsl:when test="$outName != name()">
                                <xsl:value-of select="concat($quote, $outName, $quote)"/>
                            </xsl:when>
                            <xsl:otherwise>NULL</xsl:otherwise>
                        </xsl:choose>
                        <xsl:if test="$group">
                            <xsl:value-of select="concat(', ', $listHName)"/>
                        </xsl:if>
                        <xsl:call-template name="outputRespHandleInfo">
                            <xsl:with-param name="hName" select="$hName"/>
                            <xsl:with-param name="outName" select="$outName"/>
                            <xsl:with-param name="isDate" select="$isDate"/>
                        </xsl:call-template>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:if>
        </xsl:when>
        <xsl:otherwise><!--request-->
            <xsl:text disable-output-escaping="yes">      { "</xsl:text>
            <xsl:choose>
                <xsl:when test="$group">
                    <xsl:value-of select="concat($outName, $listSuffix, $quote, ', ', $quote, $quote, ', ', $listHName)"/>
                </xsl:when>
                <xsl:when test="$outName != name() or *[1]">
                    <xsl:value-of select="concat($outName, $quote, ', ', $quote, $name2, $quote)"/>
                    <xsl:if test="*[1]">
                        <xsl:choose>
                            <xsl:when test="$createHandleFunction">
                                <xsl:value-of select="concat(', create', $outName, 'Handle()')"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="$hName"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:variable name="t" select="translate(normalize-space(.), $upper, $lower)"/>
                    <xsl:variable name="typeInfo">
                        <xsl:choose>
                            <xsl:when test="@t='date' or $isDate!=0">, createRxReqDateHandle()</xsl:when>
                            <xsl:when test="@t='bool' or (string(@t)='' and (substring($t,1,1)='y' or substring($t,1,1)='n' or $t='true' or $t='false' or $t='1'))">                    
                                <xsl:value-of 
                                select="concat(', createBoolHandleReq(', $quote, $trueValue, $quote, ', ', $quote, $falseValue, $quote, ')')"/>
                            </xsl:when>
                            <xsl:when test="@t='int' or (string(@t)='' and string(number($t))=$t)">, createIntHandleReq()</xsl:when>
                            <!--xsl:when test="$t!='' and substring('String', 1, string-length($t))"> /*enum*/</xsl:when-->
                        </xsl:choose>               
                    </xsl:variable>
                    <xsl:variable name="handleInfo">
                        <!--xsl:if test="*[1] and $group=0 and $isArray=0 and $createHandleFunction=0"-->
                        <xsl:if test="*[1] and $group=0 and $createHandleFunction=0">
                            <xsl:value-of select="$hName"/>
                        </xsl:if>
                    </xsl:variable>
                    <xsl:value-of select="concat($outName, $quote)"/>
                    <xsl:if test="$typeInfo!='' or $handleInfo!=''">, NULL</xsl:if>
                    <xsl:value-of select="concat($typeInfo, $handleInfo)"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:otherwise><!--request-->
    </xsl:choose>
    <xsl:value-of select="concat(' },', $eol)"/>
</xsl:template>

<xsl:template name="groupKnownElements">
    <xsl:if test="$response=1 or ..=/">
        <xsl:for-each select="*">
            <xsl:variable name="cname">
                <xsl:call-template name="getNodeName"/>
            </xsl:variable>
            <xsl:variable name="lcName" select="translate($cname, $upper_, $lower)"/>
            <xsl:variable name="lcdName" select="translate($lcName, $digits, '')"/>
            <xsl:choose>
                <xsl:when test="$response=1 and ($lcName='lastname' or $lcName='lname')">   PARENTSUB_HANDLE2(Name, NameH);
</xsl:when>
                <xsl:when test="$response=1 and ($lcdName='addr' or $lcdName='address' or $lcdName='streetaddress')">
                    <xsl:variable name="p" select="count(preceding::*[@name=$cname or name()=$cname] | ancestor::*[@name=$cname or name()=$cname])"/>
                    <xsl:variable name="address">Address<xsl:if test="$p"><xsl:value-of select="$p + 1"/></xsl:if></xsl:variable>
                    <xsl:value-of select="concat($tab, 'PARENTSUB_HANDLE2(Address, ', $address, 'H);', $eol)"/>
                </xsl:when>
                <!--xsl:when test="$lcName='dob'">  ESPstruct Date DOB;
</xsl:when-->
                <xsl:otherwise>
                    <xsl:variable name="numberSuffix">
                        <xsl:call-template name="getNumberSuffixForNode"/>
                    </xsl:variable>
                    <xsl:if test="$numberSuffix = '1'">
                        <xsl:variable name="baseName">
                            <xsl:variable name="outName">
                                <xsl:call-template name="getOutNameNode"/>
                            </xsl:variable>
                            <xsl:variable name="len" select="string-length($outName)"/>
                            <xsl:variable name="lsuf" select="string-length($numberSuffix)"/>
                            <xsl:value-of select="concat(substring($outName, 1, $len - $lsuf), $listSuffix)"/>
                        </xsl:variable>
                        <xsl:value-of select="concat($tab, 'PARENTSUB_HANDLE2(', $baseName, ', ', $baseName, 'H);', $eol)"/>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:for-each>
    </xsl:if>
</xsl:template>

<xsl:template name="outputRespHandleInfo">
<xsl:param name="hName"/>
<xsl:param name="outName"/>
<xsl:param name="isDate"/>
    <xsl:variable name="t" select="translate(normalize-space(.), $upper, $lower)"/>
    <xsl:choose>
        <xsl:when test="*[1]">
            <xsl:value-of select="$hName"/>
        </xsl:when>
        <xsl:when test="@t='date' or $isDate!=0">, createDateParser()</xsl:when>
        <xsl:when test="@t='bool' or (string(@t)='' and (substring($t,1,1)='y' or substring($t,1,1)='n' or $t='true' or $t='false' or $t='1'))">
            <xsl:value-of 
            select="concat(', createBoolHandle(', $quote, $trueValue, $quote, ', ', $quote, $falseValue, $quote, ')')"/>    
        </xsl:when>
        <xsl:when test="$outName='UniqueId'">, createDidHandle()</xsl:when>
        <xsl:when test="@t='int' or (string(@t)='' and string(number($t))=$t)">, createIntHandle(true)</xsl:when>
        <!--xsl:when test="$t!='' and $t!=substring('String', 1, string-length($t))"> /*enum*/</xsl:when-->
    </xsl:choose>
</xsl:template>

<xsl:template name="printFunctionHeader">
<xsl:param name="fnName"/>
<xsl:text disable-output-escaping="yes">
//======================================================================
// </xsl:text><xsl:value-of select="$fnName"/><xsl:text disable-output-escaping="yes">
//======================================================================
static QxHandle* </xsl:text><xsl:value-of select="concat($fnName, $eol, '{')"/>
</xsl:template>

<xsl:template match="@*"/>

</xsl:stylesheet>
