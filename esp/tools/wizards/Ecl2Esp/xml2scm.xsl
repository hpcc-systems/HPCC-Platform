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

<!--by default, this stylesheet translates response from 3rd party server.
    Set the following to 0 to generate request to be sent to 3rd party server-->
<xsl:param name="response" select="1"/>

<!--the following is suffix used for names of lists of elements like 'List' would group 
    Address elements under AddressList (corresponding to arrays in SCM) -->
<xsl:param name="listSuffix" select="'s'"/>

<xsl:include href="xml2common.xsl"/>


<xsl:variable name="ReqRespSuffix">
    <xsl:choose>
        <xsl:when test="$response=0">Request</xsl:when>
        <xsl:otherwise>Response</xsl:otherwise>
    </xsl:choose>
</xsl:variable>

<xsl:key name="k" match="*" use="concat(name(..), ../@name, name(), @name)"/>


<xsl:template match="/">
    <xsl:if test="$response=0">#include "wsm_share_esp.ipp"
</xsl:if>

//==================================================================================
// <xsl:value-of select="concat($method, $ReqRespSuffix)"/>
//==================================================================================
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
    <xsl:if test="$response=1">
//==================================================================================
// END
//==================================================================================
</xsl:if>
</xsl:template>

<xsl:template name="generateRequest">
    <xsl:apply-templates select="."/>
    <xsl:variable name="lcMethod" select="translate($method, $upper, $lower)"/>
    <xsl:text>
ESPstruct [nil_remove] </xsl:text>
    <xsl:value-of select="$method"/>Options : Base<xsl:choose>
        <xsl:when test="contains($lcMethod, 'search')">Search</xsl:when>
        <xsl:when test="contains($lcMethod, 'report')">Report</xsl:when>
    </xsl:choose><xsl:text>Option
{
</xsl:text>
    <xsl:for-each select="*">
        <xsl:variable name="name">
            <xsl:call-template name="getNodeName"/>         
        </xsl:variable>
        <xsl:variable name="n" select="translate($name, $upper, $lower)"/>
        <xsl:if test="$response=0 and substring($n, 1, 7)='include' and $n!='include_schemas_' and $n!='includealsofound'">
            <xsl:text>  bool </xsl:text>
            <xsl:call-template name="getOutNameNode"/>
            <xsl:text>;
</xsl:text>
        </xsl:if>
    </xsl:for-each>
    <xsl:if test="MaxResultsThisTime">  int ReturnCount(10);
</xsl:if>
    <xsl:if test="SkipRecords"> int StartingRecord(1);
</xsl:if>   
    <xsl:if test="PhoneticMatch">   bool UsePhonetics(false);
</xsl:if>   
    <xsl:if test="AllowNickNames">  bool UseNicknames(false);
</xsl:if>
    <xsl:if test="NoDeepDive">  bool IncludeAlsoFound(false);
</xsl:if>
    <xsl:if test="SelectIndividually">  bool SelectIndividually(false);
</xsl:if>
<xsl:text>};

ESPrequest [nil_remove] </xsl:text>
    <xsl:value-of select="$method"/><xsl:text>Request : BaseRequest
{
    ESPstruct </xsl:text><xsl:value-of select="$method"/><xsl:text>Options Options;
</xsl:text>
        <xsl:call-template name="mapEntry"/>
        <xsl:text>};
</xsl:text>
</xsl:template>


<xsl:template name="generateResponse">
    <xsl:apply-templates/>
    <xsl:text>
ESPstruct [nil_remove] </xsl:text>
    <xsl:value-of select="$method"/><xsl:text>Response
{
    ESPstruct ResponseHeader Header;
</xsl:text>
    <xsl:call-template name="mapEntries"/>
    <xsl:text>};

ESPresponse [encode(0), nil_remove] </xsl:text>
    <xsl:value-of select="$method"/>
    <xsl:text>ResponseEx
{
    [xsd_type("tns:</xsl:text>
    <xsl:value-of select="$method"/><xsl:text>Response")] string response;
};
</xsl:text>
</xsl:template>


<xsl:template match="*[*]">
    <xsl:variable name="name" select="name()"/>
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
        <xsl:apply-templates/>
        <xsl:variable name="firstChild" select="name(*[1])"/>
        <xsl:variable name="isArray" select="(*[2]) and not(*[name()!=$firstChild])"/>
        <xsl:variable name="outName">
            <xsl:variable name="lcName" select="translate($name, $upper, $lower)"/>
            <xsl:choose>
                <xsl:when test="$response=0 and count(.|$root)=1">
                    <xsl:value-of select="concat($method, 'By')"/>
                    <xsl:if test="$by='SearchBy'"> : BaseSearchBy</xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="getOutNameNode"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="($response=0 or count(.|$root)!=1) and (not($isArray) or (*[1]/*))">
ESPstruct [nil_remove] <xsl:value-of select="substring($outName, 1, string-length($outName) - string-length($numberSuffix))"/>
            <xsl:text> {
</xsl:text>
            <xsl:call-template name="mapEntries"/>
            <xsl:value-of select="concat('};', $eol)"/>
        </xsl:if>
    </xsl:if>
</xsl:template>

<xsl:template name="mapEntries">
    <xsl:call-template name="groupKnownElements"/>
    <xsl:variable name="this" select="."/>
    <xsl:for-each select="*">
        <xsl:variable name="skip">
            <xsl:call-template name="shouldSkipElement"/>
        </xsl:variable>
        <xsl:if test="$skip=0">
            <xsl:variable name="nodes2" select="key('k', concat(name(..), ../@name, name(), @name))"/>
            <xsl:variable name="snodes2" select="$nodes2[count(.. | $this) = 1]"/>  
            <xsl:variable name="firstSuchSibling" select="count(. | $snodes2[1]) = 1"/><!--first sibling with this name-->
            <xsl:if test="not($snodes2[2]) or $firstSuchSibling">
                <xsl:variable name="num_snodes2" select="count($snodes2)"/>
                <xsl:call-template name="mapEntry">
                    <xsl:with-param name="group" select="number($num_snodes2 &gt; 1 and (count(../*) &gt; $num_snodes2))"/>
                    <xsl:with-param name="occurs" select="$num_snodes2"/>
                </xsl:call-template>                                            
            </xsl:if>
        </xsl:if>
    </xsl:for-each>
</xsl:template>

<xsl:template name="mapEntry">
<xsl:param name="group" select="0"/>
<xsl:param name="occurs" select="1"/>
    <xsl:variable name="name" select="name()"/>
    <xsl:variable name="outName">
        <xsl:variable name="lcName1" select="translate($name, $upper, $lower)"/>
        <xsl:choose>
            <xsl:when test="$response=0 and count(.|$root)=1">
                <xsl:value-of select="concat($method, 'By')"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="getOutNameNode"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    <xsl:choose>
        <xsl:when test="($group!=0 or $occurs &gt; 1) and *[1]">
            <xsl:text>  ESParray&lt;ESPstruct </xsl:text>
            <xsl:value-of select="concat($outName, '> ', $outName, $listSuffix, ';', $eol)"/>
        </xsl:when>
        <xsl:when test="($group!=0 or $occurs &gt; 1)">
            <xsl:text>  ESParray&lt;string, </xsl:text>
            <xsl:value-of select="concat($outName, '> ', $outName, $listSuffix, ';', $eol)"/>
        </xsl:when>
        <xsl:when test="*[1]">
            <xsl:variable name="firstChildName" select="name(*[1])"/>
            <xsl:variable name="isArray" select="(*[2]) and not(*[name()!=$firstChildName])"/>
            <xsl:choose>
                <xsl:when test="$response=0 and count(.|$root)=1 and not($isArray)">
                    <xsl:value-of select="concat($tab, 'ESPstruct ', $outName, ' ', $by)"/>
                </xsl:when>
                <xsl:when test="$isArray">
                    <xsl:variable name="outChildName">
                        <xsl:for-each select="*[1]">
                            <xsl:call-template name="getOutNameNode"/>
                        </xsl:for-each>
                    </xsl:variable>
                    <xsl:variable name="numberSuffix">
                        <xsl:call-template name="getNumberSuffixForNode"/>
                    </xsl:variable>
                    <xsl:text>  ESParray&lt;</xsl:text>
                    <xsl:choose>
                        <xsl:when test="*[1]/*[1]">ESPstruct </xsl:when>
                        <xsl:otherwise>string </xsl:otherwise>
                    </xsl:choose>
                    <xsl:value-of 
                    select="concat($outChildName, '> ', substring($outName, 1, string-length($outName) - string-length($numberSuffix)))"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:text>  ESPstruct </xsl:text>
                    <xsl:variable name="numberSuffix">
                        <xsl:call-template name="getNumberSuffixForNode"/>
                    </xsl:variable>
                    <xsl:value-of 
                    select="concat(substring($outName, 1, string-length($outName) - string-length($numberSuffix)), ' ', $outName)"/>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:value-of select="concat(';', $eol)"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:variable name="lcName" select="translate($outName, $upper, $lower)"/>
            <xsl:variable name="isDate">
                <xsl:call-template name="isDate">
                    <xsl:with-param name="s" select="$name"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:variable name="isDateRange">
                <xsl:call-template name="endsWith">
                    <xsl:with-param name="s" select="$lcName"/>
                    <xsl:with-param name="suffix" select="'datebegin'"/>
                </xsl:call-template>
            </xsl:variable>         
            <xsl:variable name="numberSuffix">
                <xsl:call-template name="getNumberSuffixForNode"/>
            </xsl:variable>
            <xsl:variable name="t" select="translate(normalize-space(.), $upper, $lower)"/>
            <xsl:choose>
                <xsl:when test="$isDate!=0">
                    <xsl:text>  ESPstruct Date </xsl:text>
                    <xsl:value-of select="concat($outName, ';', $eol)"/>
                </xsl:when>
                <xsl:when test="$isDateRange!=0">
                    <xsl:text>  ESPstruct DateRange </xsl:text>
                    <xsl:value-of select="concat(substring($outName, 1, $isDateRange+3), ';', $eol)"/>
                </xsl:when>
                <xsl:when test="$numberSuffix!=''">
                    <xsl:if test="$numberSuffix = '1'">
                        <xsl:variable name="baseName">
                            <xsl:variable name="len" select="string-length($outName)"/>
                            <xsl:variable name="lsuf" select="string-length($numberSuffix)"/>
                            <xsl:value-of select="substring($outName, 1, $len - $lsuf)"/>
                        </xsl:variable>
                        <xsl:text>  ESParray&lt;string, </xsl:text>
                        <xsl:value-of select="concat($baseName, '> ', $baseName, $listSuffix, ';', $eol)"/>
                    </xsl:if>
                </xsl:when>
                <xsl:when test="@t='bool' or (string(@t)='' and (substring($t,1,1)='y' or substring($t,1,1)='n' or $t='true' or $t='false' or $t='1'))">
                    <xsl:text>  bool </xsl:text>
                    <xsl:value-of select="concat($outName, ';', $eol)"/>
                </xsl:when>
                <xsl:when test="@t='int' or (string(@t)='' and string(number($t))=$t)">
                    <xsl:text>  int </xsl:text>
                    <xsl:value-of select="concat($outName, ';', $eol)"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:text>  string </xsl:text>
                    <!--xsl:if test="$text!='String'">/*enum*/ </xsl:if-->
                    <xsl:choose>
                        <xsl:when test="$outName!=''">
                            <xsl:value-of select="$outName"/>
                        </xsl:when>
                        <xsl:otherwise>Value /*text()*/</xsl:otherwise>
                    </xsl:choose>
                    <xsl:value-of select="concat(';', $eol)"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="groupKnownElements">
    <xsl:choose>
        <xsl:when test="$response=1">
            <xsl:for-each select="*">
                <xsl:variable name="cname">
                    <xsl:call-template name="getNodeName"/>
                </xsl:variable>
                <xsl:variable name="n" select="translate($cname, $upper_1, $lower)"/>
                <xsl:choose>
                    <xsl:when test="$n='lastname' or $n='lname'">   ESPstruct Name Name;
</xsl:when>
                    <xsl:when test="$n='addr' or $n='address' or $n='streetaddress'">   ESPstruct Address Address;
</xsl:when>
                </xsl:choose>
            </xsl:for-each>
        </xsl:when>
        <xsl:when test="..=/"><!--request-->
            <xsl:choose>
                <xsl:when test="LastName">  ESPstruct Name Name;
</xsl:when>
                <xsl:when test="Addr">  ESPstruct Address Address;
</xsl:when>
            </xsl:choose>
        </xsl:when>
    </xsl:choose>
</xsl:template>


<xsl:template match="@*|*|text()"/>

</xsl:stylesheet>
