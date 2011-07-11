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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
<xsl:param name="responseDataset" select="'Results'"/>
<xsl:param name="customNamesFile" select="'customNames.xml'"/>
<xsl:variable name="apos">'</xsl:variable>
<xsl:variable name="quote">"</xsl:variable>
<xsl:variable name="tab">
    <xsl:text>  </xsl:text>
</xsl:variable>
<xsl:variable name="digits" select="0123456789"/>
<xsl:variable name="lower">abcdefghijklmnopqrstuvwxyz</xsl:variable>
<xsl:variable name="upper">ABCDEFGHIJKLMNOPQRSTUVWXYZ</xsl:variable>
<xsl:variable name="upper_" select="concat($upper, '_')"/>
<xsl:variable name="upper_1" select="concat($upper_, '1')"/>
<xsl:variable name="upper_d" select="concat($upper_, $digits)"/>
<xsl:variable name="eol"><xsl:text>
</xsl:text>
</xsl:variable>

<xsl:variable name="root" select="/*[$response=0] | /*[$response=1]/Dataset[@name=$responseDataset]"/>

<xsl:variable name="by">
    <xsl:variable name="lcMethod" select="translate($method, $upper, $lower)"/>
    <xsl:choose>
        <xsl:when test="contains($lcMethod, 'search')">SearchBy</xsl:when>
        <xsl:when test="contains($lcMethod, 'report')">ReportBy</xsl:when>
        <xsl:otherwise>By</xsl:otherwise>
    </xsl:choose>
</xsl:variable>

<xsl:variable name="customNames" select="document($customNamesFile)/CustomNameTranslation"/>
<xsl:variable name="validateCustomNames">
    <xsl:if test="not($customNames) and $customNamesFile != ''">
        <xsl:message terminate="yes">File <xsl:value-of select="$serviceFileName"/> was not found or is invalid!</xsl:message>
    </xsl:if>
</xsl:variable>

<xsl:template name="shouldSkipElement">
<xsl:param name="generatingScm" select="1"/>
    <xsl:choose>
        <xsl:when test="$response=1 or ../..=/">
            <xsl:variable name="name">
                <xsl:call-template name="getNodeName"/>
            </xsl:variable>
            <xsl:variable name="n" select="translate($name, $upper, $lower)"/>
            <xsl:variable name="isFlag">
                <xsl:call-template name="endsWith">
                    <xsl:with-param name="s" select="$n"/>
                    <xsl:with-param name="suffix" select="'flag'"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:variable name="isDateEnd">
                <xsl:call-template name="endsWith">
                    <xsl:with-param name="s" select="$n"/>
                    <xsl:with-param name="suffix" select="'dateend'"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:variable name="siblingNames">
                <xsl:text> </xsl:text>
                <xsl:for-each select="../*">
                    <xsl:value-of select="concat(name(), ' ')"/>
                </xsl:for-each>
            </xsl:variable>
            <xsl:variable name="lcSiblingNames" select="translate($siblingNames, $upper_, $lower)"/>
            <xsl:variable name="isNamePart">
                <xsl:choose>
                    <xsl:when test="$response=0 and ../LastName">
                            <xsl:choose>
                                <xsl:when test="$name='FirstName' or $name='MiddleName' or $name='LastName'">1</xsl:when>
                                <xsl:when test="$name='UnParsedFullName' or $name='NamePrefix' or $name='NameSuffix'">1</xsl:when>
                            </xsl:choose>
                    </xsl:when>
                    <xsl:when test="$response=1">
                        <xsl:variable name="hasLastNameSibling">
                            <xsl:call-template name="searchMultiplePatterns">
                                <xsl:with-param name="s" select="$lcSiblingNames"/>
                                <xsl:with-param name="patterns" select="'lname|lastname'"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:if test="$hasLastNameSibling!=0">
                            <xsl:variable name="m" select="translate($n, '_1', '')"/>
                            <xsl:choose>
                                <xsl:when test="$m='firstname' or $m='middlename' or $m='lastname'">1</xsl:when>
                                <xsl:when test="$m='fname' or $m='mname' or $m='lname'">1</xsl:when>
                                <xsl:when test="$m='unparsedfullname' or $m='name' or $m='title'">1</xsl:when>
                                <xsl:when test="$m='namesuffix'">1</xsl:when>
                            </xsl:choose>
                        </xsl:if>
                    </xsl:when>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="isAddressPart">
                <xsl:choose>
                    <xsl:when test="$response=0 and ../Addr">
                        <xsl:choose>
                            <xsl:when test="$name='PrimRange' or $name='PreDir' or $name='PrimName' or $name='Suffix'">1</xsl:when>
                            <xsl:when test="$name='PostDir' or $name='UnitDesig' or $name='SecRange' or $name='City'">1</xsl:when>
                            <xsl:when test="$name='State' or $name='Zip' or $name='Zip4' or $name='Addr'">1</xsl:when>
                            <xsl:when test="$name='Addr2' or $name='StateCityZip' or $name='PostalCode'">1</xsl:when>
                            <xsl:when test="$name='county-name'">1</xsl:when>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:when test="$response=1">
                        <xsl:variable name="hasAddressSibling">
                            <xsl:variable name="lc1SiblingNames" select="translate($lcSiblingNames, '1', '')"/>
                            <xsl:call-template name="searchMultiplePatterns">
                                <xsl:with-param name="s" select="$lc1SiblingNames"/>
                                <xsl:with-param name="patterns" select="'addr|address|streetaddress'"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:if test="$hasAddressSibling">
                            <xsl:variable name="m" select="translate($n, '_1', '')"/>
                            <xsl:choose>
                                <xsl:when test="$m='addr' or $m='address' or $m='streetaddress'">1</xsl:when>
                                <xsl:when test="$m='city' or $m='state' or $m='csz'">1</xsl:when>
                                <xsl:when test="$m='suffix' or $m='addrsuffix' or $m='zip' or $m='zip5' or $m='zip4'">1</xsl:when>
                                <xsl:when test="$m='primrange' or $m='primname'">1</xsl:when>
                                <xsl:when test="$m='predir' or $m='postdir' or $m='unitdesig'">1</xsl:when>
                                <xsl:when test="$m='secrange' or $m='pcityname'">1</xsl:when>
                                <xsl:when test="$m='vcityname' or $m='st' or $m='county' or $m='countyname'">1</xsl:when>
                            </xsl:choose>
                        </xsl:if>
                    </xsl:when>
                </xsl:choose>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="$isNamePart='1' or $isAddressPart='1'">1</xsl:when>
                <xsl:when test="$n='ssnmask' or $n='dlmask'">1</xsl:when>
                <xsl:when test="$n='dppapurpose' or $n='glbpurpose'">1</xsl:when>
                <xsl:when test="$n='maxresultsthistime' or ($n='isdeepdive' and $generatingScm=0)">1</xsl:when>
                <xsl:when test="$n='allownicknames' or $n='phoneticmatch'">1</xsl:when>
                <xsl:when test="$n='include_schemas_' or $n='cluster_' or $n='queue_'">1</xsl:when>
                <xsl:when test="$n='skiprecords' or $n='limitresults_' or $n='nodeepdive'">1</xsl:when>
                <xsl:when test="$n='penalt' or $n='penaltthreshold'">1</xsl:when>
                <xsl:when test="$n='selectindividually' or $n='datarestrictionmask'">1</xsl:when>
                <xsl:when test="../*[name()=concat($n, '_name')]">1</xsl:when>
                <xsl:when test="$isDateEnd!=0">1</xsl:when>
                <xsl:when test="$isFlag!=0">
                    <xsl:variable name="baseName" select="concat(substring($n, 1, $isFlag - 1), 'name')"/>
                    <xsl:choose>
                        <xsl:when test="../*[name()=$baseName]">1</xsl:when>
                        <xsl:otherwise>0</xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:when test="substring($n, 1, 7)='include'">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:when>
        <xsl:otherwise>0</xsl:otherwise>
    </xsl:choose>
</xsl:template>


<xsl:template name="searchMultiplePatterns">
<xsl:param name="s"/><!--for instance, " a b c d e".  Note the space in beginning-->
<xsl:param name="patterns"/><!--for instance, "b|c|d"-->
    <xsl:choose>
        <xsl:when test="contains($patterns, '|')"><!--looing for one of many to find-->
            <xsl:choose>
                <xsl:when test="contains($s, concat(' ', substring-before($patterns, '|'), ' '))">1</xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="searchMultiplePatterns">
                        <xsl:with-param name="s" select="$s"/>
                        <xsl:with-param name="patterns" select="substring-after($patterns, '|')"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:when>
        <xsl:when test="contains($s, concat(' ', $patterns, ' '))">1</xsl:when>
        <xsl:otherwise>0</xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="_getOutNameChar">
<xsl:param name="inName"/>
<xsl:param name="pos" select="0"/>
<xsl:param name="prevCharWas_" select="0"/>
<xsl:param name="hasLower" select="number(translate($inName, $lower, $upper)!=$inName)"/>
    <xsl:if test="$inName!=''">
        <xsl:variable name="char" select="substring($inName, 1, 1)"/>
        <xsl:choose>
            <xsl:when test="$char='_' or $char='.'"/>
            <xsl:when test="$char=':'">_</xsl:when>
            <xsl:when test="$pos=0 or $prevCharWas_">
                <xsl:value-of select="translate($char, $lower, $upper)"/>
            </xsl:when>
            <xsl:when test="not($hasLower)">
                <xsl:value-of select="translate($char, $upper, $lower)"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$char"/>
            </xsl:otherwise>
        </xsl:choose>
        <xsl:variable name="isLower" select="$char!='_' and translate($char, $lower, '') = ''"/>
        <xsl:call-template name="_getOutNameChar">
            <xsl:with-param name="inName" select="substring($inName, 2)"/>
            <xsl:with-param name="pos" select="$pos+1"/>
            <xsl:with-param name="prevCharWas_" select="translate($char, $lower, '')=$char and translate($char, $upper, '')=$char"/>
            <xsl:with-param name="hasLower" select="$hasLower or $isLower"/>
        </xsl:call-template>
    </xsl:if>
</xsl:template>

<xsl:template name="getOutName">
<xsl:param name="inName"/>
    <xsl:if test="$inName!=''">
        <xsl:variable name="outName">
            <xsl:variable name="lcInName" select="translate($inName, $upper, $lower)"/>
            <xsl:variable name="ucInName" select="translate($inName, $lower, $upper)"/>
            <xsl:choose>
                <xsl:when test="$inName=$ucInName"><!--name with all caps is probably an acronym so use as is-->
                    <xsl:value-of select="$inName"/>
                </xsl:when>
                <xsl:when test="$lcInName='ssn'">SSN</xsl:when>         
                <xsl:when test="$lcInName='did'">UniqueId</xsl:when>
                <xsl:when test="$lcInName='bdid'">BDID</xsl:when>
                <xsl:when test="$lcInName='dob'">DOB</xsl:when>
                <xsl:when test="$lcInName='driverslicense' or $lcInName='dl_number'">DriverLicenseNumber</xsl:when>
                <xsl:when test="$lcInName='isdeepdive'">AlsoFound</xsl:when>
                <xsl:when test="$lcInName='row'">
                    <xsl:variable name="parentName">
                        <xsl:for-each select="..">
                            <xsl:call-template name="getOutNameNode"/>
                        </xsl:for-each>
                    </xsl:variable>
                    <xsl:choose>
                        <xsl:when test="$parentName = concat($method, 'Records')">
                            <xsl:value-of select="$method"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$parentName"/>
                        </xsl:otherwise>
                    </xsl:choose>
                    <xsl:text>Record</xsl:text>
                </xsl:when>
                <xsl:when test="$lcInName='dataset' and count(.|$root)=1">
                    <xsl:value-of select="concat($method, 'Records')"/>
                </xsl:when>
                <xsl:when test="contains($inName, '.')">
                    <xsl:call-template name="_getOutNameChar">
                        <xsl:with-param name="inName" select="substring-after($inName, '.')"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:variable name="endsWith_name">
                        <xsl:call-template name="endsWith">
                            <xsl:with-param name="s" select="$lcInName"/>
                            <xsl:with-param name="suffix" select="'_name'"/>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:variable name="endsWith_dt">
                        <xsl:call-template name="endsWith">
                            <xsl:with-param name="s" select="$lcInName"/>
                            <xsl:with-param name="suffix" select="'_dt'"/>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:choose>
                        <xsl:when test="$endsWith_name!=0">
                            <xsl:call-template name="_getOutNameChar">
                                <xsl:with-param name="inName" select="substring($inName, 1, $endsWith_name - 1)"/>
                            </xsl:call-template>
                        </xsl:when>
                        <xsl:when test="$endsWith_dt!=0">
                            <xsl:call-template name="_getOutNameChar">
                                <xsl:with-param name="inName" select="concat(substring($inName, 1, $endsWith_dt - 1), 'Date')"/>
                            </xsl:call-template>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:call-template name="_getOutNameChar">
                                <xsl:with-param name="inName" select="$inName"/>
                            </xsl:call-template>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="outName2">
            <xsl:choose>
                <xsl:when test="contains($outName, 'name')">
                    <xsl:value-of select="concat(substring-before($outName, 'name'), 'Name')"/>
                    <xsl:value-of select="substring-after($outName, 'name')"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$outName"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="$customNames">
                <xsl:call-template name="customNameTranslation">
                    <xsl:with-param name="s" select="$outName2"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$outName2"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:if>
</xsl:template>

<xsl:template name="getNodeName">
    <xsl:choose>
        <xsl:when test="string(@a)!=''">
            <xsl:value-of select="@a"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="name()"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="getOutNameNode">
    <xsl:call-template name="getOutName">
        <xsl:with-param name="inName">
            <xsl:call-template name="getNodeName"/>
        </xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template name="endsWith">
<xsl:param name="s"/>
<xsl:param name="suffix"/>
    <xsl:variable name="len" select="string-length($s)"/>
    <xsl:variable name="l" select="string-length($suffix)"/>
    <xsl:variable name="pos" select="$len - $l + 1"/>
    <xsl:choose>
        <xsl:when test="substring($s, $pos)=$suffix">
            <xsl:value-of select="$pos"/>
        </xsl:when>
        <xsl:otherwise>0</xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="getNumberSuffix">
<xsl:param name="s"/>
    <xsl:variable name="len" select="string-length($s)"/>
    <xsl:if test="$len!=0">
        <xsl:variable name="numbers" select="'0123456789'"/>
        <xsl:variable name="lastChar" select="substring($s, $len)"/>
        <xsl:if test="translate($lastChar, $numbers, '')=''">
            <xsl:call-template name="getNumberSuffix">
                <xsl:with-param name="s" select="substring($s, 1, $len - 1)"/>
            </xsl:call-template>
            <xsl:value-of select="$lastChar"/>
        </xsl:if>
    </xsl:if>
</xsl:template>

<xsl:template name="getNumberSuffixForNode">
    <xsl:variable name="numberSuffix">
        <xsl:call-template name="getNumberSuffix">
            <xsl:with-param name="s" select="name()"/>
        </xsl:call-template>
    </xsl:variable>
    <xsl:if test="$numberSuffix!=''">
        <xsl:variable name="baseName">
            <xsl:variable name="len" select="string-length(name())"/>
            <xsl:variable name="lsuf" select="string-length($numberSuffix)"/>
            <xsl:value-of select="substring(name(), 1, $len - $lsuf)"/>
        </xsl:variable>     
        <xsl:choose>
            <xsl:when test="$numberSuffix!='1' and ../*[name()=concat($baseName, '1')]">
                <xsl:value-of select="$numberSuffix"/>
            </xsl:when>
            <xsl:when test="$numberSuffix='1' and ../*[name()=concat($baseName, '2')]">
                <xsl:value-of select="$numberSuffix"/>
            </xsl:when>
        </xsl:choose>
    </xsl:if>
</xsl:template>


<xsl:template name="isDate">
<xsl:param name="s"/>
    <xsl:variable name="lcName" select="translate($s, $upper, $lower)"/>
    <xsl:choose>
        <xsl:when test="$lcName='dob'">1</xsl:when>
        <xsl:when test="starts-with($lcName, 'date')">1</xsl:when>
        <xsl:when test="contains($lcName, '_date_') and starts-with(text(), '[')">1</xsl:when>
        <xsl:otherwise>
            <xsl:variable name="endsWith_date">
                <xsl:call-template name="endsWith">
                    <xsl:with-param name="s" select="$lcName"/>
                    <xsl:with-param name="suffix" select="'date'"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:variable name="endsWith_dt">
                <xsl:call-template name="endsWith">
                    <xsl:with-param name="s" select="$lcName"/>
                    <xsl:with-param name="suffix" select="'dt'"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="$endsWith_date!=0 or $endsWith_dt!=0">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="customNameTranslation">
<xsl:param name="s"/>
    <xsl:variable name="def" select="$customNames/global/*[name()=$s]"/>
    <xsl:choose>
        <xsl:when test="$def">
            <xsl:value-of select="$def"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="$s"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

</xsl:stylesheet>
