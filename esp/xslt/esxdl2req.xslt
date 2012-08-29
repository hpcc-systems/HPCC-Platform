<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
    <xsl:template match="SampleMessageResponse">
        <xsl:choose>
            <xsl:when test="Type='request'">
                <xsl:apply-templates select="message/ESXDL" mode="request"/>
            </xsl:when>
            <xsl:when test="Type='response'">
                <xsl:apply-templates select="message/ESXDL" mode="response"/>
            </xsl:when>
        </xsl:choose>
    </xsl:template>
    <xsl:template match="ESXDL" mode="request">
        <xsl:variable name="reqname" select="EsdlService/EsdlMethod/@request_type"/>
        <xsl:apply-templates select="EsdlRequest[@name=$reqname]"/>
    </xsl:template>
    <xsl:template match="ESXDL" mode="response">
        <xsl:variable name="respname" select="EsdlService/EsdlMethod/@response_type"/>
        <xsl:apply-templates select="EsdlResponse[@name=$respname]"/>
    </xsl:template>
    <xsl:template match="EsdlStruct" mode="content">
            <xsl:apply-templates select="*"/>
    </xsl:template>
    <xsl:template match="EsdlElement[@complex_type]">
            <xsl:element name="{@name}">
                    <xsl:variable name="complex_type" select="@complex_type"/>
                    <xsl:apply-templates mode="content" select="//EsdlStruct[@name=$complex_type]"/>
            </xsl:element>
    </xsl:template>
    <xsl:template match="EsdlElement[@complex_type='Date']">
            <xsl:element name="{@name}">
                <xsl:element name="Year">2000</xsl:element>
                <xsl:element name="Month">12</xsl:element>
                <xsl:element name="Day">31</xsl:element>
            </xsl:element>
    </xsl:template>
    <xsl:template match="EsdlElement[@type]">
            <xsl:element name="{@name}">
                <xsl:choose>
                    <xsl:when test="@default"><xsl:value-of select="@default"/></xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test="@type='string'">string</xsl:when>
                            <xsl:when test="@type='int'">1000</xsl:when>
                            <xsl:when test="@type='short'">100</xsl:when>
                            <xsl:when test="@type='bool'">1</xsl:when>
                            <xsl:otherwise>unkown type</xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:element>
    </xsl:template>
    <xsl:template match="EsdlArray">
        <xsl:element name="{@name}">
                    <xsl:variable name="complex_type" select="@type"/>
                    <xsl:element name="{@item_tag}">
                        <xsl:apply-templates mode="content" select="//EsdlStruct[@name=$complex_type]"/>
                    </xsl:element>
                    <xsl:element name="{@item_tag}">
                        <xsl:apply-templates mode="content" select="//EsdlStruct[@name=$complex_type]"/>
                    </xsl:element>
        </xsl:element>
    </xsl:template>
    <xsl:template match="EsdlRequest">
        <xsl:element name="{@name}">
                <xsl:if test="@parent">
                    <xsl:variable name="parent" select="@parent"/>
                    <xsl:apply-templates mode="content" select="../*[@name=$parent]"/>
                </xsl:if>
                <xsl:apply-templates select="." mode="content"/>
        </xsl:element>
    </xsl:template>
    <xsl:template match="EsdlRequest" mode="content">
                <xsl:apply-templates select="*"/>
    </xsl:template>

    <xsl:template match="EsdlResponse">
        <xsl:element name="{@name}">
                <xsl:if test="@parent">
                    <xsl:variable name="parent" select="@parent"/>
                    <xsl:apply-templates mode="content" select="../*[@name=$parent]"/>
                </xsl:if>
                <xsl:apply-templates select="." mode="content"/>
        </xsl:element>
    </xsl:template>
    <xsl:template match="EsdlResponse" mode="content">
                <xsl:apply-templates select="*"/>
    </xsl:template>
</xsl:stylesheet>
