<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
    <xsl:template match="ESXDL">
        <xsd:schema elementFormDefault="qualified" targetNamespace="http://webservices.seisint.com/WsAccurint" xmlns:tns="http://webservices.seisint.com/WsAccurint" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsd:element name="string" nillable="true" type="xsd:string" /> 
            <xsd:complexType name="EspException">
                <xsd:all>
                    <xsd:element name="Code" type="xsd:string" minOccurs="0"/>
                    <xsd:element name="Audience" type="xsd:string" minOccurs="0"/>
                    <xsd:element name="Source" type="xsd:string" minOccurs="0"/>
                    <xsd:element name="Message" type="xsd:string" minOccurs="0"/>
                </xsd:all>
            </xsd:complexType>
            <xsd:complexType name="ArrayOfEspException">
                <xsd:sequence>
                    <xsd:element name="Source" type="xsd:string" minOccurs="0"/>
                    <xsd:element name="Exception" type="tns:EspException" minOccurs="0" maxOccurs="unbounded"/>
                </xsd:sequence>
            </xsd:complexType>
            <xsd:element name="Exceptions" type="tns:ArrayOfEspException"/>
            <xsl:apply-templates select="EsdlStruct"/>
            <xsl:apply-templates select="EsdlRequest"/>
            <xsl:apply-templates select="EsdlResponse"/>
        </xsd:schema>
    </xsl:template>
    <xsl:template match="EsdlStruct">
        <xsd:complexType>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsd:all>
                <xsl:apply-templates select="EsdlElement|EsdlArray"/>
            </xsd:all>
        </xsd:complexType>
    </xsl:template>
    <xsl:template match="EsdlElement">
        <xsd:element minOccurs="0">
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsl:attribute name="type">
            <xsl:choose>
            <xsl:when test="@xsd_type"><xsl:value-of select="@xsd_type"/></xsl:when>
            <xsl:when test="@type='bool'">xsd:<xsl:value-of select="'boolean'"/></xsl:when>
            <xsl:when test="@type">xsd:<xsl:value-of select="@type"/></xsl:when>
            <xsl:when test="@complex_type">tns:<xsl:value-of select="@complex_type"/></xsl:when>
            </xsl:choose></xsl:attribute>
        </xsd:element>
    </xsl:template>
    <xsl:template match="EsdlArray">
        <xsd:element minOccurs="0">
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsd:complexType>
                <xsd:sequence>
                    <xsd:element minOccurs="0" maxOccurs="unbounded">
                        <xsl:attribute name="name"><xsl:value-of select="@item_tag"/></xsl:attribute>
                        <xsl:choose>
                            <xsl:when test="@type='string'">
                                <xsl:attribute name="type">xsd:string</xsl:attribute>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:attribute name="type">tns:<xsl:value-of select="@type"/></xsl:attribute>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsd:element>
                </xsd:sequence>
            </xsd:complexType>
        </xsd:element>
    </xsl:template>
    <xsl:template match="EsdlRequest">
        <xsd:element>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsd:complexType>
                <xsd:all>
                    <xsl:apply-templates select="EsdlElement"/>
                </xsd:all>
            </xsd:complexType>
        </xsd:element>
    </xsl:template>
    <xsl:template match="EsdlResponse">
        <xsd:element>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsd:complexType>
                <xsd:all>
                    <xsl:apply-templates select="EsdlElement"/>
                </xsd:all>
            </xsd:complexType>
        </xsd:element>
    </xsl:template>
</xsl:stylesheet>
