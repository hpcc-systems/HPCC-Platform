<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
    <xsl:param name="create_wsdl" select="false()"/>
    <xsl:param name="location" select="'http://localhost:8000/WsService?ver_=0'"/>
    <xsl:param name="tnsParam" select="/esxdl/@ns_uri"/>
    <xsl:param name="version" select="/esxdl/@version"/>
    <xsl:param name="no_annot_Param" select="false()"/>
    <xsl:param name="all_annot_Param" select="false()"/>

    <!--
        Note: This version of the stylesheet assumes that the XML input has been processed
        in the following ways:

        - All structures not explicitly referenced or used in another structure
          have been stripped out.
        - All ancestor structure's elements are collapsed into the defintion
          of their children.
        - In order for ArrayOf definitions to be generated, the esdl XML this processes
          must have the desired structures marked with 'arrayOf="1"' attributes.
    -->

    <xsl:template match="esxdl">
        <xsl:choose>
            <xsl:when test="$create_wsdl">
                <xsl:call-template name="CreateWsdl"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="CreateSchema"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="EsdlStruct">
        <xsd:complexType>
          <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
          <!--
            Only emit the xsd group element if our current struct has at least one child node.
          -->
          <xsl:if test="child::*[1]">
            <xsl:variable name="xsd_group">
              <xsl:choose>
                <xsl:when test="@xsd_group"><xsl:value-of select="@xsd_group"/></xsl:when>
                <xsl:otherwise>all</xsl:otherwise>
              </xsl:choose>
            </xsl:variable>
            <xsl:element name="xsd:{$xsd_group}">
              <xsl:apply-templates select="EsdlElement|EsdlArray|EsdlEnum"/>
            </xsl:element>
          </xsl:if>
        </xsd:complexType>
        <!--
            Under the original non-esdl schema/wsdl generation, when an EspArray is defined:

                ESParray<ESPstruct RedFlag> RedFlags;

            it causes the the output of a separate complexType named 'ArrayOfRedFlags'.
            Contrast that with an ESParray definition like:

                ESParray<ESPstruct RiskIndicator, HighRiskIndicator> HighRiskIndicators;

            which generates XSD output consisting of an unnamed complexType in place.
          We duplicate that behavior by adding an 'arrayOf' attribute to EsdlStruct
          definitions which are used in such ArrayOf definitions. In addition to the
          actual struct's defn output above, we output it's related ArrayOf defn below.
        -->
        <xsl:if test="@arrayOf='1'">
            <!--
                For whatever reason the current non-esdl based XSD/WSDL generator always
                generates an 'ArrayOfName' defn whenever an 'ArrayOfNameEx' is generated
                even if ArrayOfName is never used. Duplicate this behavior here but it's
                isolated to this one location so will be easy to remove if desired.
            -->
            <xsl:if test="@name='NameEx'">
                <xsd:complexType name="ArrayOfName">
                    <xsd:sequence>
                        <xsd:element minOccurs="0" maxOccurs="unbounded" name="Name" type="tns:Name"/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsl:if>
            <xsd:complexType>
                <xsl:attribute name="name">ArrayOf<xsl:value-of select="@name"/></xsl:attribute>
                <xsd:sequence>
                    <xsd:element minOccurs="0" maxOccurs="unbounded">
                        <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
                            <xsl:attribute name="type">tns:<xsl:value-of select="@name"/></xsl:attribute>
                    </xsd:element>
                </xsd:sequence>
            </xsd:complexType>
        </xsl:if>
        <!--
            This structure has been marked as the first to use an array EspStringArray
            so a separate EspStringArray structure definition must be generated.
        -->
        <xsl:if test="@espStringArray='1'">
            <xsd:complexType name="EspStringArray">
                <xsd:sequence>
                    <xsd:element name="Item" type="xsd:string" minOccurs="0" maxOccurs="unbounded"/>
                </xsd:sequence>
            </xsd:complexType>
        </xsl:if>
    </xsl:template>

    <xsl:template match="EsdlElement">
        <xsd:element>
            <xsl:if test="not(@required)">
                <xsl:attribute name="minOccurs">0</xsl:attribute>
            </xsl:if>
            <xsl:attribute name="name">
                <xsl:choose>
                    <xsl:when test="@xml_tag"><xsl:value-of select="@xml_tag"/></xsl:when>
                    <xsl:otherwise><xsl:value-of select="@name"/></xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:attribute name="type">
                <xsl:choose>
                    <xsl:when test="@xsd_type"><xsl:value-of select="@xsd_type"/></xsl:when>
                    <xsl:when test="@type='bool'">xsd:<xsl:value-of select="'boolean'"/></xsl:when>
                    <xsl:when test="@type='unsigned'">xsd:<xsl:value-of select="'unsignedInt'"/></xsl:when>
                    <xsl:when test="@type">xsd:<xsl:value-of select="@type"/></xsl:when>
                    <xsl:when test="@complex_type">tns:<xsl:value-of select="@complex_type"/></xsl:when>
                </xsl:choose>
            </xsl:attribute>
            <!--
                There are some .ecm files which use 'boolean' instead of 'bool', and for some reason those fields
                don't ever have a 'default' attribute when the XSD is generated. Reproduce that behavior here:
             -->
            <xsl:if test="@default and @type != 'boolean'">
                <xsl:attribute name="default">
                    <xsl:choose>
                        <xsl:when test="@type='bool' and @default='1'">true</xsl:when>
                        <xsl:when test="@type='bool' and @default='0'">false</xsl:when>
                        <!--
                                The Request defined in wsm_echotest.ecm has a default value which includes some
                                characters normally encoded (eg &amp;) in an XSDs attribute. The XSD currently
                                generated by the ESPs doesn't encode them. I've hardcoded the default value here
                                to duplicate that behavior. The XML generated by the esdl_def code encodes the
                                characters already encoded so they'd need a double-decoding here. This seemed the
                                simplest solution. Perhaps we need to revisit when and what the esdl_def toXML code
                                encodes and what it doesn't.
                         -->
                        <xsl:when test="@name='ValueIn' and ../@name='EchoTestRequest' and starts-with(./@default, 'Test')">
                            <xsl:text disable-output-escaping="yes">Test string: &lt;abc&gt; &amp; &lt;def&gt;</xsl:text>
                        </xsl:when>
                        <xsl:otherwise><xsl:value-of select="@default"/></xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </xsl:if>
            <xsl:if test="boolean($all_annot_Param)">
                <xsl:if test="@html_head or @form_ui or @collapsed or @cols or @rows or @optional">
                    <xsd:annotation>
                        <xsd:appinfo>
                            <form>
                                <xsl:if test="@form_ui">
                                    <xsl:attribute name="ui"><xsl:value-of disable-output-escaping="yes" select="@form_ui"/></xsl:attribute>
                                </xsl:if>
                                <xsl:if test="@html_head">
                                    <xsl:attribute name="html_head"><xsl:value-of disable-output-escaping="yes" select="@html_head"/></xsl:attribute>
                                </xsl:if>
                                <xsl:if test="@collapsed">
                                    <xsl:attribute name="collapsed">
                                        <xsl:choose>
                                            <xsl:when test="@collapsed=1">true</xsl:when>
                                            <xsl:otherwise>false</xsl:otherwise>
                                        </xsl:choose>
                                    </xsl:attribute>
                                </xsl:if>
                                <xsl:if test="@cols">
                                    <xsl:attribute name="formCols"><xsl:value-of select="@cols"/></xsl:attribute>
                                </xsl:if>
                                <xsl:if test="@rows">
                                    <xsl:attribute name="formRows"><xsl:value-of select="@rows"/></xsl:attribute>
                                </xsl:if>
                                <xsl:if test="@optional">
                                    <xsl:attribute name="optional"><xsl:value-of select="@optional"/></xsl:attribute>
                                </xsl:if>
                            </form>
                        </xsd:appinfo>
                    </xsd:annotation>
                    </xsl:if>
                </xsl:if>
        </xsd:element>
    </xsl:template>

    <xsl:template match="EsdlEnumItem" mode="annotation">
        <xsl:if test="@desc">
            <item>
                <xsl:attribute name="name"><xsl:value-of select="@enum"/></xsl:attribute>
                <xsl:attribute name="description"><xsl:value-of select="@desc"/></xsl:attribute>
            </item>
        </xsl:if>
    </xsl:template>

    <xsl:template match="EsdlEnumItem">
        <xsd:enumeration>
            <xsl:attribute name="value"><xsl:value-of select="@enum"/></xsl:attribute>
        </xsd:enumeration>
    </xsl:template>

    <xsl:template match="EsdlEnumType">
        <xsd:simpleType>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <!--
                EnumType has annotation only if it's EnumItems have @desc attributes.
                Check the first EnumItem to see if it has a @desc attribute
             -->
            <xsl:if test="(EsdlEnumItem[@desc]) and EsdlEnumItem/@desc!=''">
                <xsl:if test="not($no_annot_Param) or boolean($all_annot_Param)">
                    <xsd:annotation>
                        <xsd:appinfo>
                            <xsl:apply-templates select="EsdlEnumItem" mode="annotation" />
                        </xsd:appinfo>
                    </xsd:annotation>
                </xsl:if>
            </xsl:if>
            <xsd:restriction>
                <xsl:attribute name="base">xsd:<xsl:value-of select="@base_type"/></xsl:attribute>
                <xsl:apply-templates select="EsdlEnumItem"/>
            </xsd:restriction>
        </xsd:simpleType>
    </xsl:template>

    <xsl:template match="EsdlEnum">
        <xsd:element>
            <xsl:choose>
                <xsl:when test="@required"></xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="minOccurs">0</xsl:attribute>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:attribute name="name">
                <xsl:choose>
                    <xsl:when test="@xml_tag"><xsl:value-of select="@xml_tag" /></xsl:when>
                    <xsl:otherwise><xsl:value-of select="@name" /></xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:attribute name="type">
                <xsl:choose>
                    <xsl:when test="@xsd_type"><xsl:value-of select="@xsd_type" /></xsl:when>
                    <xsl:when test="@enum_type">tns:<xsl:value-of select="@enum_type" /></xsl:when>
                </xsl:choose>
            </xsl:attribute>
            <xsl:if test="@default or (@default='')">
                <xsl:attribute name="default"><xsl:value-of select="@default"/></xsl:attribute>
            </xsl:if>
        </xsd:element>
    </xsl:template>
    <xsl:template match="EsdlArray">
        <xsl:choose>
            <xsl:when test="boolean(@item_tag)">
                <xsd:element minOccurs="0">
                    <xsl:attribute name="name">
                        <xsl:choose>
                            <xsl:when test="@xml_tag"><xsl:value-of select="@xml_tag"/></xsl:when>
                            <xsl:otherwise><xsl:value-of select="@name"/></xsl:otherwise>
                        </xsl:choose>
                    </xsl:attribute>
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
            </xsl:when>
            <xsl:otherwise>
                <xsl:choose>
                    <xsl:when test="@type='string'">
                        <!--
                            Adjust the element definition so that it's type is tns:EspStringArray to
                            match the behavior of the current generator. Note that this doesn't generate
                            the actual EspStringArray defn, just a reference to where it's used.
                         -->
                        <xsd:element minOccurs="0">
                            <xsl:attribute name="name">
                                <xsl:choose>
                                    <xsl:when test="@xml_tag"><xsl:value-of select="@xml_tag"/></xsl:when>
                                    <xsl:otherwise><xsl:value-of select="@name"/></xsl:otherwise>
                                </xsl:choose>
                            </xsl:attribute>
                            <xsl:attribute name="type">tns:EspStringArray</xsl:attribute>
                        </xsd:element>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsd:element minOccurs="0">
                            <xsl:attribute name="name">
                                <xsl:choose>
                                    <xsl:when test="@xml_tag"><xsl:value-of select="@xml_tag"/></xsl:when>
                                    <xsl:otherwise><xsl:value-of select="@name"/></xsl:otherwise>
                                </xsl:choose>
                            </xsl:attribute>
                            <xsl:attribute name="type">tns:ArrayOf<xsl:value-of select="@type"/></xsl:attribute>
                        </xsd:element>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template match="EsdlList">
        <xsd:element minOccurs="0" maxOccurs="unbounded">
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
	    <xsl:attribute name="type">
            	<xsl:choose>
                    <xsl:when test="@type='string'">xsd:string</xsl:when>
                    <xsl:otherwise>tns:<xsl:value-of select="@type"/></xsl:otherwise>
            	</xsl:choose>
	    </xsl:attribute>
	</xsd:element>
    </xsl:template>
    <xsl:template match="EsdlRequest">
        <xsd:element>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsd:complexType>
                <xsd:all>
                    <xsl:apply-templates select="EsdlElement|EsdlArray|EsdlList|EsdlEnum"/>
                </xsd:all>
            </xsd:complexType>
        </xsd:element>
    </xsl:template>
    <xsl:template match="EsdlResponse">
        <xsd:element>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsd:complexType>
                <xsd:all>
                    <xsl:apply-templates select="EsdlElement|EsdlArray|EsdlList|EsdlEnum"/>
                </xsd:all>
            </xsd:complexType>
        </xsd:element>
    </xsl:template>
    <xsl:template match="EsdlService">
        <wsdl:message name="EspSoapFault">
            <wsdl:part name="parameters" element="tns:Exceptions"/>
        </wsdl:message>
        <xsl:for-each select="/esxdl/EsdlMethod">
            <wsdl:message>
                <xsl:attribute name="name"><xsl:value-of select="@name"/>SoapIn</xsl:attribute>
                <wsdl:part name="parameters">
                    <xsl:attribute name="element">tns:<xsl:value-of select="@request_type"/></xsl:attribute>
                </wsdl:part>
            </wsdl:message>
            <wsdl:message>
                <xsl:attribute name="name"><xsl:value-of select="@name"/>SoapOut</xsl:attribute>
                <wsdl:part name="parameters">
                    <xsl:attribute name="element">tns:<xsl:value-of select="@response_type"/></xsl:attribute>
                </wsdl:part>
            </wsdl:message>
        </xsl:for-each>
        <wsdl:portType>
            <xsl:attribute name="name"><xsl:value-of select="@name"/>ServiceSoap</xsl:attribute>
            <xsl:for-each select="/esxdl/EsdlMethod">
                <wsdl:operation>
                    <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
                    <wsdl:input>
                        <xsl:attribute name="message">tns:<xsl:value-of select="@name"/>SoapIn</xsl:attribute>
                    </wsdl:input>
                    <wsdl:output>
                        <xsl:attribute name="message">tns:<xsl:value-of select="@name"/>SoapOut</xsl:attribute>
                    </wsdl:output>
                    <wsdl:fault name="excfault" message="tns:EspSoapFault"/>
                </wsdl:operation>
            </xsl:for-each>
        </wsdl:portType>
        <wsdl:binding>
            <xsl:variable name="serviceName" select="@name"/>
            <xsl:attribute name="name"><xsl:value-of select="@name"/>ServiceSoap</xsl:attribute>
            <xsl:attribute name="type">tns:<xsl:value-of select="@name"/>ServiceSoap</xsl:attribute>
            <soap:binding transport="http://schemas.xmlsoap.org/soap/http" style="document"/>
            <xsl:for-each select="/esxdl/EsdlMethod">
                <wsdl:operation>
                    <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
                    <soap:operation style="document">
                        <xsl:attribute name="soapAction"><xsl:value-of select="$serviceName"/>/<xsl:value-of select="@name"/>?ver_=<xsl:value-of select="$version"/></xsl:attribute>
                    </soap:operation>
                    <wsdl:input>
                        <soap:body use="literal"/>
                    </wsdl:input>
                    <wsdl:output>
                        <soap:body use="literal"/>
                    </wsdl:output>
                    <wsdl:fault name="excfault">
                        <soap:fault name="excfault" use="literal"/>
                    </wsdl:fault>
                </wsdl:operation>
            </xsl:for-each>
        </wsdl:binding>
        <wsdl:service>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <wsdl:port>
                <xsl:attribute name="name"><xsl:value-of select="@name"/>ServiceSoap</xsl:attribute>
                <xsl:attribute name="binding">tns:<xsl:value-of select="@name"/>ServiceSoap</xsl:attribute>
                <soap:address location="{$location}"/>
            </wsdl:port>
        </wsdl:service>
    </xsl:template>
    <xsl:template name="CreateSchema">
        <xsl:param name="inwsdl" select="false()"/>
        <xsd:schema elementFormDefault="qualified" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <xsl:attribute name="targetNamespace"><xsl:value-of select="$tnsParam"/></xsl:attribute>
            <xsl:copy-of select="namespace::tns"/>

            <xsd:element name="string" nillable="true" type="xsd:string"/>
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
            <xsl:apply-templates select="EsdlEnumType" />
            <xsl:apply-templates select="EsdlStruct"/>
            <xsl:apply-templates select="EsdlRequest"/>
            <xsl:apply-templates select="EsdlResponse"/>

        </xsd:schema>
    </xsl:template>
    <xsl:template name="CreateWsdl">
        <wsdl:definitions xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:http="http://schemas.xmlsoap.org/wsdl/http/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/">
            <xsl:attribute name="targetNamespace"><xsl:value-of select="$tnsParam"/></xsl:attribute>
            <xsl:copy-of select="namespace::tns"/>
            <wsdl:types>
                <xsl:call-template name="CreateSchema">
                    <xsl:with-param name="inwsdl" select="true()"/>
                </xsl:call-template>
            </wsdl:types>
            <xsl:apply-templates select="EsdlService[1]"/>
        </wsdl:definitions>
    </xsl:template>
</xsl:stylesheet>
