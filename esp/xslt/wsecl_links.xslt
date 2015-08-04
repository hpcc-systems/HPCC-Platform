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
<!DOCTYPE xsl:stylesheet [
    <!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="yes"/>
    <xsl:variable name="pathval" select="/links/path"/>
    <xsl:variable name="queryname" select="/links/query"/>
    <xsl:template match="/">
        <xsl:apply-templates select="links"/>
    </xsl:template>
    <xsl:template match="links">
        <h1>WsECL1 links:</h1>
        <ul>
            <li>
                <a href="/{$pathval}/{$queryname}?xsd">Xml Schema.</a>
            </li>
            <li>
                <a href="/{$pathval}/{$queryname}?wsdl">WSDL</a>
            </li>
            <li>
                <a href="/{$pathval}/{$queryname}?form">XSLT based form</a>
            </li>
        </ul>
        <xsl:if test="number(version)>=2">
            <br/>
            <h1>WsECL2 links:</h1>
            <ul>
                <li>
                    <a href="/WsEcl2/forms/query/{$pathval}/{$queryname}">Form</a>
                </li>
                <li>
                    <a href="/WsEcl2/example/request/{$pathval}/{$queryname}.xml">Sample Request</a>
                </li>
                <li>
                    <a href="/WsEcl2/example/response/{$pathval}/{$queryname}.xml">Sample Response</a>
                </li>
                <li>
                    <a href="/WsEcl2/definitions/commentblock/{$pathval}/{$queryname}/soap/{$queryname}.xml">ECL SOAP BLOCK</a>
                </li>
                <li>
                    <a href="/WsEcl2/definitions/query/{$pathval}/{$queryname}/main/{$queryname}.wsdl">Query WSDL -- WsEcl V2.0</a>
                </li>
                <li>
                    <a href="/WsEcl2/definitions/query/{$pathval}/{$queryname}/main/{$queryname}.xsd">Query XML Schema -- WsEcl V2.0.</a>
                </li>
                <xsl:for-each select="input_datasets/dataset">
                    <xsl:variable name="datasetname" select="name"/>
                    <li>
                        <a href="/WsEcl2/definitions/query/{$pathval}/{$queryname}/input/{$datasetname}.xsd">Input Xml Schema -- <xsl:value-of select="$datasetname"/>
                        </a>
                    </li>
                </xsl:for-each>
                <xsl:for-each select="result_datasets/dataset">
                    <xsl:variable name="datasetname" select="name"/>
                    <li>
                        <a href="/WsEcl2/definitions/query/{$pathval}/{$queryname}/result/{$datasetname}.xsd">Result Xml Schema -- <xsl:value-of select="$datasetname"/>
                        </a>
                    </li>
                </xsl:for-each>
            </ul>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
