<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
