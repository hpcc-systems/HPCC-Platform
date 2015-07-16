<?xml version="1.0" encoding="UTF-8"?>
<!--

## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
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
        <head>
            <title>WsECL Service Links</title>
            <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
        </head>

            <br/>
            <br/>
            <h1><xsl:value-of select="$pathval"/>&nbsp;/&nbsp;<xsl:value-of select="$queryname"/>&nbsp;&nbsp; WsECL links:</h1>
            <ul>
                <li>
                    <a  target="_blank" href="/WsEcl/forms/ecl/query/{$pathval}/{$queryname}">Form</a>
                </li>
                <li>
                    Sample REST URL:&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/example/url/query/{$pathval}/{$queryname}">link</a>
                </li>
                <li>
                    Sample SOAP Request:&nbsp;&nbsp;<a href="/WsEcl/example/request/query/{$pathval}/{$queryname}?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/example/request/query/{$pathval}/{$queryname}">link</a>
                </li>
                <li>
                    Sample SOAP Response:&nbsp;&nbsp;<a href="/WsEcl/example/response/query/{$pathval}/{$queryname}?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/example/response/query/{$pathval}/{$queryname}">link</a>
                </li>
                <li>
                    Sample JSON Request:&nbsp;&nbsp;<a href="/WsEcl/example/request/query/{$pathval}/{$queryname}/json?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/example/request/query/{$pathval}/{$queryname}/json">link</a>
                </li>
                <li>
                    Sample JSON Response:&nbsp;&nbsp;<a href="/WsEcl/example/response/query/{$pathval}/{$queryname}/json?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/example/response/query/{$pathval}/{$queryname}/json">link</a>
                </li>
                <li>
                    Sample JSONP Response:&nbsp;&nbsp;<a href="/WsEcl/example/response/query/{$pathval}/{$queryname}/json?display&amp;jsonp=methodname">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/example/response/query/{$pathval}/{$queryname}/json?jsonp=methodname">link</a>
                </li>
                <li>
                    Parameter XML:&nbsp;&nbsp;<a href="/WsEcl/definitions/query/{$pathval}/{$queryname}/resource/soap/{$queryname}.xml?display">display</a>
                </li>
                <li>
                    SOAP (Post SOAP messages to this URL):&nbsp;&nbsp;<a href="/WsEcl/soap/query/{$pathval}/{$queryname}">/WsEcl/soap/query/<xsl:value-of select="$pathval"/>/<xsl:value-of select="$queryname"/></a>
                </li>
                <li>
                    JSON (Post JSON messages to this URL):&nbsp;&nbsp;<a href="/WsEcl/json/query/{$pathval}/{$queryname}">/WsEcl/json/query/<xsl:value-of select="$pathval"/>/<xsl:value-of select="$queryname"/></a>
                </li>
                <li>
                    WSDL:&nbsp;&nbsp;<a href="/WsEcl/definitions/query/{$pathval}/{$queryname}/main/{$queryname}.wsdl?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/definitions/query/{$pathval}/{$queryname}/main/{$queryname}.wsdl">link</a>
                </li>
                <li>
                    XML SCHEMA:&nbsp;&nbsp;<a href="/WsEcl/definitions/query/{$pathval}/{$queryname}/main/{$queryname}.xsd?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/definitions/query/{$pathval}/{$queryname}/main/{$queryname}.xsd">link</a>
                </li>
                <xsl:for-each select="input_datasets/dataset">
                    <xsl:variable name="datasetname" select="name"/>
                    <li>
                        Input Xml Schema -- <xsl:value-of select="$datasetname"/>:&nbsp;&nbsp;<a href="/WsEcl/definitions/query/{$pathval}/{$queryname}/input/{$datasetname}.xsd?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/definitions/query/{$pathval}/{$queryname}/input/{$datasetname}.xsd">link</a>
                    </li>
                </xsl:for-each>
                <xsl:for-each select="result_datasets/dataset">
                    <xsl:variable name="datasetname" select="name"/>
                    <li>
                        Result Xml Schema -- <xsl:value-of select="$datasetname"/>:&nbsp;&nbsp;<a href="/WsEcl/definitions/query/{$pathval}/{$queryname}/result/{$datasetname}.xsd?display">display</a>&nbsp;&nbsp;<a  target="_blank" href="/WsEcl/definitions/query/{$pathval}/{$queryname}/result/{$datasetname}.xsd">link</a>
                    </li>
                </xsl:for-each>
            </ul>
    </xsl:template>
</xsl:stylesheet>
