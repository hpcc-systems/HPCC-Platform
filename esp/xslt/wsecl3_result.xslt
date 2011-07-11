<?xml version="1.0" encoding="utf-8"?>
<!--

## Copyright (c) 2011 HPCC Systems.  All rights reserved.
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope">
    <xsl:output method="html"/>
    <xsl:param name="url" select="'unknown'"/>
    <xsl:param name="rowStart" select="0"/>
    <xsl:param name="rowCount"/>
    <xsl:param name="rowTotal"/>
    
    <xsl:include href="/esp/xslt/result_lib.xslt"/>
    
    <xsl:variable name="async" select="/child::node()/Async"/>
    <xsl:variable name="wuid" select="/child::node()/Workunit"/>
    <xsl:variable name="eclwatch" select="/child::node()/EclWatch"/>

    <xsl:template match="/">
        <xsl:choose>
            <xsl:when test="local-name()='Envelope'"><xsl:apply-templates mode="response_root" select="/soap:Envelope/soap:Body/*[1]"/></xsl:when>
            <xsl:otherwise><xsl:apply-templates mode="response_root" select="/"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="*" mode="response_root">
        <html>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
                <link REL="stylesheet" TYPE="text/css" HREF="/esp/files/default.css"/>
            </head>
            <body>
                <h3>
                    <xsl:call-template name="id2string">
                        <xsl:with-param name="toconvert" select="name(/*)"/>
                    </xsl:call-template>
                </h3>
                <xsl:if test="$async">
                    <b>Query was run asynchronusly</b>
                    <br/>
                </xsl:if>
                <xsl:if test="$wuid">
                    Workunit ID: 
                    <xsl:choose>
                        <xsl:when test="$eclwatch">
                            <xsl:choose>
                                <xsl:when test="starts-with($eclwatch, 'http')">
                                    <a href="{$eclwatch}/WsWorkunits/WUInfo?Wuid={$wuid}">
                                        <b>
                                            <xsl:value-of select="$wuid"/>
                                        </b>
                                    </a>
                                </xsl:when>
                                <xsl:otherwise>
                                    <a href="http://{$eclwatch}/WsWorkunits/WUInfo?Wuid={$wuid}">
                                        <b>
                                            <xsl:value-of select="$wuid"/>
                                        </b>
                                    </a>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <b>
                                <xsl:value-of select="$wuid"/>
                            </b>
                        </xsl:otherwise>
                    </xsl:choose>
                    <br/>
                    <br/>
                </xsl:if>
                <xsl:apply-templates select="/child::node()/Results"/>
            </body>
        </html>
    </xsl:template>
    
    <xsl:template match="Results">
        <xsl:apply-templates select="Exception"/>
        <xsl:apply-templates select="Result">
            <xsl:with-param name="wsecl" select="true()"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="Result/Tracing"/>
    </xsl:template>
    <xsl:template match="Exception">
        <b>Exception</b>
        <br/>
        Reported by: <xsl:value-of select="Source"/>
        <br/>
     Message: <xsl:value-of select="Message"/>
    </xsl:template>
       <xsl:template match="Log">
            <tr>
          <td><xsl:value-of select="Category"/></td>
          <td><xsl:value-of select="Time"/></td>
          <td><xsl:value-of select="substring-before(substring-after(Prefix, '{'),'}')"/></td>
          <td><xsl:value-of select="substring-after(Prefix, '@')"/></td>
          <td><xsl:value-of select="substring-before(Prefix, '{')"/></td>
          <td><xsl:value-of select="substring-before(Prefix, ' ch=')"/></td>
          <td><xsl:value-of select="Text"/></td>
            </tr>
        </xsl:template>
       <xsl:template match="Tracing">
        <html>
            <head>
                    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                    <link REL="stylesheet" TYPE="text/css" HREF="files/default.css"/>
            </head>
                <body>
                    <table border="1" frame="box" rules="all">
                            <tr>
                <th>Category</th>
                <th>Time</th>
                <th>Server Activity #</th>
                <th>Activity ID</th>
                <th>IP</th>
                <th>Channel</th>
                <th>Text</th>
              </tr>
                            <xsl:apply-templates select="Log[Category='TRACE']"/>
                            <xsl:apply-templates select="Log[Category='TIMING']"/>
                            <xsl:apply-templates select="Log[Category='ERROR']"/>
                            <xsl:apply-templates select="Log[Category='UNKNOWN']"/>
                    </table>
                </body>
         </html>
    </xsl:template>

</xsl:stylesheet>

