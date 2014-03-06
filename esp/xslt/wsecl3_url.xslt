<?xml version="1.0" encoding="UTF-8"?>
<!--

## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
-->
<!DOCTYPE xsl:stylesheet [
    <!ENTITY nbsp "&#160;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="yes"/>
    <xsl:variable name="target" select="/resturl/target"/>
    <xsl:variable name="queryname" select="/resturl/query"/>
    <xsl:variable name="urlParams" select="/resturl/urlParams"/>
    <xsl:template match="/">
        <xsl:apply-templates select="resturl"/>
    </xsl:template>
    <xsl:template match="resturl">
        <head>
            <title>WsECL Query Sample Rest URLs</title>
            <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
            <script>
                function doUpdateURL(content_type)
                {
                    var updateURL = document.getElementById('updateURL' + content_type);
                    var hrefURL = document.getElementById('hrefURL' + content_type);
                    hrefURL.innerHTML = updateURL.value;
                    hrefURL.setAttribute('href', updateURL.value);
                }
            </script>
        </head>
        <br/>
        <h1>WsECL REST URLs</h1>
        <br/>
        <h2><xsl:value-of select="$target"/>&nbsp;/&nbsp;<xsl:value-of select="$queryname"/></h2><br/>
        <br/>
        Add parameter values to the following URLs to execute the <xsl:value-of select="$queryname"/> query on <xsl:value-of select="$target"/>.
        <br/>
        <br/>
        XML REST URL Link:<br/>
        <br/>
        <a id="hrefURLXML" target="_blank" href="/WsEcl/submit/query/{$target}/{$queryname}/xml{$urlParams}">/WsEcl/submit/query/<xsl:value-of select="$target"/>/<xsl:value-of select="$queryname"/>/xml<xsl:value-of select="$urlParams"/></a>
        <br/>
        <br/>
        <form>
            Edit Link: <br/>
            <input type="text" id="updateURLXML" name="updateURLXML" value="/WsEcl/submit/query/{$target}/{$queryname}/xml{$urlParams}" size="150"/><br/>
            <input type="button" onClick="doUpdateURL('XML')" value="Update"/>
        </form>
        <br/>
        <br/>
        JSON REST URL Link:<br/>
        <br/>
        <a id="hrefURLJSON" target="_blank" href="/WsEcl/submit/query/{$target}/{$queryname}/json{$urlParams}">/WsEcl/submit/query/<xsl:value-of select="$target"/>/<xsl:value-of select="$queryname"/>/json<xsl:value-of select="$urlParams"/></a>
        <br/>
        <br/>
        <form>
            Edit Link: <br/>
            <input type="text" id="updateURLJSON" value="/WsEcl/submit/query/{$target}/{$queryname}/json{$urlParams}" size="150"/><br/>
            <input type="button" onClick="doUpdateURL('JSON')" value="Update"/>
        </form>
    </xsl:template>
</xsl:stylesheet>
