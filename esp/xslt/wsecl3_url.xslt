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
    <xsl:variable name="url" select="/resturl/url"/>
    <xsl:template match="/">
        <xsl:apply-templates select="resturl"/>
    </xsl:template>
    <xsl:template match="resturl">
        <head>
            <title>WsECL Query Sample Rest URL</title>
            <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
            <script>
                function doUpdateURL()
                {
                    var updateURL = document.getElementById('updateURL');
                    var hrefURL = document.getElementById('hrefURL');
                    hrefURL.innerHTML = updateURL.value;
                    hrefURL.setAttribute('href', updateURL.value);
                }
            </script>
        </head>
        <br/>
        <h1>WsECL REST URL</h1>
        <br/>
        <h2><xsl:value-of select="$target"/>&nbsp;/&nbsp;<xsl:value-of select="$queryname"/></h2><br/>
        <br/>
        Add parameter values to the following URL to execute the <xsl:value-of select="$queryname"/> query on <xsl:value-of select="$target"/>.
        <br/>
        <br/>
        <br/>
        REST URL Link:<br/>
        <br/>
        <a id="hrefURL" target="_blank" href="{$url}"><xsl:value-of select="$url"/></a>
        <br/>
        <br/>
        <br/>
        <form>
            Edit Link: <br/>
            <input type="text" id="updateURL" name="updateURL" value="{$url}" size="150"/><br/>
            <input type="button" name="Update" onClick="doUpdateURL()" value="Update"/>
        </form>
    </xsl:template>
</xsl:stylesheet>
