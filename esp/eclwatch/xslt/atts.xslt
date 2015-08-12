<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="text" indent="no" media-type="application/x-javascript"/>


<xsl:template match="/">
function createPopups()
{
    var popups=new Object();
    <xsl:apply-templates/>
    return popups;
}

window.popups=createPopups();
</xsl:template>

<xsl:template match="Popup">
    popups["<xsl:value-of select="Id"/>"]=prop=new Object();
    <xsl:apply-templates/>
</xsl:template>

<xsl:template match="Att">
    prop["<xsl:value-of select="Name"/>"]="<xsl:value-of select="Value"/>";
</xsl:template>

<xsl:template match="Exception">
    window.status='Error: <xsl:value-of select="."/>';
</xsl:template>

<xsl:template match="text()|comment()"/>

</xsl:stylesheet>
