<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
