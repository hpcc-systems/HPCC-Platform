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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <xsl:template name="doForm">
    <xsl:text disable-output-escaping="yes">&lt;form name="input" method="get" action="</xsl:text>
    <xsl:value-of select="$url"/>
    <xsl:text disable-output-escaping="yes">" &gt;</xsl:text>
    <table bordercolor="#0066CC" border="1" cellspacing="0">
     <tr>
       <td>
         <table bgcolor="#DFEFFF">
    <xsl:for-each select="//part">
        <xsl:choose>
            <xsl:when test="@type='xsd:boolean'">
                 <tr>
                   <td>
                   </td>
                   <td>
                    <font face="Verdana" size="2">
                     <xsl:text disable-output-escaping="yes">&lt;input type="checkbox" name="</xsl:text>
                     <xsl:value-of select="@name"/>
                     <xsl:text disable-output-escaping="yes">" &gt;</xsl:text>
                     <xsl:call-template name='id2string'>
                        <xsl:with-param name='toconvert' select='@name' />
                     </xsl:call-template>
                     <xsl:if test="@required='1'">*</xsl:if>
                    </font>
                   </td>
                 </tr>
            </xsl:when>
            <xsl:otherwise>
                 <tr>
                   <td>
                    <font face="Verdana" size="2">
                     <xsl:call-template name='id2string'>
                        <xsl:with-param name='toconvert' select='@name' />
                     </xsl:call-template>
                     <xsl:text>:</xsl:text>
                    </font>
                   </td>
                   <td>
                    <font face="Verdana" size="2">
                     <xsl:text disable-output-escaping="yes">&lt;input type="text" name="</xsl:text>
                     <xsl:value-of select="@name"/>
                     <xsl:text disable-output-escaping="yes">" &gt;</xsl:text>
                     <xsl:if test="@required='1'">*</xsl:if>
                    </font>
                   </td>
                 </tr>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:for-each>
      </table>
       </td>
     </tr>
    </table>
    <br/>
    <br/>
    <input type="submit" value="Submit"/>
    <xsl:text disable-output-escaping="yes">&lt;/form&gt;</xsl:text>
    </xsl:template>
</xsl:stylesheet>