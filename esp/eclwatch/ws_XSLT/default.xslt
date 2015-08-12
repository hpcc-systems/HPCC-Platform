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