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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <xsl:template match="/">
        <html>
            <head/>
            <body>
                <xsl:for-each select="//Dataset">
                    <xsl:for-each select="Row">
                        <xsl:if test="position()=1">
                            <xsl:text disable-output-escaping="yes">&lt;table border="1" cellspacing="0" &gt;</xsl:text>
                            <tr>
                                <xsl:for-each select="*">
                                    <th>
                                        <xsl:value-of select="name()"/>
                                    </th>
                                </xsl:for-each>
                            </tr>
                        </xsl:if>
                        <tr>
                            <xsl:for-each select="*">
                                <td align="center">
                                    <xsl:value-of select="."/>
                                </td>
                            </xsl:for-each>
                        </tr>
                    </xsl:for-each>
                    <xsl:if test="position()=last()">
                        <xsl:text disable-output-escaping="yes">&lt;/table&gt;</xsl:text>
                    </xsl:if>
                </xsl:for-each>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
