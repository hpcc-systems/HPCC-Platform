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
