
<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
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

<xsl:output method="text" indent="no" media-type="application/x-javascript"/>

<xsl:param name="fromDate" select="&from;"/>
<xsl:param name="toDate" select="&to;"/>

<xsl:template match="*[TimeStamps/TimeStamp[substring(@application,1,6)='Thor -']]">
    <xsl:variable name="wuid" select="name(.)"/>
    <xsl:variable name="cluster" select="@clusterName"/>
    <xsl:variable name="state" select="@state"/>
    <xsl:for-each select="TimeStamps/TimeStamp[Started and substring(@application,1,6)='Thor -' and $toDate>=translate(substring-before(Started,'T'),'-','')]">
        <xsl:variable name="graph" select="substring(@application,8)"/>
        <xsl:variable name="started" select="normalize-space(Started)"/>           
        <xsl:variable name="finished" select="normalize-space(following-sibling::TimeStamp[Finished and (@application=current()/@application or @application='EclAgent')][1])"/>
        <xsl:if test="string-length($finished) and (translate(substring-before($finished,'T'),'-','')>=$fromDate) ">
            parent.displayJob('<xsl:value-of select="$wuid"/>','<xsl:value-of select="$graph"/>','<xsl:value-of select="$started"/>','<xsl:value-of select="$finished"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$state"/>','sasha')
        </xsl:if>       
    </xsl:for-each>
</xsl:template>

<xsl:template match="text()|comment()"/>

</xsl:stylesheet>

