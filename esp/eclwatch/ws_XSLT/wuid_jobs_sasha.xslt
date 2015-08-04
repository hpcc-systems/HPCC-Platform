
<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
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

