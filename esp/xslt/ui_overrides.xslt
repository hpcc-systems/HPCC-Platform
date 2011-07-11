<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="html" encoding="UTF-8"/>

<!--define line feed character-->
<xsl:variable name="LF"><xsl:text>
</xsl:text>
</xsl:variable>

<!--the following template matches a schema node -->
<xsl:template match="*" mode="override">
<xsl:param name="objNode"/>
<xsl:param name="columnHeader"/>
<xsl:param name="rowId"/>

    <xsl:variable name="name" select="name()"/>
    <xsl:if test="not($columnHeader)">
        <xsl:choose>
            <xsl:when test="$Command='DeployMultiple'">
                <xsl:choose>
                    <xsl:when test="$name='ImportAllModules' or $name='ImportImplicitModules'">
                        <xsl:variable name="pos" select="number(substring-after($rowId, 'Requests.Deploy.'))+1"/>
                        <xsl:variable name="depNode" 
                        select="$objNode/../.. | $objRootNode/Requests/Deploy[not($objNode)][$pos]"/>
                        <xsl:if test="starts-with($depNode/Info/SourceRepositoryType, 'R')">
                            <xsl:attribute name="disabled">true</xsl:attribute>
                        </xsl:if>
                    </xsl:when>
                    <xsl:when test="$name='input' and @name='submitBtnCheck'">
                        <xsl:variable name="req" select="$objRootNode/Requests"/>
                        <xsl:if test="string($objRootNode/Checkable)='0' or not($req/Deploy[1])">
                            <xsl:attribute name="disabled">true</xsl:attribute>
                        </xsl:if>
                        <xsl:attribute name="value">
                            <xsl:text>Check</xsl:text>
                            <xsl:if test="$req/Deploy[2]"> All</xsl:if>
                        </xsl:attribute>
                    </xsl:when>
                    <xsl:when test="$name='input' and @name='submitBtnDeploy'">
                        <xsl:variable name="req" select="$objRootNode/Requests"/>
                        <xsl:if test="(string($objRootNode/AllDeployable)='0' and string($objRootNode/Checkable)='1') or not($req/Deploy[1])">
                            <xsl:attribute name="disabled">true</xsl:attribute>
                        </xsl:if>
                        <xsl:attribute name="value">
                            <xsl:text>Deploy</xsl:text>
                            <xsl:if test="$req/Deploy[2]"> All</xsl:if>
                        </xsl:attribute>
                    </xsl:when>         
                </xsl:choose>   
            </xsl:when>
            <xsl:when test="$Command='ListSuperFilesUsedByQuery' and $name='Name'">
                <xsl:variable name="desc" select="$objNode/../Description"/>
                <xsl:if test="string($desc)!=''">
                    <br/>
                    <xsl:value-of select="$desc"/>
                </xsl:if>
            </xsl:when>         
        </xsl:choose>
    </xsl:if>
</xsl:template>

<!--the following template matches a schema node -->
<xsl:template match="*" mode="overrideCell">
<xsl:param name="objNode"/>
<xsl:param name="columnHeader"/>
<xsl:param name="rowId"/>

    <xsl:variable name="name" select="name()"/>
    <xsl:if test="not($columnHeader)">
        <xsl:choose>
            <xsl:when test="$name='highPriority' and $Command='DeployMultiple'">
                <xsl:variable name="pos" select="number(substring-after($rowId, 'Requests.Deploy'))+1"/>
                <xsl:variable name="depNode" 
                select="$objNode/../.. | $objRootNode/Requests/Deploy[not($objNode)][$pos]"/>
                <xsl:variable name="highPriority">
                    <xsl:choose>
                        <xsl:when test="$objNode">
                            <xsl:value-of select="$objNode"/>
                        </xsl:when>
                        <xsl:otherwise>1</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="wuHighPriority">
                    <xsl:choose>
                        <xsl:when test="$depNode/WorkunitHighPriority">
                            <xsl:value-of select="$depNode/WorkunitHighPriority"/>
                        </xsl:when>
                        <xsl:otherwise>1</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:if test="$wuHighPriority!=$highPriority">
                    <xsl:variable name="priority">
                        <xsl:choose>
                            <xsl:when test="$wuHighPriority='1'">High</xsl:when>
                            <xsl:otherwise>Low</xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <xsl:attribute name="style">background-color:orange</xsl:attribute>
                    <xsl:attribute name="onmouseover">EnterContent('ToolTip', null, '<xsl:value-of select="$priority"/> priority was specified by ECL!', true); Activate();</xsl:attribute>
                    <xsl:attribute name="onmouseout">deActivate()</xsl:attribute>
                </xsl:if>
            </xsl:when>
            
            <xsl:when test="$Command='SetupSuperFilesRemap'">
                <xsl:choose>
                    <xsl:when test="$name='RemapInfo'">
                        <xsl:variable name="outerRowId" select="substring-before($rowId, '.RemapInfo')"/>
                        <div style="text-align:left">
                            <b>&nbsp;
                                <a id="{$outerRowId}.Link" href="" onclick="return onShowHideSourceFileTable(this, '{$outerRowId}')">
                                    <img src="{$filePath}/img/folder.gif" style="vertical-align:text-bottom" border="0"/>
                                    <xsl:text>Source Files:</xsl:text>
                                </a>
                            </b>
                        </div>
                        <table id="{$outerRowId}.SourceFiles" class="left-aligned-table" style="display:none" width="800">
                            <thead>
                                <tr class="blue-bottom">
                                    <th width="25">
                                        &nbsp;
                                        <script type="text/javascript">
                                            var ms = ms_create('<xsl:value-of select="$outerRowId"/>.SourceFiles', onRowCheck ); ms.b_singleSelect = true;
                                        </script>
                                    </th>
                                    <th width="675">Description</th>
                                    <th width="100">Version</th>
                                </tr>
                            </thead>
                            <tbody>
                            </tbody>
                        </table>
                    </xsl:when>
                </xsl:choose>
            </xsl:when>
        </xsl:choose>   
    </xsl:if>
</xsl:template>

</xsl:stylesheet>
