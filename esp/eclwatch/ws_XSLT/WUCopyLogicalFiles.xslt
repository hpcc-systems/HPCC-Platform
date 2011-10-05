<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" >
    <xsl:template match="/">
        <xsl:apply-templates select="WUCopyLogicalFilesResponse"/>
    </xsl:template>
   <xsl:template match="WUCopyLogicalFilesResponse">
       <xsl:variable name="wuid" select="Wuid"/>
      <html>
         <head>
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
         </head>
         <body class="yui-skin-sam">
            <h3>Copying Workunit Files</h3>
            <xsl:if test="ClusterFiles/Cluster/NotOnCluster">
                The following files are being copyied to the specified clusters:
                <xsl:for-each select="ClusterFiles/Cluster">
                    <br/><br/><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><b>Cluster: </b><xsl:value-of select="ClusterName"/><br/><br/>
                    <xsl:for-each select="NotOnCluster/WULogicalFileCopyInfo">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;</xsl:text><xsl:value-of select="LogicalName"/>
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;</xsl:text>
                            <xsl:if test="string(DfuCopyWuid)">
                                <a><xsl:attribute name="href">/FileSpray/GetDFUWorkunit?wuid=<xsl:value-of select="DfuCopyWuid"/></xsl:attribute><xsl:value-of select="DfuCopyWuid"/></a>
                            </xsl:if>
                        <xsl:value-of select="DfuCopyError"/>
                    </xsl:for-each>
                </xsl:for-each>
            </xsl:if>
            <br/>
            <form action="none">
                <input type="button" value="Back" onclick="history.back()"/>
            </form>
         </body>
      </html>
   </xsl:template>
</xsl:stylesheet>
