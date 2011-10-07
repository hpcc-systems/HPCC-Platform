<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" >
    <xsl:template match="/">
        <xsl:apply-templates select="WUDeployWorkunitResponse"/>
    </xsl:template>
   <xsl:template match="WUDeployWorkunitResponse">
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
            <h3>Workunit Published!</h3>
            <xsl:if test="ClusterFiles/Cluster/NotOnCluster">
                The following files are located on different clusters in this environment, to copy them to the specified cluster press the "Copy Files" button:
                <xsl:for-each select="ClusterFiles/Cluster">
                    <br/><br/><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text><b>Cluster: </b><xsl:value-of select="ClusterName"/><br/><br/>
                    <xsl:for-each select="NotOnCluster/WULogicalFileCopyInfo">
                        <xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;</xsl:text><xsl:value-of select="LogicalName"/><br/>
                    </xsl:for-each>
                </xsl:for-each>
            </xsl:if>
            <br/>
            <form action="WUCopyLogicalFiles" method="get">
                <input type="hidden" name="Wuid" value="{$wuid}"/>
                <input type="hidden" name="CopyLocal" value="1"/>
                <input type="submit" value="Copy Files"/><xsl:text disable-output-escaping="yes">&amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;</xsl:text>
                <input type="button" value="Back" onclick="history.back()"/>
            </form>
         </body>
      </html>
   </xsl:template>
</xsl:stylesheet>
