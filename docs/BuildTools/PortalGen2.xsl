<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:doc="http://nwalsh.com/xsl/documentation/1.0"
                xmlns:exsl="http://exslt.org/common"
                xmlns:set="http://exslt.org/sets"
		            version="1.0"
                exclude-result-prefixes="doc exsl set">

<!-- ********************************************************************
     $: PortalGen.xsl :  2017-09-23 16:20:PanaG :$
     ******************************************************************** 

     This file is used to generate HTML Files intended for the Web Portal.   
     It is based on the (EclipseHelp) XSL DocBook 
     Stylesheet distribution from Norman Walsh.
     Modified and customized for HPCC Systems by GPanagiotatos - 2013,2017

     ******************************************************************** -->
<!-- Import docbook XSL and other resources from Jenk-Build locale -->
<xsl:import href="../resources/docbook-xsl/eclipse/profile-eclipse.xsl"/>
<xsl:param name="img.src.path">../</xsl:param>
<xsl:param name="html.stylesheet">LN-html.css</xsl:param>
<xsl:param name="use.id.as.filename" select="1" />
<xsl:param name="chapter.autolabel" select="0" />
<xsl:param name="suppress.navigation" select="1" />
<xsl:param name="section.autolabel" select="0" />
<xsl:param name="chunk.section.depth" select="2" />
<xsl:param name="toc.section.depth">2</xsl:param>
<xsl:param name="variablelist.as.table" select="1" />
<xsl:param name="generate.toc">book toc</xsl:param>

<!-- Custom HTML specific processing instruction rules -->
 <xsl:template match="processing-instruction('linebreak')">
   <BR/>
 </xsl:template>

<!-- Colourize ECL Code -->
<xsl:template name="user.head.content">
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/@hpcc-js/util"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/@hpcc-js/common"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/@hpcc-js/codemirror"></script>
  <script>
      var hpccCommon = window["@hpcc-js/common"];
      var hpccCodemirror = window["@hpcc-js/codemirror"];
  </script>
</xsl:template>
<xsl:template name="user.footer.content">
    <script>
        hpccCommon.select("body").selectAll("pre.programlisting")
            .each(function (d) {
                var bbox = this.getBoundingClientRect();

                var div = document.createElement("div");
                this.parentNode.insertBefore(div, this);

                var element = hpccCommon.select(this);
                var cm = new hpccCodemirror.ECLEditor()
                    .target(div)
                    .resize({ width: Math.max(800, bbox.width), height: bbox.height })
                    .text(element.text().trim())
                    .readOnly(true)
                    .render()
                    ;
                element.remove();
            })
            ;
    </script>
</xsl:template>

</xsl:stylesheet>
