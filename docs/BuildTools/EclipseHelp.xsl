<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:doc="http://nwalsh.com/xsl/documentation/1.0"
                xmlns:exsl="http://exslt.org/common"
                xmlns:set="http://exslt.org/sets"
		version="1.0"
                exclude-result-prefixes="doc exsl set">

<!-- ********************************************************************
     $: EclipseHelp.xsl 1676 2013-09-23 16:20:PanaG :$
     ******************************************************************** 

     This file is used by EclipseHelp Intended to generate Eclispe Help.  
     It is based on the XSL DocBook Stylesheet distribution from Norman Walsh.
     Modified and customized for HPCC Systems by GPanagiotatos - 2013

     ******************************************************************** -->
<!-- Import docbook XSL and other resources from Jenk-Build locale -->
<xsl:import href="../docbook-xsl/eclipse/profile-eclipse.xsl"/>
<xsl:param name="img.src.path">../</xsl:param>
<xsl:param name="html.stylesheet">eclipsehelp.css</xsl:param>
<xsl:param name="chapter.autolabel" select="0" />
<xsl:param name="eclipse.plugin.id" select="ECLR.Eclipse.plugin" />
<xsl:param name="eclipse.plugin.name" select="ECLR.for.Eclipse" />
<xsl:param name="eclipse.plugin.provider" select="HPCC Systems" />
<xsl:param name="section.autolabel" select="0" />
<xsl:param name="variablelist.as.table" select="1" />
<xsl:param name="generate.toc">book toc</xsl:param>

</xsl:stylesheet>
