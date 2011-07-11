<?xml version="1.0" encoding="UTF-8"?>
<!--
## Copyright © 2011 HPCC Systems.  All rights reserved.
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format" xml:space="default">
<xsl:strip-space elements="*"/>
<xsl:output method="text" indent="no" omit-xml-declaration="yes"/>
<xsl:template match="text()"/>
<xsl:param name="process" select="'unknown'"/>
<xsl:param name="isLinuxInstance" select="0"/>

<xsl:template match="/">
   <xsl:apply-templates select="/Environment/Software"/>
</xsl:template>

<xsl:template match="EclCCProcess|EclCCServerProcess">
    <xsl:if test="@name=$process">
     # eclcc.ini

     # default locations for these will match the usual RPM install location. But if that is changed,
     # or you have an unusual custom configuration, these may need changing
     #libraryPath=./cl/lib
     #includePath=./cl/include
     #plugins=./plugins
     #templatePath=.
     #eclLibrariesPath=./ecllibrary
  </xsl:if>
</xsl:template>

</xsl:stylesheet>
