<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
xmlns:set="http://exslt.org/sets" exclude-result-prefixes="set">

  <xsl:template name="EspLoggingTransactionID">
    <xsl:param name="agentNode"/>
    <xsl:if test="string($agentNode/@MaxTransIDLength) != ''">
      <MaxTransIDLength><xsl:value-of select="$agentNode/@MaxTransIDLength"/></MaxTransIDLength>
    </xsl:if>
    <xsl:if test="string($agentNode/@MaxTransIDSequenceNumber) != ''">
      <MaxTransIDSequenceNumber><xsl:value-of select="$agentNode/@MaxTransIDSequenceNumber"/></MaxTransIDSequenceNumber>
    </xsl:if>
    <xsl:if test="string($agentNode/@MaxTransSeedTimeoutMinutes) != ''">
      <MaxTransSeedTimeoutMinutes><xsl:value-of select="$agentNode/@MaxTransSeedTimeoutMinutes"/></MaxTransSeedTimeoutMinutes>
    </xsl:if>
  </xsl:template>
</xsl:stylesheet>
