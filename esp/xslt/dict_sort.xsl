<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
##############################################################################
-->

<!-- sort an xml alphabetically -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <!-- node / is the parent of root node -->
    <xsl:template match="/">
        <xsl:call-template name="sort-child">
            <xsl:with-param name="node" select="child::*[1]"/>
        </xsl:call-template>
    </xsl:template>
    <!-- sort all children of a node -->
    <xsl:template name="sort-child">
        <xsl:param name="node"/>
        <xsl:choose>
            <xsl:when test="count($node/child::*)">
                <xsl:element name="{local-name($node)}" namespace="{namespace-uri($node)}">
                    <!-- namespace -->
                    <xsl:for-each select="$node/namespace::*">
                       <xsl:copy-of select="."/>
                    </xsl:for-each>
                    <!--  attributes -->
                    <xsl:for-each select="$node/@*">
                        <xsl:copy-of select="."/>
                    </xsl:for-each>
                    <!-- children -->
                    <xsl:for-each select="$node/*">
                        <xsl:sort select="name()"/>
                        <xsl:call-template name="sort-child">
                            <xsl:with-param name="node" select="."/>
                        </xsl:call-template>
                    </xsl:for-each>
                </xsl:element>
            </xsl:when>
            <xsl:otherwise>
                <xsl:copy-of select="$node"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
