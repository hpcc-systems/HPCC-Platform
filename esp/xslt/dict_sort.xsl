<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
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
