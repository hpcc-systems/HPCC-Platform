<?xml version="1.0"?>
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

<!DOCTYPE overlay [
  <!--uncomment these for production-->
  <!ENTITY filePathEntity "/esp/files_">
  <!ENTITY xsltPathEntity "/esp/xslt">

]>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns="http://www.w3.org/1999/xhtml">

  <xsl:output method="html"/>

  <xsl:template match="/">
    <html>
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <style type="text/css">
          body {
          font:13px/1.231 verdana,arial,helvetica,clean,sans-serif;
          font-family: monospace;
          }
          *|*:root {
          background-color: white;
          color: black;
          }
          .start-tag {
          color: purple;
          font-weight: bold;
          }
          .end-tag {
          color: purple;
          font-weight: bold;
          }
          .text
          {
          color: black;
          font-weight: bold;
          }
          .comment {
          color: green;
          font-style: italic;
          }
          .content {
          padding-left: 2em;
          }
          .attr-name {
          color: olive;
          font-weight: normal;
          }
          .attr-value {
          color: blue;
          font-weight: normal;
          }

          .expd {
          cursor: pointer;
          text-align: left;
          vertical-align: top;
          display: inline-block;
          }

          .not-expd {
          margin-left: 1em;
          }

          #top > .expd-open, #top > .expd-closed {
          margin-left: 1em;
          }

          .expd-closed > .content {
          display: none;
          }
        </style>
        <title>EclWatch</title>
        <script language="JavaScript1.2">
          <xsl:text disable-output-escaping="yes"><![CDATA[
            function toggleElement(id)
            {
              span = document.getElementById( 'div_' + id );
              img  = document.getElementById( 'exp_' + id );
              if (span.style.display == 'none')
              {
                span.style.display = 'block';
                img.firstChild.data = '- <';
              }
              else
              {
                span.style.display = 'none';
                img.firstChild.data = '+ <';
              }
            }
            function toggleComment(id)
            {
              span = document.getElementById( 'div_' + id );
              img  = document.getElementById( 'exp_' + id );
              if (span.style.display == 'none')
              {
                span.style.display = 'block';
                img.firstChild.data = '- <!--';
              }
              else
              {
                span.style.display = 'none';
                img.firstChild.data = '+ <!--';
              }
            }
          ]]></xsl:text>
        </script>
      </head>
      <body>
        <xsl:apply-templates/>
      </body>
    </html>  
  </xsl:template>

  <xsl:template match="*">
    <xsl:param name="tags"/>
    <div class="not-expd">
      <xsl:text>&lt;</xsl:text>
      <span class="start-tag"><xsl:value-of select="name(.)"/></span>
      <xsl:call-template name="namespace-attr" />
      <xsl:apply-templates select="@*"/>
      <xsl:text>/&gt;</xsl:text>
    </div>
  </xsl:template>

  <xsl:template match="*[node()]">
    <xsl:param name="tags"/>
    <div class="not-expd">
      <xsl:text>&lt;</xsl:text>
      <span class="start-tag"><xsl:value-of select="name(.)"/></span>
      <xsl:call-template name="namespace-attr" />
      <xsl:apply-templates select="@*"/>
      <xsl:text>&gt;</xsl:text>

      <span class="text"><xsl:value-of select="."/></span>

      <xsl:text>&lt;/</xsl:text>
      <span class="end-tag"><xsl:value-of select="name(.)"/></span>
      <xsl:text>&gt;</xsl:text>
    </div>
  </xsl:template>

  <xsl:template match="*[* or comment() or processing-instruction()]">
    <xsl:param name="tags"/>
    <div>
      <xsl:call-template name="expand">
        <xsl:with-param name="tags">
          <xsl:value-of select="$tags"/>
          <xsl:value-of select="position()"/>
        </xsl:with-param>
        <xsl:with-param name="tagname">
          <xsl:value-of select="name(.)"/>
        </xsl:with-param>
      </xsl:call-template>

      <span id="div_{$tags}{position()}">
        <div class="content">
          <xsl:apply-templates>
            <xsl:with-param name="tags">
              <xsl:value-of select="$tags"/>
              <xsl:value-of select="name(.)"/>
              <xsl:value-of select="position()"/>
              <xsl:text>_</xsl:text>
            </xsl:with-param>
          </xsl:apply-templates>
        </div>

        <div class="not-expd">
          <xsl:text>&lt;/</xsl:text>
          <span class="end-tag"><xsl:value-of select="name(.)"/></span>
          <xsl:text>&gt;</xsl:text>
        </div>
      </span>
    </div>
  </xsl:template>

  <xsl:template match="comment()">
    <xsl:param name="tags"/>
    <div class="comment not-expd">
      <xsl:text>&lt;!--</xsl:text>
      <xsl:value-of select="."/>
      <xsl:text>--&gt;</xsl:text>
    </div>
  </xsl:template>

  <xsl:template match="comment()[string-length(.) &gt; 60]">
    <xsl:param name="tags"/>
    <div class="expd-open">
      <xsl:call-template name="expand-comment">
        <xsl:with-param name="tags">
          <xsl:value-of select="$tags"/>
          <xsl:value-of select="position()"/>
        </xsl:with-param>
      </xsl:call-template>

      <span id="div_{$tags}{position()}"  class="comment">
        <div class="content">
          <xsl:value-of select="."/>
        </div>
        <span class="not-expd">
          <xsl:text>--&gt;</xsl:text>
        </span>
      </span>
    </div>
  </xsl:template>

  <xsl:template match="processing-instruction()">
    <xsl:param name="tags"/>
  </xsl:template>

  <xsl:template match="@*">
    <xsl:text> </xsl:text>
    <span class="attr-name"><xsl:value-of select="name(.)"/></span>
    <xsl:text>=</xsl:text>
    <span class="attr-value">"<xsl:value-of select="."/>"</span>
  </xsl:template>

  <xsl:template match="text()">
    <xsl:if test="normalize-space(.)">
      <xsl:value-of select="."/>
    </xsl:if>
  </xsl:template>

  <xsl:template name="expand">
    <xsl:param name="tags"/>
    <xsl:param name="tagname" select="''"/>
    <div class="expd" id="exp_{$tags}" onclick="toggleElement('{$tags}')">
      <xsl:text>&#x2212; &lt;</xsl:text>
      <span class="start-tag">
        <xsl:value-of select="$tagname"/>
      </span>
      <xsl:call-template name="namespace-attr" />
      <xsl:apply-templates select="@*"/>
      <xsl:text>&gt;</xsl:text>
    </div>
  </xsl:template>

  <xsl:template name="namespace-attr">
    <xsl:if test="count(namespace::node()) > count(../namespace::node())">
      <xsl:variable name="namespaces-str">
        <xsl:for-each select="../namespace::node()"><xsl:text>&lt;</xsl:text><xsl:value-of select="name()" /><xsl:text>&gt;</xsl:text></xsl:for-each>
      </xsl:variable>
      <xsl:for-each select="namespace::node()">
        <xsl:variable name="n1" select="name()"/>
        <xsl:variable name="v1" select="."/>
        <xsl:if test="$v1 != 'http://www.w3.org/XML/1998/namespace'"> <!-- extra namespace added by browsers -->
          <xsl:variable name="namespace-str"><xsl:text>&lt;</xsl:text><xsl:value-of select="name()" /><xsl:text>&gt;</xsl:text></xsl:variable>
          <xsl:if test="not(contains($namespaces-str, $namespace-str))">
            <xsl:choose>
              <xsl:when test="$n1 != ''">
                <xsl:text> xmlns:</xsl:text><xsl:value-of select="$n1" /><xsl:text>="</xsl:text><xsl:value-of select="$v1"/><xsl:text>"</xsl:text>
              </xsl:when>
              <xsl:otherwise>
                <xsl:text> xmlns="</xsl:text><xsl:value-of select="$v1"/><xsl:text>"</xsl:text>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:if>
        </xsl:if>
      </xsl:for-each>
    </xsl:if>
  </xsl:template>

  <xsl:template name="expand-comment">
    <xsl:param name="tags"/>
    <div id="exp_{$tags}" class="expd comment" onclick="toggleComment('{$tags}')">
      <xsl:text>&#x2212; &lt;!--</xsl:text>
    </div>
  </xsl:template>

</xsl:stylesheet>
