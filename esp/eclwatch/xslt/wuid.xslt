<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>
    <xsl:variable name="wuid" select="Workunit/Wuid"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="$wuid"/></title>
            <style type="text/css">
                table {}
                table.workunit tr { padding:12 0; }
                table.workunit th { text-align:left; vertical-align:top; }
                table.workunit td { padding:2 2 2 12; }
                .sbutton { width: 7em; font: 10pt arial , helvetica, sans-serif; }
            </style>

          </head>
          <body>
                <xsl:apply-templates />
          </body> 
        </html>
    </xsl:template>


    <xsl:template match="Workunit">
        <form id="protect" action="/wuid/{$wuid}" method="post">
        <table class="workunit">
            <colgroup>
               <col width="20%"/>
               <col width="80%"/>
            </colgroup>
            <tr><th>Workunit:</th><td><a href="/wuid/{Wuid}/xml"><xsl:value-of select="Wuid"/></a></td></tr>
            
            <tr><th>State:</th>
                <td>
                <select size="1" name="state">
                    <option value="0"><xsl:if test="State='unknown'"><xsl:attribute name="selected"/></xsl:if>unknown</option>
                    <option value="1"><xsl:if test="State='compiled'"><xsl:attribute name="selected"/></xsl:if>compiled</option>
                    <option value="2"><xsl:if test="State='running'"><xsl:attribute name="selected"/></xsl:if>running</option>
                    <option value="3"><xsl:if test="State='completed'"><xsl:attribute name="selected"/></xsl:if>completed</option>
                    <option value="4"><xsl:if test="State='failed'"><xsl:attribute name="selected"/></xsl:if>failed</option>
                    <option value="5"><xsl:if test="State='archived'"><xsl:attribute name="selected"/></xsl:if>archived</option>
                    <option value="6"><xsl:if test="State='aborting'"><xsl:attribute name="selected"/></xsl:if>aborting</option>
                    <option value="7"><xsl:if test="State='aborted'"><xsl:attribute name="selected"/></xsl:if>aborted</option>
                    <option value="8"><xsl:if test="State='blocked'"><xsl:attribute name="selected"/></xsl:if>blocked</option>
                </select>
                </td>
            </tr>

            <tr><th>Owner:</th><td><xsl:value-of select="Owner"/></td></tr>

            <tr><th>Jobname:</th><td><input type="text" name="jobName" value="{JobName}" size="40"/></td></tr>

            <tr><th>Description:</th><td><input type="text" name="description" value="{Description}" size="40"/></td></tr>

            <tr><th>Protected:</th>
                <td>
                <input type="checkbox" name="checked"> <xsl:if test="number(Protected)"><xsl:attribute name="checked"/></xsl:if></input>
                </td>
            </tr>

            <tr><th>Cluster:</th><td><xsl:value-of select="Cluster"/></td></tr>

            <xsl:if test="count(Exception)">
            <tr><th>Exceptions:</th><td><table><xsl:apply-templates select="Exception"/></table></td></tr>
            </xsl:if>

            <xsl:if test="count(Result)">
            <tr><th>Results:</th><td><table><xsl:apply-templates select="Result"/></table></td></tr>
            </xsl:if>

            <xsl:if test="count(Graph)">
            <tr><th>Graphs:</th><td><table class="list"><xsl:apply-templates select="Graph"/></table></td></tr>
            </xsl:if>

            <xsl:if test="count(Timer)">
            <tr><th>Timings:</th><td><table><colgroup><col/><col align="char" char="."/></colgroup><xsl:apply-templates select="Timer"/></table></td></tr>
            </xsl:if>

            <tr><th>Helpers:</th>
            <td>
            <table class="list">
            <tr>
            <xsl:if test="string-length(Cpp)"><td><a href="/wuid/{Wuid}/cpp">cpp</a></td></xsl:if>
            <xsl:if test="string-length(Dll)"><td><a href="/wuid/{Wuid}/dll">dll</a></td></xsl:if>
            <xsl:if test="string-length(ResTxt)"><td><a href="/wuid/{Wuid}/res.txt">res.txt</a></td></xsl:if>
            <xsl:if test="string-length(ThorLog)"><td><a href="/wuid/{Wuid}/thormaster.log">thormaster.log</a></td></xsl:if>
            </tr>
            </table>
            </td>
            </tr>

            <tr><th></th>
                <td>
                <input type="submit" name="save" value="Save" class="sbutton"/>
                <input type="submit" name="reset" value="Reset" class="sbutton"/>
                </td>
            </tr>
            <tr><th></th>
                <td>
                <input type="submit" name="abort" value="Abort" class="sbutton" onclick="return confirm('Abort workunit?')"/>
                <xsl:if test="string-length(EclQueue)">
                <input type="hidden" name="eclQueue" value="{EclQueue}"/>
                <input type="submit" name="resubmit" value="Resubmit" class="sbutton"/>
                <input type="submit" name="recompile" value="Recompile" class="sbutton"/>
                </xsl:if>
                </td>
            </tr>
        </table>
        </form>
        <a href="/index">root</a>
    </xsl:template>

    <xsl:template match="Exception">
        <tr>
        <th><xsl:value-of select="Source"/></th>
        <td><xsl:value-of select="Message"/></td>
        </tr>
    </xsl:template>

    <xsl:template match="Result">
        <tr>
        <td><xsl:value-of select="Name"/></td>
        <td>
        <xsl:choose>
            <xsl:when test="string-length(Link)"> 
                <a href="/wuid/{$wuid}/result/{Link}"><xsl:value-of select="Value"/></a>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="Value"/>
            </xsl:otherwise>
        </xsl:choose>
        </td>
        <td>
        <xsl:if test="string-length(LogicalName)"> 
            <a href="/dfu/{LogicalName}"><xsl:value-of select="LogicalName"/></a>
        </xsl:if>
        </td>
        </tr>
    </xsl:template>

    <xsl:template match="Graph">
        <xsl:variable name="cols" select="8"/>
        <xsl:if test="((position()-1) mod $cols)=0">
            <xsl:text disable-output-escaping="yes">&lt;tr></xsl:text>
        </xsl:if>
        <td><a href="/graph/{$wuid}/{Name}"><xsl:value-of select="Name"/></a></td>
        <xsl:if test="(position() mod $cols)=0 or position()=last()">
            <xsl:text disable-output-escaping="yes">&lt;/tr></xsl:text>
        </xsl:if>
    </xsl:template>

    <xsl:template match="Timer">
        <tr>
        <td><xsl:value-of select="Name"/></td>
        <td><xsl:value-of select="Value"/></td>
        <td>        
            <xsl:if test="number(Count)>1">(<xsl:value-of select="Count"/> calls)</xsl:if>
        </td>
        </tr>
    </xsl:template>



</xsl:stylesheet>
