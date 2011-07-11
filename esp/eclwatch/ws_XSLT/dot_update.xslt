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

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              xmlns:xlink="http://www.w3.org/1999/xlink"
                              xmlns:svg="http://www.w3.org/2000/svg">
<xsl:output method="text" indent="no" media-type="application/x-javascript"/>
<xsl:param name="Running"/>


<xsl:template match="/">
function updateStyle(object, tagname, style)
{
    var list=object.getElementsByTagName(tagname);
    for(var i in list)
    {
        list.item(i).setAttribute('style',style);
    }
}


function updateTextAttributes(object, attr)
{
    object.setAttribute('x',attr[0]);
    object.setAttribute('y',attr[1]);
    object.setAttribute('style',attr[3]);
    
    if (object.getFirstChild() == null)
        object.appendChild(object.ownerDocument.createTextNode(attr[2]))
    else
        object.getFirstChild().setData(attr[2]);
        
}

function updateEdge(id,style1,style2,text)
{
    var object=svgdoc.getElementById(id);
    if(!object) throw "no node"+id;

    updateStyle(object,'polygon',style1);
    updateStyle(object,'path',style2);

    var list=object.getElementsByTagName('text');
    for(var i=3;i&lt;arguments.length;i++)
    {
        var obj=list.item(i-3);
        if(obj)
        {
            updateTextAttributes(obj,arguments[i]);
        }
        /*
        else if(list.item(0))
        {
            obj=list.item(0).cloneNode(false);
            if(!obj) throw "could not create";
            updateTextAttributes(obj,arguments[i]);
            object.appendChild(obj);
        }
        */
    }

    for(var i=arguments.length-3;i&lt;list.length;i++)
    {
        object.removeChild(list(i));
    }
}

function updateNode(id,style1,style2)
{
    var object=svgdoc.getElementById(id);
    if(!object) throw "no node"+id;

    updateStyle(object,'polygon',style1);
    updateStyle(object,'text',style2);
}

var svg=document.all['SVGGraph'];
var svgdoc=svg.getSVGDocument();

function updateGraph()
{
    try
    {
        <xsl:apply-templates/>
    }
    catch(e)
    {
        alert('Exception in dynamically updating the graph. ' + e);
        svg.reload();
        resize_graph();
    }
}

updateGraph();
<xsl:if test="not(number($Running))">
if(window.refresh) { clearInterval(refresh); refresh=null; }
</xsl:if>
</xsl:template>

<xsl:template match="svg:g[@class='node']">
    <xsl:variable name="nodeid">
        <xsl:value-of select="svg:a/@xlink:href"/>
    </xsl:variable>    
    <xsl:variable name="textstyle" select="svg:a/svg:text/@style"/>
    updateNode('<xsl:value-of select="$nodeid"/>','<xsl:value-of select="svg:a/svg:polygon/@style"/>',<xsl:text/>
    <xsl:choose><xsl:when test="string-length(svg:a/svg:text/@style)">'<xsl:value-of select="svg:a/svg:text/@style"/>'</xsl:when><xsl:otherwise>'fill:black;'</xsl:otherwise></xsl:choose>);<xsl:text/>
</xsl:template>

<xsl:template match="svg:g[@class='edge']">
    <xsl:variable name="nodeid">
        <xsl:value-of select="svg:a/@xlink:href"/>
    </xsl:variable>    
    updateEdge('<xsl:value-of select="$nodeid"/>','<xsl:value-of select="svg:a/svg:polygon/@style"/>','<xsl:value-of select="svg:a/svg:path/@style"/>'<xsl:text/>
    <xsl:for-each select="svg:a/svg:text">
        <xsl:text/>,[<xsl:value-of select="@x"/>,<xsl:value-of select="@y"/>,'<xsl:value-of select="."/>','<xsl:value-of select="@style"/>']<xsl:text/>
    </xsl:for-each>
    <xsl:text>);</xsl:text>
</xsl:template>
<xsl:template match="text()|comment()"/>

</xsl:stylesheet>

