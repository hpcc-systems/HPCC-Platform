<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                              xmlns:xlink="http://www.w3.org/1999/xlink"
                              xmlns:svg="http://www.w3.org/2000/svg">
<xsl:output method="text" indent="no" media-type="application/x-javascript"/>
<xsl:param name="running"/>


<xsl:template match="/">
function updateStyle(object, tagname, style)
{
    var list=object.getElementsByTagName(tagname);
    if(list &amp;&amp; list.length)
        list.item(0).setAttribute('style',style);
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
        }*/
        else
        {
            throw "new text node added";
        }
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
    var list=object.getElementsByTagName('text');
    for(var i in list)
    {
        var obj=list.item(i);
        if(obj)
        {
            obj.setAttribute('style',style2);
        }
    }
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
        svg.reload();
    }
}

updateGraph();

<xsl:if test="not(number($running))">
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

