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
    <xsl:template match="Graph">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
          <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title><xsl:value-of select="Wuid"/>/<xsl:value-of select="Type"/></title>
            <script language="JavaScript1.2">
            function reload_graph()
            {
                hide_popup();

                var script=document.all['popup_labels'];
                if(script) script.src='/image/<xsl:value-of select="Wuid"/>/<xsl:value-of select="Type"/>.jsu';
            }
            var refresh=null;
            <xsl:if test="number(Running)">
            refresh=setInterval(reload_graph,30000);
            </xsl:if>

            <xsl:text disable-output-escaping="yes">
            function clip(w)
            {
                var n=Number(String(w).match(/\d+/)[0]);    
                return n;
            }

            function resize_graph(width,height)
            {
                var svg=document.all['SVGGraph'];
                if(!svg) return;

                var w=clip(width), h=clip(height);
                var ws=Math.min(16383,w), hs=Math.min(16383,h)
                svg.style.width=ws;
                svg.style.height=hs;

                var root=svg.getSVGDocument().rootElement;
                root.currentScale=Math.min(ws/w,hs/h);
            }
            
            function try_to_fit(iframe,w,h,xn1,yn1,xm1,ym1,xn2,yn2,xm2,ym2)
            {
                var x=0, y=0;
                if(w>=xm1-xn1)
                    w=xm1-xn1-1;
                if(h>=ym1-yn1)
                    h=ym1-yn1-1;

                if(xm1-xm2>w &amp;&amp; ym1-yn1>h)
                {
                    x=xm2;
                    y=Math.min(ym2,ym1-h);
                }
                else if(xm1-xn1>w &amp;&amp; ym1-ym2>h)
                {
                    x=Math.min(xm2,xm1-w);
                    y=ym2;
                }
                else if(xn2-xn1>w &amp;&amp; ym1-yn1>h)
                {
                    x=xn2-w;
                    y=Math.max(yn1,yn2-h);
                }
                else if(xm1-xn1>w &amp;&amp; yn2-yn1>h)
                {
                    x=Math.max(xn2-w,xn1);
                    y=yn2-h;
                }
                else return null;

                iframe.style.left=x;
                iframe.style.top=y;
                iframe.style.width=w-2;
                iframe.style.height=h-2;
                iframe.style.visibility='visible';
            }

            function show_popup(popup_id)
            {
                if(!window.popups) return;

                var popup=window.popups[popup_id];
                if(!popup) return;

                var frame=document.frames['popupFrame'];
                if(!frame) return;
                    
                var iframe=document.all['popupFrame'];
                if(!iframe) return;

                var svg=document.all['SVGGraph'];
                if(!svg) return;

                var o=svg.getSVGDocument().getElementById(popup_id);
                if(!o) return;

                var root=svg.getSVGDocument().rootElement, 
                    scale=root.currentScale,
                    shift=root.currentTranslate;

                var rect=o.getBBox();
                var x=rect.x*scale+shift.x, 
                    y=rect.y*scale+shift.y,
                    w=rect.width*scale,
                    h=rect.height*scale;

                var html='&lt;table id="tab1" style="background-color:yellow;border:2 solid black;margin:0;padding:0;font:normal normal lighter 14 normal Times">&lt;colgroup>&lt;col align="left" valign="top"/>&lt;col/>&lt;/colgroup>';
                for(var i in popup)
                {
                    html+='&lt;tr>&lt;th>'+i+':&lt;/th>&lt;td>'+String(popup[i]).replace(/&lt;/g,'&amp;lt;').replace(/\n/g,'&lt;br/>')+'&lt;/td>&lt;/tr>';
                }
                html+='&lt;/table>';

                frame.document.body.innerHTML=html;
                frame.document.body.style.margin='0';

                var tab=frame.document.all['tab1'];
                
                iframe.style.width=400;
                try_to_fit(iframe,tab.clientWidth+6,tab.clientHeight+6,
                           document.body.scrollLeft,document.body.scrollTop,document.body.scrollLeft+document.body.clientWidth,document.body.scrollTop+document.body.clientHeight,
                           svg.offsetLeft+x,svg.offsetTop+y,svg.offsetLeft+x+w,svg.offsetTop+y+h);
            }

            function hide_popup()
            {
                var iframe=document.all['popupFrame'];
                if(!iframe) return;
                iframe.style.visibility='hidden';
                iframe.style.height=1;
                iframe.style.width=1;
            }

            </xsl:text>
            </script>
            <script id="popup_labels" language="JavaScript1.2" src="/image/{Wuid}/{Type}.js">a</script>
          </head>
          <body>
              <h4>Workunit: <a href="/wuid/{Wuid}"><xsl:value-of select="Wuid"/></a></h4> 
              <embed id="SVGGraph" width="1" height="1" src="/image/{Wuid}/{Type}.svg" type="image/xml+svg" pluginspage="http://www.adobe.com/svg/viewer/install/"/> 
              <iframe id="popupFrame" frameborder="0" scrolling="no" style="position:absolute;left:0;top:0;width:400;height:1;visibility:hidden"></iframe>
          </body> 
        </html>
    </xsl:template>
</xsl:stylesheet>
