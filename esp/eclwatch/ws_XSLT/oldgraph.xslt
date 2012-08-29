<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

    <xsl:output method="html"/>
    <xsl:template match="Graph">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <title>
                    <xsl:value-of select="Wuid"/>
                </title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script language="JavaScript1.2">
            function reload_graph()
            {
                hide_popup();

                var script=document.all['popup_labels'];
                if(script) script.src='/WUProcessGraph?Wuid=<xsl:value-of select="Wuid"/>&amp;Name=<xsl:value-of select="Name"/>&amp;Type=jsu';
            }
            var refresh=null;
            <xsl:if test="number(Running)">
            refresh=setInterval(reload_graph,30000);
            </xsl:if>
                    <xsl:text disable-output-escaping="yes">
            function clip(w)
            {
                var n=Number(String(w).match(/\d+/)[0]);    
                return Math.min(n,16383);
            }

            function resize_graph(width,height)
            {
                var svg=document.all['SVGGraph'];
                if(!svg) return;

                svg.style.width=clip(width);
                svg.style.height=clip(height);
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
                var rect=o.getBBox();


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
                           svg.offsetLeft+rect.x,svg.offsetTop+rect.y,svg.offsetLeft+rect.x+rect.width,svg.offsetTop+rect.y+rect.height);
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
                <script id="popup_labels" language="JavaScript1.2" src="/WUProcessGraph?Wuid={Wuid}&amp;Name={Name}&amp;Type=js">a</script>
            </head>
      <body class="yui-skin-sam">
                <h4>Workunit: <a href="/WUInfo?Wuid={Wuid}&amp;IncludeGraphs=1&amp;IncludeTimings=1&amp;IncludeActions=1&amp;IncludeResults=1&amp;IncludeLogs=1&amp;IncludeExceptions=1&amp;IncludeLogs=1">
                        <xsl:value-of select="Wuid"/>
                    </a>
                </h4>.svg
                <embed id="SVGGraph" width="1" height="1" src="/WUProcessGraph?Wuid={Wuid}.svg" type="image/xml+svg" pluginspage="http://www.adobe.com/svg/viewer/install/"/>
                <iframe id="popupFrame" frameborder="0" scrolling="no" style="position:absolute;left:0;top:0;width:400;height:1;visibility:hidden"/>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
