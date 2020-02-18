<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
   <xsl:template match="/Exceptions">
      <html>
         <head>
           <link rel="stylesheet" type="text/css" href="files_/yui/build/fonts/fonts-min.css" />
           <link rel="stylesheet" type="text/css" href="files_/css/espdefault.css" />
           <link rel="stylesheet" type="text/css" href="files_/css/eclwatch.css" />
           <link rel="stylesheet" type="text/css" href="files_/yui/build/menu/assets/skins/sam/menu.css" />
           <link rel="stylesheet" type="text/css" href="files_/yui/build/button/assets/skins/sam/button.css" />
           <script type="text/javascript" src="files_/scripts/espdefault.js">&#160;</script>
           <script type="text/javascript"><xsl:text disable-output-escaping="yes"><![CDATA[
                    var index = 1;
                        function onLoad()
                        {
                            //if we came here because of tree navigation in ws_roxieconfig then go back 2 pages else 1 page only
                            var loc = document.location.toString();
                            if (loc.indexOf('/ws_roxieconfig/NavMenuEvent?cmd=DeployMultiple') != -1)
                                index = 2;
                            var btn = document.getElementById('backBtn');
                            if (index < history.length)
                                btn.style.display = 'block';
                        }
                        ]]>
                        </xsl:text>
            </script>
         </head>
         <body onload="nof5();onLoad()" class="yui-skin-sam">
            <h3>Exception(s) occurred:</h3>
            <xsl:if test="Source">
               <h4>Reporter: <xsl:value-of select="Source"/></h4>
            </xsl:if>
            <table border="0">
               <tbody>
                  <tr>
                     <th>Code</th>
                     <th align="left">Message</th>
                  </tr>
                  <xsl:for-each select="Exception">
                     <tr>
                        <td><xsl:value-of select="Code"/></td>
                        <td align="left"><xsl:value-of select="Message"/></td>
                     </tr>
                  </xsl:for-each>
               </tbody>
            </table>
            <br/>
                    <input id="backBtn" type="button" value="Go Back" onclick="history.go(-index)" style="display:none"> </input>
         </body>
      </html>
   </xsl:template>
</xsl:stylesheet>
