<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
##############################################################################
-->

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"> 
<xsl:output method="html"/>

<xsl:variable name="debug" select="0"/>
<xsl:variable name="filePath">
    <xsl:choose>
        <xsl:when test="$debug">c:/development/bin/debug/files</xsl:when>
        <xsl:otherwise>files_</xsl:otherwise>
    </xsl:choose>
</xsl:variable>

<xsl:variable name="rootNode"   select="/*[1]"/>

<xsl:variable name="disableCache">
   <xsl:choose>
      <xsl:when test="$rootNode/@cache='false' or $rootNode/@cache='0'">1</xsl:when>
      <xsl:otherwise>0</xsl:otherwise>
   </xsl:choose>
</xsl:variable>

<xsl:template match="/*">
    <html>
        <head>
            <title>ESP Navigation Window</title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
      <link type="text/css" rel="StyleSheet" href="{$filePath}/css/sortabletable.css"/>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <xsl:if test="@selectedTab">
                <link type="text/css" rel="StyleSheet" href="{$filePath}/css/tabs.css"/>
            </xsl:if>
            <style>
        .tree a, A:link, a:visited, a:active, A:hover {
        color: #000000;
        text-decoration: bold;
        font: normal 8pt verdana, arial, helvetica, sans-serif;
        }
        .tooltip {
        visibility:hidden;
        }
        .small {font: normal 10pt Serif; }
      </style>
            <style type="text/css" media="screen">
                @import url( <xsl:value-of select="$filePath"/>/css/rightSideBar.css );
            </style>
      <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
      <script language="JavaScript" src="{$filePath}/scripts/tree.js"></script>
            <script language="JavaScript" src="{$filePath}/popup.js"></script>
            <script language="JavaScript" src="{$filePath}/scripts/tooltip.js"></script>
            <script language="JavaScript" src="{$filePath}/scripts/rightSideBar.js"></script>
            <script language="JavaScript">
               <xsl:attribute name="src">
                  <xsl:choose>
                     <xsl:when test="string(@template)!=''">
                            <xsl:value-of select="@template"/>
                     </xsl:when>
                     <xsl:otherwise>
                            <xsl:value-of select="concat($filePath, '/scripts/tree_template.js')"/>
                     </xsl:otherwise>
                  </xsl:choose>
               </xsl:attribute>
            </script>
            <xsl:call-template name="insertJavascriptTree"/>
        </head>
    <body class="yui-skin-sam" onload="nof5();createTree()" onresize="leftFrameResized()">
            <div id="pageBody">
                <xsl:if test="@selectedTab">
                    <xsl:apply-templates select="TabContainer">
                        <xsl:with-param name="selectedTab" select="@selectedTab"/>
                    </xsl:apply-templates>
                </xsl:if>
                <xsl:if test="string(@caption)!=''">
                    <h3><xsl:value-of select="@caption"/></h3>
                </xsl:if>
                <xsl:if test="string(@subcaption)!=''">
                    <b><xsl:value-of select="@subcaption"/></b>
                </xsl:if>
                <div id="tree_0">
                    <xsl:if test="@border='1'">
                        <xsl:attribute name="style">border:1px groove lightgray</xsl:attribute>
                    </xsl:if>
                </div>  
                <xsl:for-each select="Buttons">
                    <br/>
                    <center>
                        <xsl:for-each select="*">
                            <xsl:if test="position()!=1">&nbsp;</xsl:if>
                            <xsl:copy-of select="."/>               
                        </xsl:for-each>
                    </center>
                </xsl:for-each>
                <xsl:if test="$debug">
                    <br/><textarea rows="50" cols="120" id="textarea"></textarea>
                </xsl:if>
                <form method="post" id="treeForm" action="" target="main">
                    <input type="hidden" id="Cmd" name="Cmd" value="Publish"/>
                    <input type="hidden" id="XmlArg" name="XmlArg"/>
                </form>
                <div id="ToolTip" class="tooltip"/>
                <div id="menu" border = "outset black 1px" 
                 style="left:0px; top:0px; visibility:hidden; position:absolute; backgroundColor:menu"/>
                <xml id='xmlArgs'/>
            </div>
            <div id="pageRightBar">
                <input type="button" title="Click to hide tree" hidefocus="true" onclick="onToggleTreeView(this)" 
                    style="height:100%; width:100%; background:lightgrey;">
                </input>
            </div>          
        </body>
    </html>
</xsl:template>

<xsl:template name="insertJavascriptTree">
    <script language="JavaScript">
        function createTree()
        {
            var o_tree = new tree ([], TREE_TPL);
            o_tree.on_sel_changed  = onItemSelected;
            o_tree.on_command      = onCommand;
            //o_tree.on_expanding    = onItemExpanding;
            //o_tree.on_context_menu = onContextMenu;
            //tooltipBodyColor = 'highlight';
            //tooltipBodyTextColor = 'window';
            //import_xml(o_tree.get_item(0), xmlInfo.documentElement);
            <xsl:if test="$disableCache=1">
                o_tree.b_cache_children = false;
            </xsl:if>
            <xsl:if test="string(@action)!=''">
                o_tree.action='<xsl:value-of select="@action"/>';
            </xsl:if>
            <xsl:if test="@border='1' or @border='true'">
                o_tree.border='<xsl:value-of select="@border"/>';
            </xsl:if>
            <xsl:apply-templates mode="get_scripts"/>
            <xsl:apply-templates select="Menu"/>
            <xsl:apply-templates select="Columns/Column"/>
            <xsl:apply-templates select="Folder|DynamicFolder|Link"/>
            <xsl:if test="$debug">
                document.getElementById("textarea").value = document.getElementById("tree_0").outerHTML;
            </xsl:if>
            o_tree.o_root.expand(true);
        }
        <xsl:text><![CDATA[ 
       function onItemSelected(tree)
       {
        return true;
       }
        
       function onItemExpanding(item, b_expand)
       {
            //var items = get_items_at_xpath(o_tree.o_root, 'Attribute Servers/*/Modules');
            //if (b_expand && item.a_children.length == 0)
            //   import_xml_nodeset(item, xmlInfo.selectNodes('A/B'));
           return true;
       }
    
       function onContextMenu(tree, menu)
       {
          //change default menu, if needed
          //menu.push(['Hello']);
       }
    
       function onCommand(tree, cmd, action)
       {
          //override handling of the command      
          //
          var form = document.forms['treeForm'];
          form.Cmd.value = cmd;
          
          var xmlDoc = selectionToXmlDoc(tree);
          var xml = null;
          if (xmlDoc.xml)
          {              
              xml = xmlDoc.xml;
              var lastGT = xml.lastIndexOf('>');
              //IE BUG results in stray characters after last '>' in xmlDoc.xml, so remove them, if any                       
                  if (lastGT != -1 && lastGT < xml.length)
                 xml = xml.substring(0, lastGT+1);
          }  else if(window.XMLSerializer) {
                 xml = (new XMLSerializer()).serializeToString(xmlDoc);
            }
         form.XmlArg.value = xml;
            form.action = action + '?cmd=' + cmd;    
         //alert("Form action: " + form.action + "\nXML: "+xml);
          
             //the main frame might reject loading the page if user has some outstanding (unsubmitted)
             //changes to the page.  In this case, the call form.submit() generates an "unspecified 
             //script error" and there is no way to get around that so we need a hack.  Instead of 
             //posting the form, which fails to change the page in main frame we display an intermediate
             //"Loading, please wait..." html page which is specifically written to make us post this 
             //form when that page finishes loading.
             //
          //form.submit();
          var frm = top.window.frames['main'];
          frm.location = 'files_/submitNavForm.html';    
          //alert("Form location: " + frm.location);
          return true;
       }   
        ]]></xsl:text>
    </script>
</xsl:template>

<xsl:template match="Columns/Column">
    var column = [];
    <xsl:for-each select="@*">
        column['<xsl:value-of select="name()"/>'] = "<xsl:value-of select="."/>";
    </xsl:for-each>
    column['innerHTML'] = "<xsl:value-of select="text()"/>";
    
    o_tree.add_tree_column( column );
</xsl:template>

<xsl:template match="node()|@*" mode="get_scripts">
    <xsl:value-of select="self::Script"/>
    <xsl:apply-templates select="node()|@*" mode="get_scripts"/>
</xsl:template>


<xsl:template match="Menu">o_tree.add_custom_menu('<xsl:value-of select="@name"/>', [<xsl:apply-templates select="MenuItem"/>]);</xsl:template>

<xsl:template match="MenuItem"><xsl:if test="position()!=1">,<xsl:text>
   </xsl:text></xsl:if>['<xsl:value-of select="@name"/>', '<xsl:value-of select="@action"/>', '<xsl:value-of select="@tooltip"/>']</xsl:template>

<xsl:template match="DynamicFolder|Folder|Link">
<xsl:param name="parent_id" select="'-1'"/>
    function process<xsl:value-of select="position()"/>(parent_id, name) {
        var item = add_item(o_tree, parent_id, '<xsl:value-of select="name()"/>', name, '<xsl:value-of select="@tooltip"/>',
                                        '<xsl:value-of select="@menu"/>', '<xsl:value-of select="@params"/>');
        <xsl:if test="$disableCache=0 and (@cache='false' or @cache='0')">
           item.b_cache_children=false;
        </xsl:if>
    <xsl:if test="string(@action)!='' and string(@action)!=string($rootNode/@action)">
        item.action='<xsl:value-of select="@action"/>';
    </xsl:if>
    <xsl:for-each select="Column">
        item.add_column('<xsl:value-of select="text()"/>');
    </xsl:for-each>
    <xsl:if test="@checkbox">
        item.b_checkbox = true;
        <xsl:if test="@checked">
            item.b_checked = true;
        </xsl:if>
    </xsl:if>
        <xsl:apply-templates select="DynamicFolder|Folder|Link">
            <xsl:with-param name="parent_id" select="'item.n_id'"/>
        </xsl:apply-templates>
    }
    var parent_id = <xsl:value-of select="$parent_id"/>;
    process<xsl:value-of select="position()"/>(parent_id, '<xsl:value-of select="@name"/>');
    var parent = parent_id == -1 ? o_tree.o_root : o_tree.get_item(parent_id);
    redo_item(parent);
</xsl:template>

<xsl:template match="TabContainer">
    <xsl:param name="selectedTab"/>
    <div id="tabContainer" width="100%">
        <ul id="tabNavigator">
            <xsl:for-each select="Tab">
                <li>
                    <a href="{@url}">
                        <xsl:if test="@name=$selectedTab">
                            <xsl:attribute name="class">active</xsl:attribute>
                        </xsl:if>
                        <xsl:value-of select="@name"/>
                    </a>
                </li>
            </xsl:for-each>
        </ul>
    </div>
    <br/>
    <br/>
</xsl:template>

<xsl:template match="text()|@*"/>

</xsl:stylesheet>
