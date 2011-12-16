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
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/reset/reset.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/reset-fonts-grids/reset-fonts-grids.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/resize/assets/skins/sam/resize.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/layout/assets/skins/sam/layout.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/tabview/assets/skins/sam/tabview.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/treeview/assets/skins/sam/treeview.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/paginator/assets/skins/sam/paginator.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/datatable/assets/skins/sam/datatable.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/autocomplete/assets/skins/sam/autocomplete.css" />
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <script type="text/javascript" src="/esp/files/yui/build/yahoo/yahoo-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/yuiloader/yuiloader-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/dom/dom-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/connection/connection-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/dragdrop/dragdrop-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/resize/resize-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/button/button-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/menu/menu.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/animation/animation-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/layout/layout-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/datasource/datasource-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/paginator/paginator-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/datatable/datatable-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/json/json-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/tabview/tabview-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/treeview/treeview.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/autocomplete/autocomplete-min.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/hpcc-ext/DataView.js"></script>
      <script type="text/javascript" src="/esp/files/yui/build/hpcc-ext/RowFilter.js"></script>

      <style type="text/css">
      </style>
    </head>
    <body class="yui-skin-sam" onload="setReloadFunction('refreshNode');buildTree();" onclick="checkTreeClick();">
      <div id="left1" style="text-align:left; width:600px; font-size:0.85em; height:auto; min-height:100%;">
        <div id="espNavTree" class=" ygtv-highlight" style="font-size:0.90em; padding: 2px; height: 100%;">&nbsp;</div>
      </div>

      <script type="text/javascript">
        <xsl:text disable-output-escaping="yes"><![CDATA[

      var tree, currentIconMode;

      var lastClickIsTreeClick = null;

      var treeSelections;

            /*
           function init() {
            var loader = new YAHOO.util.YUILoader({

                require: ["treeview", "button", "animation", "autocomplete"],
                base: '/esp/files/yui/build/',
                loadOptional: false,
                combine: true,
                filter: "MIN",
                allowRollup: true,

                //When the loading is all complete, we want to initialize   
                //our TabView process; we can set this here or pass this   
                //in as an argument to the insert() method:   
                onSuccess: function() {

                    //YAHOO.util.Get.css('yui/build/treeview/assets/css/folders/tree.css');
                    //YAHOO.util.Get.script('scripts/ws_roxieconfig_left.js');  
                    //YAHOO.util.Get.script('/esp/files/scripts/ws_roxieconfig.js');
                }
            });
            loader.insert();

            };

      YAHOO.util.Event.addListener(window, "load", init);
      
      */

      function checkTreeClick() {
        if (lastClickIsTreeClick == null) {
          clearNavTreeSelects();
        } 
        lastClickIsTreeClick = null;
      }

      function loadNodeData(node, fnLoadComplete)  {

        var nodeLabel = encodeURI(node.label);
        tree.locked = false;
        //prepare URL for XHR request:
        var sUrl = "/esp/navdata?" + node.data.params;
        //prepare our callback object
        var callback = {

          //if our XHR call is successful, we want to make use
          //of the returned data and create child nodes.
          success: function(oResponse) {
            var xmlDoc = oResponse.responseXML;

            node.setNodesProperty("propagateHighlightUp",false);
            node.setNodesProperty("propagateHighlightDown",false);
            
            var folderNodes = xmlDoc.getElementsByTagName("DynamicFolder");
            for(var i = 0; i < folderNodes.length; i++)
            {
              var childNode = new YAHOO.widget.TextNode({label: folderNodes[i].getAttribute("name")}, node);
              //childNode.labelStyle = "icon-doc";
              childNode.setNodesProperty("propagateHighlightUp",true);
              childNode.setNodesProperty("propagateHighlightDown",true);
              childNode.data = { elementType:'DynamicFolder', params: folderNodes[i].getAttribute("params") };
            }
            folderNodes = xmlDoc.getElementsByTagName("Link");
            for(var i = 0; i < folderNodes.length; i++)
            {
              var childNode = new YAHOO.widget.TextNode({label: folderNodes[i].getAttribute("name"), expanded:true}, node);
              //childNode.labelStyle = "icon-doc";

              childNode.isLeaf = true;
              childNode.data = { elementType:'Link', params: folderNodes[i].getAttribute("params") };
              childNode.setNodesProperty("propagateHighlightUp",true);
              childNode.setNodesProperty("propagateHighlightDown",true);
            }
            //When we're done creating child nodes, we execute the node's
            //loadComplete callback method which comes in via the argument
            //in the response object (we could also access it at node.loadComplete,
            //if necessary):
            oResponse.argument.fnLoadComplete();

            // Reset the propagate after the rending as it speeds up the dynamic load...
            node.setNodesProperty("propagateHighlightUp",true);
            node.setNodesProperty("propagateHighlightDown",true);

          },

          //if our XHR call is not successful, we want to
          //fire the TreeView callback and let the Tree
          //proceed with its business.
          failure: function(oResponse) {
            oResponse.argument.fnLoadComplete();
          },

          //our handlers for the XHR response will need the same
          //argument information we got to loadNodeData, so
          //we'll pass those along:
          argument: {
            "node": node,
            "fnLoadComplete": fnLoadComplete
          }

          //timeout -- if more than 7 seconds go by, we'll abort
          //the transaction and assume there are no children:
          //timeout: 21000
        };

        //With our callback object ready, it's now time to
        //make our XHR call using Connection Manager's
        //asyncRequest method:
        YAHOO.util.Connect.asyncRequest('GET', sUrl, callback);
      }

      function ImportFromTree(Node)
      {
        lastClickIsTreeClick = Node;

        if (Node != null){
          Node.highlightState = 1;
          tree.render;
        }

        var handleSuccess = function(o){   
          
          if(o.responseText !== undefined){ 
             if (o.statusText == 'OK')
             {
                // update main frame and point to deploy tab.
                var f=top.frames['main'];
                
                if (f) {
                 f.window.tabView.set('activeIndex',6)                
                 //f.window.loadTab(6);
                 //f.location= '/ws_roxieconfig/NewRoxieConfigUI?ShowDeployTab=1';
                }
  
                clearNavTreeSelects();

             }
          }
               
        }   

        var handleFailure = function(o){
        }
           
        var callback =   
        {   
           success:handleSuccess,   
           failure: handleFailure,   
           argument: ['foo','bar']   
        }; 
        
        // create xml from tree.
        var postData = createXmlArg();
        YAHOO.util.Connect.initHeader("Content-Type", "application/x-www-form-urlencoded");
        var request = YAHOO.util.Connect.asyncRequest('POST', '/ws_roxieconfig/NavMenuEvent?cmd=DeployMultiple', callback, 'Cmd=DeployMultiple&XmlArg=' + postData); 
      }

      function createXmlArg() {

        // Select this node
        var rootNode = tree.getRoot();
        var xmlArgs = '';
        if (rootNode.children.length > 0) {
          // Select All it's children...
          if (rootNode.highlightState > 0)
          {
            xmlArgs = '<EspNavigationData>' + selectChildNodes(rootNode) + '</EspNavigationData>';
          }
        }


        //var hiLit = Node.tree.getNodesByProperty('highlightState',1);   
        
        //var xmlArgs = '<EspNavigationData><Folder name="Attribute Servers"><DynamicFolder name="dataland" params="type=repository&amp;subtype=as&amp;name=dataland&amp;netAddress=http://jprichard-vm:8145"><DynamicFolder name="stu_test" params="type=repository&amp;subtype=am&amp;netAddress=http://jprichard-vm:8145&amp;module=stu_test&amp;name=dataland"><Link name="Addition3" selected="true"/></DynamicFolder></DynamicFolder></Folder></EspNavigationData>';
        // if the node is a module then load it's children and then traverse up
        // otherwise wrap itself and traverse up.

        xmlArgs = escape(xmlArgs);
        xmlArgs = xmlArgs.replace(/'+'/g, '%20')
        return xmlArgs;
      }


      function selectChildNodes(Node){
        var xmlChildArgs = '';
        for(var i=0;i<Node.children.length;i++)
        {
          if (Node.children[i].highlightState > 0)
          {
            xmlChildArgs = xmlChildArgs + selectChildNodes(Node.children[i]);
          }
        }
        if (Node.parent == null) {
          return xmlChildArgs;
        }
        //return '<' + Node.data.elementType + ' name="' + Node.label + '" ' + (Node.data.params != null && Node.data.params != '' ? 'params="' + Node.data.params.replace(/&/g, '&amp;') + '" ': '') + (Node.data.elementType=='Link' ? 'selected="true" ': '') + '>' + xmlChildArgs + '</' + Node.data.elementType + '>';
        return '<' + Node.data.elementType + ' name="' + Node.label + '" ' + (Node.data.params != null && Node.data.params != '' ? 'params="' + Node.data.params.replace(/&/g, '&amp;') + '" ': '') + (Node.highlightState == 1 ? 'selected="true" ': '') + '>' + xmlChildArgs + '</' + Node.data.elementType + '>';
      }

      /*
          "contextmenu" event handler for the element(s) that
          triggered the display of the ContextMenu instance - used
          to set a reference to the TextNode instance that triggered
          the display of the ContextMenu instance.
      */

      function onNavTreeContextMenu(p_oEvent, eventargs) {

          lastClickIsTreeClick = true;
          var oTarget = this.contextEventTarget;

          /*
               Get the TextNode instance that that triggered the
               display of the ContextMenu instance.
          */

          oCurrentTextNode = tree.getNodeByElement(oTarget);

          if (!oCurrentTextNode) {
              // Cancel the display of the ContextMenu instance.

              this.cancel();

          } else {
             if (!eventargs[0].ctrlKey) {
                clearNavTreeSelects();
             }

            if (oCurrentTextNode.depth > 1) {
              oCurrentTextNode.highlight();
            }
          }

      }

      function deploy() {
        ImportFromTree(null);
      }

      function onImportTreeClick(oArgs) {
        tree.onEventToggleHighlight(oArgs);
      }

      function refreshNode() {
        tree.removeChildren(oCurrentTextNode);
        oCurrentTextNode.unhighlight();
        oCurrentTextNode.expand();
      }

      function clearNavTreeSelects() {
        var hiLit = tree.getNodesByProperty('highlightState',1);
          if (YAHOO.lang.isNull(hiLit)) { 
        } else {
          for (var i = 0; i < hiLit.length; i++) {
            if (hiLit[i].depth == 3) {
              hiLit[i].unhighlight();
            }
          }
        }
        hiLit = tree.getNodesByProperty('highlightState',2);
          if (YAHOO.lang.isNull(hiLit)) { 
        } else {
          for (var i = 0; i < hiLit.length; i++) {
            if (hiLit[i].depth == 2) {
              hiLit[i].unhighlight();
            }
          }
        }
        hiLit = tree.getNodesByProperty('highlightState',1);
          if (YAHOO.lang.isNull(hiLit)) { 
        } else {
          for (var i = 0; i < hiLit.length; i++) {
              hiLit[i].unhighlight();
          }
        }
      }


      /*
          Instantiate a ContextMenu:  The first argument passed to the constructor
          is the id for the Menu element to be created, the second is an
          object literal of configuration properties.
      */


      var oNavTreeContextMenu; 

      /*
           Subscribe to the "contextmenu" event for the element(s)
           specified as the "trigger" for the ContextMenu instance.
      */
      var lastTreeNode;

      var onTreeClick = function(oArgs) {
         lastClickIsTreeClick = true;
         oCurrentTextNode = oArgs.node;
         if (!oArgs.event.ctrlKey && !oArgs.event.shiftKey) {
            clearNavTreeSelects();
         }
         if (oArgs.event.shiftKey) {
           if (oArgs.node.depth == 3) {
             //debugger;
             // check if the lastTreeNode and the current node have the same parent.
             if (lastTreeNode.parent == oArgs.node.parent) {
                var markNodes = false;
                for(var i=0;i<lastTreeNode.parent.children.length;i++) {
                  if ((lastTreeNode.parent.children[i] == lastTreeNode || lastTreeNode.parent.children[i] == oArgs.node) && !markNodes) {
                     markNodes = true;
                  } else {
                    if (markNodes) {
                        lastTreeNode.parent.children[i].highlight(); 
                        if (lastTreeNode.parent.children[i] == lastTreeNode || lastTreeNode.parent.children[i] == oArgs.node) {
                           break;
                        }
                    }
                  }
                }
                tree.clearTextSelection();
             }
             return false;
           }
         }
         
         if (oArgs.node.depth == 3) {
           lastTreeNode = oArgs.node;
         }
         var retVal = tree.onEventToggleHighlight(oArgs);
         if (oArgs.event.ctrlKey) {
            tree.clearTextSelection();
         }
         return retVal;
      };

      ]]></xsl:text>
        <xsl:call-template name="insertJavascriptTree"/>
      </script>

    </body>
    </html>
</xsl:template>

<xsl:template name="insertJavascriptTree">

  function buildTree() {
  tree = new YAHOO.widget.TreeView("espNavTree");

  tree.subscribe('dblClickEvent',function(oArgs) {
  ImportFromTree(oArgs.Node);
  });

  tree.locked = false;
  tree.setNodesProperty("propagateHighlightUp", true);
  tree.setNodesProperty("propagateHighlightDown", true);

  var root = tree.getRoot();
  root.setNodesProperty("propagateHighlightUp", true);
  root.setNodesProperty("propagateHighlightDown", true);

  tree.setDynamicLoad(loadNodeData, currentIconMode);
  tree.subscribe("clickEvent", onTreeClick);

  var serverNode, folderNode;
  <xsl:for-each select="Folder">
      serverNode = new YAHOO.widget.TextNode({label: "<xsl:value-of select="@name"/>", expanded:true, dynamicLoadComplete:false}, root);
      serverNode.data = { elementType: 'Folder', params: '' };
      serverNode.setNodesProperty("propagateHighlightUp",true);
      serverNode.setNodesProperty("propagateHighlightDown",true);
      <xsl:for-each select="DynamicFolder">
        folderNode = new YAHOO.widget.TextNode({label: "<xsl:value-of select="@name"/>"}, serverNode);
        //folderNode.labelStyle = "icon-doc";
        folderNode.setNodesProperty("propagateHighlightUp",true);
        folderNode.setNodesProperty("propagateHighlightDown",true);
        folderNode.data = { elementType: 'DynamicFolder', params: '<xsl:value-of select="@params" disable-output-escaping="yes"/>' };
      </xsl:for-each>
    </xsl:for-each>
    tree.render();
    
    var oNavTreeContextMenu = new YAHOO.widget.ContextMenu(
        "navtreecontextmenu",
        {
            trigger: "espNavTree",
            lazyload: true,
            itemdata: [
                { text: "Import", onclick: {fn: ImportFromTree}  },
                { text: "Clear Selections", onclick: {fn: clearNavTreeSelects}},
                { text: "Refresh", onclick: {fn: refreshNode}}
            ]
        }
    );
    
    oNavTreeContextMenu.subscribe("triggerContextMenu", onNavTreeContextMenu);

  }
</xsl:template>


</xsl:stylesheet>
