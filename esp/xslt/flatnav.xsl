<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
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
/*
  <xsl:copy-of select="/*" />
*/

//Here's the code that will set up our Left Nav instance. 

var serverTypes = new Array();

(function(){
            <xsl:call-template name="insertMenu"/>
  <xsl:text disable-output-escaping="yes"><![CDATA[
  document.getElementById('left1').innerHTML = '<br /><table cellpadding="2" cellspacing="2"><tr><td>Type:</td><td><div id="serverTypes"></div></td></tr><tr><td>Server:</td><td><div id="servers"></div></td></tr><tr id="modulerow"><td>Module:</td><td><div id="moduleslist"></div></td></tr><tr><td>Filter:</td><td id="navlistfilter" style="display:none;"></td></tr></table><div id="navlist" style="display:none;"></div>';
  ]]></xsl:text>
  createServerTypeList('serverTypes');
  createServersList('servers', '0');
  //loadNodeData(serverTypes[0].servers[0].params);
  })();

  <xsl:text disable-output-escaping="yes"><![CDATA[

var serverType = 0;
var server = 0;
var selectedModuleName;
var selectedModuleParams;

var oNavTreeContextMenu = new YAHOO.widget.ContextMenu(
    "navtreecontextmenu",
    {
        trigger: "navlist",
        lazyload: true,
        itemdata: [
            { text: "Import", onclick: {fn: ImportFromTree}  },
            { text: "Clear Selections", onclick: {fn: clearNavTreeSelects}}
        ]
    }
);


function onNavTreeContextMenu(p_oEvent) {

    var oTarget = this.contextEventTarget;

    /*
         Get the TextNode instance that that triggered the
         display of the ContextMenu instance.
    */

    //debugger;
    // 
    oCurrentRow = getRowElement(oTarget, dtImportQueries);
    if (!oCurrentRow) {
        // Cancel the display of the ContextMenu instance.
        this.cancel();
    } else {
      dtImportQueries.selectRow(oCurrentRow);
    }

}

function getRowElement(el, dt) {

    var p = el;

    do {

        if (p && p.id !== '') {
           var rec = dt.getRow(p.id);
           if (rec) {
            return rec;
           }
        }

        p = p.parentNode;

        if (!p) {
            break;
        }

    } 
    while (p.tagName.toLowerCase() !== "body");

    return null;
}

oNavTreeContextMenu.subscribe("triggerContextMenu", onNavTreeContextMenu);

function clearNavTreeSelects() {
  var selectedRows = dtImportQueries.getSelectedRows();
  for(var i=0;i<selectedRows.length;i++)
  {
    dtImportQueries.unselectRow(selectedRows[i]);
  }
}

function ImportFromTree() {
  var handleSuccess = function(o){   
    
    if(o.responseText !== undefined){ 
       if (o.statusText == 'OK')
       {
          //alert('Import OK');
          tabView.deselectTab(6);
          tabView.selectTab(6);
          clearNavTreeSelects();

       }
    }
         
  }   

  var handleFailure = function(o){ 
     debugger;  
  }
     
  var callback =   
  {   
     success:handleSuccess,   
     failure: handleFailure,   
     argument: ['foo','bar']   
  }; 
  
  // create xml from tree.
  var postData = createXmlArg();
  var request = YAHOO.util.Connect.asyncRequest('POST', '/ws_roxieconfig/NavMenuEvent?cmd=DeployMultiple&Cmd=DeployMultiple&XmlArg=' + postData, callback); 

}

function createXmlArg() {

  // Select this node

  // attributes from the list...
  xmlArgs = selectAttributes();

  // Module if there is one....
  if (modulesLoaded) {
    xmlArgs = '<DynamicFolder name="' + selectedModuleName + '" params="' + selectedModuleParams.replace(/&/g, '&amp;') + '">' + xmlArgs + '</DynamicFolder>';
  }
  // Server

  xmlArgs = '<DynamicFolder name="' + serverTypes[serverType].servers[server].name + '" params="' + serverTypes[serverType].servers[server].params.replace(/&/g, '&amp;') + '">' + xmlArgs + '</DynamicFolder>';

  // Server Type

  xmlArgs = '<Folder name="' + serverTypes[serverType].name + '">' + xmlArgs + '</Folder>';
  // wrap in root.
  var xmlArgs = '<EspNavigationData>' + xmlArgs + '</EspNavigationData>';
  return escape(xmlArgs);
}


function selectAttributes() {
  var attributesStr = '';
  var selectedRows = dtImportQueries.getSelectedRows();
  for(var i=0;i<selectedRows.length;i++) {
    var rec = dtImportQueries.getRecord(selectedRows[i]);
    var label = rec.getData('label');
    var params = rec.getData('params');
    attributesStr += '<Link name="' + label + '" ' + 'selected="true"></Link>';
  }
  return attributesStr;
}

//oNavTreeContextMenu.subscribe("triggerContextMenu", onNavTreeContextMenu);

function createServerTypeList(elementId) {
  var selectStr = '<select id="serverTypesSelect" style="width:150px;" onchange="createServersList(\'servers\', this.value)">';
  for(var i=0;i<serverTypes.length;i++) {
    selectStr += '<option value="' + i + '">' + serverTypes[i].name + '</option>';
  }
  selectStr += '</select>';
  var serverTypesEl = document.getElementById(elementId);
  if (serverTypesEl) {
     serverTypesEl.innerHTML = selectStr;
  }
}

function createServersList(ElementId, ServerTypeId) {
  serverType = parseInt(ServerTypeId);
  var selectStr = '<select id="serversSelect" style="width:150px;" onchange="loadModules(this.value)">';
  for(var i=0;i<serverTypes[serverType].servers.length;i++) {
    selectStr += '<option value="' + i + '">' + serverTypes[serverType].servers[i].name + '</option>';
  }
  selectStr += '</select>';
  var serverTypesEl = document.getElementById(ElementId);
  if (serverTypesEl) {
     serverTypesEl.innerHTML = selectStr;
  }
  loadModules('0');
}

function createModulesList(ElementId) {
  var selectStr = '<select id="modulesSelect" style="width:150px;" onchange="onModuleSelection(this.value)">';
  for(var i=0;i<modules.length;i++) {
    selectStr += '<option value="' + i + '">' + modules[i].name + '</option>';
  }
  selectStr += '</select>';
  var modulesEl = document.getElementById(ElementId);
  if (modulesEl) {
     modulesEl.innerHTML = selectStr;
     document.getElementById('modulesSelect').focus();
  }
  
  onModuleSelection('0');
}

function loadModules(ServerId)
{
  modulesLoaded = false;
  document.getElementById('moduleslist').innerHTML = '<img src="/esp/files/yui/build/treeview/assets/skins/sam/loading.gif" style="height:18px;" />';
  document.getElementById('moduleslist').style.display = "block";
  server = parseInt(ServerId);
  loadNodeData(serverTypes[serverType].servers[server].params);
}

function onModuleSelection(ModuleId)
{
  var moduleId = parseInt(ModuleId);

  selectedModuleName = modules[moduleId].name;
  selectedModuleParams = modules[moduleId].params;
  loadNodeData(selectedModuleParams);
}

function filterNavQueries(Filter)
{
  if (Filter.length > 0) {
    dtImportQueries.Filter(Filter, "label");
  }
  else  {
    dtImportQueries.ClearFilters();
  }
}

function checkNavListForEnter() {
    if (window.event && window.event.keyCode == 13)
    {
      filterNavQueries(document.getElementById('navlistfilterField').value);
    }
}

function onSelectImportQuery(oArgs) {
    dtImportQueries.onEventSelectRow(oArgs);
    dtImportQueries.clearTextSelection();
}

var oFoldersAC;
var dsImportQueries, dtImportQueries;
var modulesLoaded;
var modules; 

function loadNodeData(Params)  {

  document.getElementById('navlist').innerHTML = '<img src="/esp/files/yui/build/treeview/assets/skins/sam/loading.gif" style="height:18px;" />';
  var sUrl = "/esp/navdata?" + Params;
  //prepare our callback object
  var callback = {

    //if our XHR call is successful, we want to make use
    //of the returned data and create child nodes.
    success: function(oResponse) {

      var xmlDoc = oResponse.responseXML;
      var folderNodes = xmlDoc.getElementsByTagName("DynamicFolder");
      //var folderNodes = xmlDoc.getElementsByTagName("Link");
      if (folderNodes.length)
      {
        modulesLoaded = true;
        modules = new Array();
        for(var i = 0; i < folderNodes.length; i++)
        {
          modules[i] = { name: folderNodes[i].getAttribute("name"), params: folderNodes[i].getAttribute("params") };
        }
        document.getElementById('modulerow').style.display = "block";
        createModulesList('moduleslist');
      }

      folderNodes = xmlDoc.getElementsByTagName("Link");
      if (folderNodes.length)
      {
        if (!modulesLoaded) {
          document.getElementById('modulerow').style.display = "none";
        }
        document.getElementById('navlistfilter').innerHTML = '<input type="text" id="navlistfilterField" onchange="filterNavQueries(this.value)" onkeypress="checkNavListForEnter()" />';

        document.getElementById('navlist').style.display = 'none';
        document.getElementById('navlist').innerHTML = '<div id="dtimportqueries"></div>';
        var importQueries = new Array();
        for(var i = 0; i < folderNodes.length; i++)
        {
          importQueries[i] = { label: folderNodes[i].getAttribute("name"), params: folderNodes[i].getAttribute("params") };
        }

        dsImportQueries = new YAHOO.util.DataSource(importQueries);   
        // Optional to define fields for single-dimensional array    
        dsImportQueries.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;  
        dsImportQueries.responseSchema = {fields : ["label", "params"]};   

        var myColumnDefs = [   
             {key:"label", label: selectedModuleName}
         ];   
   
         dtImportQueries = new YAHOO.widget.FullDataView("dtimportqueries",   
                 myColumnDefs, dsImportQueries);   
         dtImportQueries.subscribe("rowMouseoverEvent", dtImportQueries.onEventHighlightRow);
         dtImportQueries.subscribe("rowMouseoutEvent", dtImportQueries.onEventUnhighlightRow);
         dtImportQueries.subscribe("rowClickEvent", onSelectImportQuery);   
         document.getElementById('navlistfilter').style.display = 'block';
         document.getElementById('navlist').style.display = 'block';
      }
      //When we're done creating child nodes, we execute the node's
      //loadComplete callback method which comes in via the argument
      //in the response object (we could also access it at node.loadComplete,
      //if necessary):
    },

    //if our XHR call is not successful, we want to
    //fire the TreeView callback and let the Tree
    //proceed with its business.
    failure: function(oResponse) {
      debugger;
    },
    argument: { foo:"foo", bar:"bar" }
  };

  var getMenus = YAHOO.util.Connect.asyncRequest("GET",
            sUrl,
            callback);
}

]]></xsl:text>
  
</xsl:template>

<xsl:template name="insertMenu">
  var navContainer = document.getElementById('left1');
  if (navContainer) {

  // Create the initial folder objects.
  var i = 0;
  var j = 0;
  <xsl:for-each select="Folder">
    j = <xsl:value-of select="position()-1" />;
    serverTypes[j] = { name: '<xsl:value-of select="@name"/>', servers: new Array() };
    <xsl:for-each select="DynamicFolder">
    i = <xsl:value-of select="position()-1" />;
    serverTypes[j].servers[i] = {name: '<xsl:value-of select="@name"/>', params: '<xsl:value-of select="@params" disable-output-escaping="yes"/>'};
    </xsl:for-each>
  </xsl:for-each>
  
  }

</xsl:template>

</xsl:stylesheet>
