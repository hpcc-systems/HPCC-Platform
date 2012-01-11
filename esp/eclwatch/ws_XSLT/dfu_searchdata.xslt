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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:xs="http://www.w3.org/2001/XMLSchema"
  xmlns:msxsl="urn:schemas-microsoft-com:xslt">

  <xsl:param name="showColumns" select="/DFUSearchDataResponse/ShowColumns"/>
    <xsl:param name="oldFile" select="/DFUSearchDataResponse/LogicalName"/>
    <xsl:param name="chooseFile" select="/DFUSearchDataResponse/ChooseFile"/>
  <xsl:param name="pageSize" select="/DFUSearchDataResponse/PageSize"/>
  <xsl:param name="rowStart" select="/DFUSearchDataResponse/Start"/>
  <xsl:param name="rowCount" select="/DFUSearchDataResponse/Count"/>
  <xsl:param name="rowStart0" select="/DFUSearchDataResponse/StartForGoback"/>
  <xsl:param name="rowCount0" select="/DFUSearchDataResponse/CountForGoback"/>
  <xsl:param name="columnCount" select="/DFUSearchDataResponse/ColumnCount"/>
  <xsl:param name="Total0" select="/DFUSearchDataResponse/Total"/>
  <xsl:param name="filterBy0" select="/DFUSearchDataResponse/FilterForGoBack"/>
  <xsl:param name="logicalName0" select="/DFUSearchDataResponse/LogicalName"/>
  <xsl:param name="openLogicalName0" select="/DFUSearchDataResponse/OpenLogicalName"/>
  <xsl:param name="schemaOnly" select="/DFUSearchDataResponse/SchemaOnly"/>
  <xsl:param name="cluster" select="/DFUSearchDataResponse/Cluster"/>
  <xsl:param name="clusterType" select="/DFUSearchDataResponse/ClusterType"/>
  <xsl:param name="file" select="/DFUSearchDataResponse/File"/>
  <xsl:param name="key" select="/DFUSearchDataResponse/Key"/>
  <xsl:param name="selectedKey" select="/DFUSearchDataResponse/SelectedKey"/>
  <xsl:param name="roxieSelections" select="/DFUSearchDataResponse/RoxieSelections"/>
  <xsl:param name="autoUppercaseTranslation" select="/DFUSearchDataResponse/AutoUppercaseTranslation"/>
  <xsl:param name="disableUppercaseTranslation" select="/DFUSearchDataResponse/DisableUppercaseTranslation"/>
  <xsl:param name="msgToDisplay" select="/DFUSearchDataResponse/MsgToDisplay"/>

  <xsl:variable name="debug" select="0"/>
  <xsl:variable name="stage1Only" select="0"/>
  <!--for debugging: produce intermediate nodeset only-->
  <xsl:variable name="stage2Only" select="0"/>
  <!--for debugging: process intermediate nodeset when fed as input-->
  <xsl:variable name="filePath">
    <xsl:choose>
      <xsl:when test="$debug">c:/development/bin/debug/files</xsl:when>
      <xsl:otherwise>/esp/files_</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:output method="html"/>
  <xsl:include href="dfusearchresult.xslt"/>
  <xsl:template match="DFUSearchDataResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files_/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files_/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files_/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files_/css/espdefault.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <xsl:text disable-output-escaping="yes"><![CDATA[
        <script type="text/javascript" src="/esp/files/scripts/prototype.js"></script>
        <script type="text/javascript" src="/esp/files/scripts/scriptaculous.js"></script>
        <script type="text/javascript" src="/esp/files/scripts/dragdrop.js"></script>
        ]]></xsl:text>

        <script language="JavaScript1.2">

          var Columns = new Array();
          Columns[0] = createColumn(0, 'rowid', 0);


          var ClusterNames=new Array();
          ClusterNames[0] = '';

          var ClusterTypes=new Array();
          ClusterTypes[0] = '';

          var FilesArray = new Array();
          FilesArray[0] = '';

          var parentWindowIsSearchData = true;
          
          var cluster = '<xsl:value-of select="$cluster"/>';
          var clusterType = '<xsl:value-of select="$clusterType"/>';
          var file = '<xsl:value-of select="$file"/>';
          var key = '<xsl:value-of select="$key"/>';
          var selectedKey = '<xsl:value-of select="$selectedKey"/>';
          var parentName = '<xsl:value-of select="ParentName"/>';
          var msgToDisplay = "<xsl:value-of select="$msgToDisplay"/>";
          <xsl:if test="$rowStart">
          var start=<xsl:value-of select="$rowStart"/>;
          </xsl:if>

          var showcolumns='<xsl:value-of select="$showColumns"/>';
                    var oldfile='<xsl:value-of select="$oldFile"/>';
                    var choosefile='<xsl:value-of select="$chooseFile"/>';
          var layoutLoaded = false;
          var disableUppercaseTranslation = <xsl:value-of select="$disableUppercaseTranslation"/>;
          <xsl:if test="Total">
          layoutLoaded = true;
          </xsl:if>

          var filterBy = null;
          var max_name_len = 3;
          var max_value_len = 4;
          var fetchrows = 20;
          <xsl:if test="PageSize">
          fetchrows = <xsl:value-of select="PageSize" />;
          </xsl:if>

          var roxieSelections = <xsl:value-of select="$roxieSelections" />;

          var start0='<xsl:value-of select="$rowStart0"/>';
            
          var count0='<xsl:value-of select="$rowCount0"/>';
          var total0='<xsl:value-of select="$Total0"/>';
          var name0='<xsl:value-of select="$openLogicalName0"/>';
          <xsl:if test="string-length($openLogicalName0) &lt; 1 and string-length($key) &gt; 0">
          name0 = '<xsl:value-of select="$key"/>';
          </xsl:if>
          var filterby0='<xsl:value-of select="$filterBy0"/>';
          var columncount='<xsl:value-of select="$columnCount"/>';
          var choosefile='<xsl:value-of select="$chooseFile"/>';
          var rbid = 0;
          var loadFromSearchRoxieFiles = false;

          <xsl:if test="string-length(OpenLogicalName) &gt; 0 and string-length(Key) = 0">
          loadFromSearchRoxieFiles = true;
          </xsl:if>
          
          <xsl:if test="ChooseFile = '1'">
                    var checkup = window.setInterval("checkChange();", 100); 

                    function checkChange() 
                    { 
                      CheckFileName(document.getElementById("OpenLogicalName")); 
                    } 
                    </xsl:if>

          function onLoad()
          {
            <xsl:text disable-output-escaping="yes"><![CDATA[
            if (msgToDisplay.length > 0) {
              if (key != selectedKey) {
                var msg = document.getElementById('msgToDisplay');
                if (msg) {
                    msg.innerHTML = msgToDisplay.replace(key, selectedKey);
                }
              }
            }

            if (parent.window.parentWindowIsSearchData) {
              if (msgToDisplay.length > 0) {
                if (key != selectedKey) {
                  var msg = parent.document.getElementById('msgToDisplay');
                  if (msg) {
                      msg.innerHTML = msgToDisplay.replace(key, selectedKey);
                  }
                }
              }

              var parentBlock = parent.document.getElementById('DataFields');
              var thisBlock = document.getElementById('DataFields');
              parentBlock.innerHTML = thisBlock.innerHTML;
              parentBlock = parent.document.getElementById('bodyTable');
              thisBlock = document.getElementById('bodyTable');
              parentBlock.innerHTML = thisBlock.innerHTML;
            } else {
            
            ]]></xsl:text>
            
                <xsl:if test="ColumnsHidden/ColumnHidden">
                  var colPosition = 0; // rowid is the first col
                  var parentColumn;
                  <xsl:for-each select="//*/xs:element[@name='Dataset']/*/*/xs:element[@name='Row']/*/*/xs:element">
                    parentColumn = createColumn(colPosition, '<xsl:value-of select="@name"/>', '<xsl:value-of select="@type"/>');
                    Columns[<xsl:value-of select="position()"/>] = parentColumn;
                    <xsl:if test="@type">
                    colPosition++;  
                    </xsl:if>
                    <xsl:for-each select="./*/*/xs:element">
                      <xsl:choose>
                        <xsl:when test="@type">
                          <xsl:variable name="childposition" select="position()-1"/>
                          parentColumn.Columns[<xsl:value-of select="$childposition"/>] = createColumn(colPosition, '<xsl:value-of select="@name"/>', '<xsl:value-of select="@type"/>');
                          colPosition++;
                        </xsl:when>
                        <xsl:otherwise>
                          <xsl:for-each select="./*/*/xs:element">
                            <xsl:variable name="childposition" select="position()-1"/>
                            parentColumn.Columns[<xsl:value-of select="$childposition"/>] = createColumn(colPosition, '<xsl:value-of select="@name"/>', '<xsl:value-of select="@type"/>');
                            colPosition++;
                          </xsl:for-each>
                        </xsl:otherwise>
                      </xsl:choose>
                    </xsl:for-each>
                  </xsl:for-each>
                  <xsl:for-each select="ColumnsHidden/ColumnHidden">
                    <xsl:if test="ColumnSize = 0">
                      menuHandler(<xsl:value-of select="position()" />, false);
                    </xsl:if>
                  </xsl:for-each>
                </xsl:if>
                loadClusters();

                <xsl:text disable-output-escaping="yes"><![CDATA[
                if (key.length == 0 && name0.length > 0)
                {
                  key = name0;
                }
                ]]></xsl:text>
              <xsl:if test="$roxieSelections=0">
                fileLookupFromKey()
              </xsl:if>
            }
          }

          function createColumn(Position, Name, Type)
          {
          var objColumn = new Object();
          objColumn.Position = Position;
          objColumn.Name = Name;
          objColumn.Type = Type;
          objColumn.Columns = new Array();
          return objColumn;
          }

          function onRowLimitChange(RowLimit)
          {
          fetchrows = RowLimit;
          }

          function onNextPage()
          {
          var startObj = document.getElementById('_start');
          if (startObj)
          {
          startObj.value = parseInt(start) + parseInt(fetchrows) + 1;
          onRBChanged(2);
          onFindClick();
          }
          }

          function onPrevPage()
          {
          var startObj = document.getElementById('_start');
          if (startObj)
          {
          startObj.value = parseInt(start) - parseInt(fetchrows) + 1;
          onRBChanged(2);
          onFindClick();
          }
          }

          function onFindClick(InputField)
          {
          var startIndex = 0;
          var startIndexCtl = document.getElementById("StartIndex");
          if (startIndexCtl)
          {
          startIndex = startIndexCtl.value;
          }
          var endIndex = 0;
          var endIndexCtl = document.getElementById("EndIndex");
          if (endIndexCtl)
          {
          endIndex = endIndexCtl.value;
          }
          var start = startIndex - 1;
          var count = endIndex - start;
          var name = keyToLoad;

          if(!name)
          {
          name = name0;
          if (!name) {
          alert('No file name is defined.');
          return;
          }
          }

          checkTextField(document.getElementById('dfucolumnsform'));

          var start = 0;
          var count = fetchrows;

          <xsl:if test="ColumnsHidden/ColumnHidden">
              <xsl:text disable-output-escaping="yes"><![CDATA[
            if (rbid > 1)
            {
              start = document.getElementById('_start').value - 1;
            }
            else if (rbid > 0) //last n records
            {
              start = total0 - count;
            }
            else //first n records
            {
              start = 0;
            }

            if(count>=10000)
            {
              alert('Count must be less than 10000');
              return false;
            }

            showcolumns = '';
            for (var i=1; i < Columns.length; i++)
                    {
                        var checkbox = document.getElementById('ShowColumns_' + i);
              if (checkbox)
              {
                          if (checkbox.checked)
                {
                              showcolumns = showcolumns + '/' + (i-1);
                }
              }
                    }
            ]]></xsl:text>
          </xsl:if>
          <xsl:text disable-output-escaping="yes"><![CDATA[
                    var url = "/WsDFU/DFUSearchData?ChooseFile=" + choosefile + "&OpenLogicalName=" + name0 +"&LogicalName=" + name0 +
            "&Start=" + start + "&Count=" + count +
            "&StartForGoback=" + start0 + "&CountForGoback=" + count0 + (parentName.length > 0 ? "&ParentName=" + parentName : "") + "&RoxieSelections=" + roxieSelections + "&DisableUppercaseTranslation=" + disableUppercaseTranslation;
            if (cluster) {
              url = url + '&Cluster=' + cluster;
            }
            if (clusterType) {
              url += '&ClusterType=' + clusterType;
            }
            if (file)
            {
              url = url + '&File=' + file;
            }
            if (key)
            {
              url = url + '&Key=' + key;
            }
            if (showcolumns.length > 0)
            {
              url = url + "&ShowColumns=" + showcolumns;
            }

            if (filterBy)
            {
              var newFilterBy = max_name_len.toString() + max_value_len.toString() + filterBy;
              // escape doesn't correctly encode + and / for some bizarre reason.
              url = url + "&FilterBy=" + escape(newFilterBy).replace("+", "%2B").replace("/", "%2F");
            }
            //url = url + "&FilterBy=" + max_name_len.toString() + max_value_len.toString() + filterBy;
            url = url + "&SchemaOnly=0";
            //alert(url);
            document.location.href=url;
            return;
           ]]></xsl:text>
          }

          <xsl:text disable-output-escaping="yes"><![CDATA[
                    function onOpenClick()
                    {
                        var name = document.getElementById("OpenLogicalName").value;
                        if(!name)
                        {
              name = name0;
              if (!name) {
                              alert('No file name is defined.');
                              return;
              }
                        }

                        document.location.href="/WsDfu/DFUGetDataColumns?ChooseFile=" + "0" + "&OpenLogicalName=" + name;
                        return;
                    }       

                    function trimAll(sString) 
                    {
                        while (sString.substring(0,1) == ' ')
                        {
                            sString = sString.substring(1, sString.length);
                        }
                        while (sString.substring(sString.length-1, sString.length) == ' ')
                        {
                            sString = sString.substring(0,sString.length-1);
                        }
                        return sString;
                    }

                    function toSpecialString(v, l)
                    {
                        var v_ret = "";
                        var v_len = v.length;
                        var v_str = v_len.toString();
                        var v_len1 = v_str.length;
                        while (v_len1 < l)
                        {
                            v_ret = v_ret.concat("0");
                            v_len1++;
                        }

                        v_ret = v_ret.concat(v_str);
                        return v_ret;
                    }

                    function checkTextField(o)
                    {
                        if (o.tagName=='INPUT' && o.type=='text' && o.value !='')
                        {
                            var oname = trimAll(o.name);
                            var ovalue = trimAll(o.value);
                            var len_name = toSpecialString(oname, max_name_len);
                            var len_value = toSpecialString(ovalue, max_value_len);

                            if (filterBy)
                                filterBy = filterBy + len_name + len_value + oname + ovalue; 
                            else
                                filterBy = len_name + len_value + oname + ovalue; 
                        }

                        var ch=o.children;
                        if (ch)
                            for (var i in ch)
                                checkTextField(ch[i]);
                         return;
                    }

          function checkEnter(InputField, e) {
            if (!e) 
              { e = window.event; }
            if (e && e.keyCode == 13)
            {
              onFindClick(InputField);
            }
          }

          function checkFilterField(InputField, e) {
              if (InputField.value.length > 0) {
                 InputField.style.background = 'lightyellow';
              } 
              else {
                 InputField.style.background = '';
              }
              checkEnter(InputField, e);
          }

                    function CheckFileName(o)
                    {
                        if (o.value !='')
                        {
                            if (oldfile != '')
                            {
                                var ovalue0 = trimAll(oldfile);
                                var ovalue = trimAll(o.value);
                                var v_len0 = ovalue0.length;
                                var v_len = ovalue.length;
                                if ((v_len0 != v_len) || (ovalue0 != ovalue))
                                {
                                    document.getElementById("GetColumns").disabled = false;
                                }
                                else
                                {
                                    document.getElementById("GetColumns").disabled = true;
                                }
                            }
                            else
                            {
                                var ovalue = trimAll(o.value);
                                var v_len = ovalue.length;
                                if (v_len > 0)
                                {
                                    document.getElementById("GetColumns").disabled = false;
                                }
                                else
                                {
                                    document.getElementById("GetColumns").disabled = true;
                                }
                            }
                        }
                        else
                        {
                            document.getElementById("GetColumns").disabled = true;
                        }
                    }

          function toggleElement(ElementId, ClassName)
          {
              obj = document.getElementById(ElementId);
              explink = document.getElementById('explink' + ElementId);
              if (obj) 
              {
                if (obj.style.visibility == 'visible')
                {
                  obj.style.display = 'none';
                  obj.style.visibility = 'hidden';
                  if (explink)
                  {
                    explink.className = ClassName + 'expand';
                  }
                }
                else
                {
                  obj.style.display = 'inline';
                  obj.style.visibility = 'visible';
                  if (explink)
                  {
                    explink.className = ClassName + 'contract';
                  }
                  else
                  {
                    alert('could not find: explink' + ElementId);
                  }
                }
              }
          }

                    function onRBChanged(id)
                    {
                        rbid = id;
                    }

                    function go_back()
                    {
                        var showcolumns = '';
                        for (i= 0; i< columncount; i++)
                        {
                            var id = 'ShowColumns_i' + (i); //no checkbox exists for column 1 
                            var form = document.forms['control'];                      
                            var checkbox = form.all[id];                       
                            if (checkbox.checked)
                                showcolumns = showcolumns + '/' + i;
                        }
                        
                        var end0 = start0 + count0;
                        var url = "/WsDfu/DFUGetDataColumns?ChooseFile=" + choosefile + "&OpenLogicalName=" + name0 + 
                                "&StartIndex=" + start0 + "&EndIndex=" + end0;
                        if (showcolumns.length > 0)
                            url = url + "&ShowColumns=" + showcolumns;
                    
                        if (filterby0)
                            url = url + "&FilterBy=" + escape(filterby0);
                    
                        document.location.href=url;
                        return true;
                    }

                    function getElementByTagName(node, tag)
                    {
                        for (var child = node.firstChild; child; child=child.nextSibling)
                            if (child.tagName == tag)
                                return child;
                        return null;
                    }

          function selectAll(State)
          {
            for(var i=1;i<Columns.length;i++)
            {
              var col = document.getElementById('ShowColumns_' + i);
              if (col)
              {
                if (State == 1 && col.checked != true)
                {
                  col.checked = true;
                  menuHandler(i, true);
                }
                if (State == 0 && col.checked == true)
                {
                  col.checked = false;
                  menuHandler(i, true);
                }
              }
            }
            
          }

                    function menuHandler(ColumnIndex, skipCheckBoxUpdate)
                    {
            if (Columns[ColumnIndex].Columns.length > 0) // it's a parent column
            {
              for(var i=0;i<Columns[ColumnIndex].Columns.length;i++)
              {
                toggleColumn(Columns[ColumnIndex].Columns[i].Position+1, true);
              }
            }
            else
            {
              toggleColumn(Columns[ColumnIndex].Position+1, skipCheckBoxUpdate);
            }
                    }

          function toggleColumn(ColumnIndex, skipCheckBoxUpdate)
          {
                        var col = document.getElementById('_col_' + ColumnIndex);
              if (col)
              {
                          var show = col.style && col.style.display && col.style.display == 'none' || col.style.visibility == 'collapse'; //show if hidden at present
                if (isFF) {
                  col.style.visibility = show ? 'visible' : 'collapse';
                } else {
                            col.style.display = show ? 'block' : 'none';
                }
                          if (!skipCheckBoxUpdate)
                          {
                              var checkbox = document.getElementById('ShowColumns_' + ColumnIndex);
                  if (checkbox)
                  {
                                checkbox.checked = checkbox.checked ? false : true;
                  }
                          }
              }
          }
  
                    function onRowCheck(checked)
                    {
            var dataForm = document.forms['data'];
            if (dataForm)
            {
                          var table = document.forms['data'].all['dataset_table'];
                          if (table != NaN)
                          {
                              var colGroup  = getElementByTagName(table, 'COLGROUP');
                              for (i=1; i<=columncount; i++)
                              {
                                  var col = colGroup.children[i];
                                      col.style.display = checked ? 'block' : 'none';

                              }
                          }
            }
                    }

                    function getElementByTagName(node, tag)
                    {
                        for (var child = node.firstChild; child; child=child.nextSibling)
                            if (child.tagName == tag)
                                return child;
                        return null;
                    }

                    function onRowCheck(checked)
                    {
            var dataForm = document.forms['data'];
            if (dataForm)
            {
                          var table     = document.forms['data'].all['dataset_table'];
                          if (table != NaN)
                          {
                              var colGroup  = getElementByTagName(table, 'COLGROUP');
                              for (i=1; i<=columncount; i++)
                              {
                                  var col = colGroup.children[i];
                                      col.style.display = checked ? 'block' : 'none';

                              }
                          }
            }
                    }
          
          function loadClusters()
          {
              var clusterframe = document.getElementById('ClusterFrame');
              if (clusterframe)
              {
                          clusterframe.src = '/WsRoxieQuery/QueryClusters';
              }
          }

          function onClusterSelect(ClusterId)
          {
              cluster = ClusterNames[ClusterId];
              clusterType = ClusterTypes[ClusterId];
              var filesframe = document.getElementById('FilesFrame');
              if (filesframe)
              {
                resetCell('FilesCell', 'File');
                resetCell('KeysCell', 'Key');
                resetCell('ParentCell', 'Parent');
                        filesframe.src = '/WsRoxieQuery/QueryOriginalFiles?ClusterName=' + ClusterNames[ClusterId] + '&ClusterType=' + ClusterTypes[ClusterId];
              }
          }

          function onFilesSelect(FileId)
          {
            if (FilesArray[FileId] != file)
            {
              msgToDisplay = '';
              parentName = '';
              name0 = '';
              key = '';
              resetCell('ParentCell', 'Parent');
              layoutLoaded = false;
            }
            file = FilesArray[FileId];
            var keysframe = document.getElementById('KeysFrame');
            if (keysframe)
            {
                    keysframe.src = '/WsRoxieQuery/QueryIndexesForOriginalFile?ClusterName=' + cluster + '&ClusterType=' + clusterType + '&FileName=' + file;
            }

          }

          function onFilesSelectByName(FileName)
          {
            for(var i=0;i<FilesArray.length;i++)
            {
              if (FilesArray[i] == FileName)
              {
                var selectList = document.getElementById('FilesSelect');
                if (selectList)
                {
                    if (!parent.window.loadFromSearchRoxieFiles)
                    {
                      selectList.selectedIndex = i - 1;
                    }
                }
                onFilesSelect(i);
                return;
              }
            }
          }

          function fileLookupFromKey()
          {
              var fileLookupFrame = document.getElementById('FileLookupFrame');
              if (key.length == 0 && name0.length > 0)
              {
                key = name0;
              }
              if (fileLookupFrame)
              {
                        fileLookupFrame.src = '/WsRoxieQuery/GetParentFiles?Cluster=' + cluster + '&ClusterType=' + clusterType + '&LogicalName=' + key;
              }
          }

          function onKeysSelect(KeyName)
          {
            keyToLoad = KeyName;
            var bodyBlock = document.getElementById('bodyTable');
            if (bodyBlock) {
              if (keyToLoad != key) {
                bodyBlock.style.display = 'none';
              } else {
                bodyBlock.style.display = 'block';
              }
            }
            //BrowseRoxieFile(keyToLoad);
          }

          function onParentSelect(ParentName)
          {
            newParentName = ParentName;
            var bodyBlock = document.getElementById('bodyTable');
            if (bodyBlock) {
              if (parentName != ParentName) {
                bodyBlock.style.display = 'none';
              } else {
                bodyBlock.style.display = 'block';
              }
            }
            //BrowseRoxieFile(key);
          }

          function resetCell(CellName, Prompt)
          {
            var CellObj = document.getElementById(CellName);
            if (CellObj)
            {
              CellObj.innerHTML = '<select id="' + Prompt + 'Select" name="' + Prompt + 'Select" style="width=100%"><option value="None">None</option></select>';
            }
          }

          function showObject(ObjName)
          {
              var Obj = document.getElementById(ObjName);
              if (Obj)
              {
                Obj.style.display = 'inline';

                Obj.style.visibility = 'visible';
              }
          }

          function hideObject(ObjName)
          {
              var Obj = document.getElementById(ObjName);
              if (Obj)
              {
                Obj.style.display = 'none';
                Obj.style.visibility = 'hidden';
              }
          }

          var layoutWnd;

          function viewRow(RowNumber)
          {
            if (layoutWnd)
            {
              layoutWnd.close();
            }
            //var strLayout = '<html><head><title>' + key + (RowNumber > 0 ? ' Row: ' + RowNumber : '') + '</title><link rel="stylesheet" type="text/css" href="/esp/files/css/list.css" /><link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" /><link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" /><link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" /></head><body><h4>'  + key + (RowNumber > 0 ? ' </h3><h4>Row: ' + RowNumber : '') + '</h4>';
            var strLayout = '<html><head><title>' + key + (RowNumber > 0 ? ' Row: ' + RowNumber : '') + '</title><link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" /></head><body><h4>'  + key + (RowNumber > 0 ? ' </h3><h4>Row: ' + RowNumber : '') + '</h4>';
            
            strLayout = strLayout + '<table class="results-table" cellspacing="0"><tr><th>Column Name</th><th>Type</th>' + (RowNumber > 0 ? '<th>Value</th><th>Length</th></tr>': '');
            var rowHtml = document.getElementById('row_' + RowNumber);
            if (rowHtml)
            {
              var children = rowHtml.childNodes;    
            }
            var k=1;
            for(var i=1; i<Columns.length; i++) 
            {       
              if (Columns[i].Columns.length>0)
              {
                strLayout = strLayout + '<tr class="rt0"><td>' + Columns[i].Name + '</td><td>' + (i > 0 ? Columns[i].Type.replace('xs:','') : '&nbsp;') + '</td>' + (RowNumber > 0 ? '<td>&nbsp;</td><td>&nbsp;</td>': '') + '</tr>';
                for(var j=0; j<Columns[i].Columns.length; j++)
                {
                  strLayout = strLayout + '<tr class="rt1" style="font-size:smaller;"><td>&nbsp;&nbsp;&nbsp;&nbsp;' + Columns[i].Columns[j].Name + '</td><td>' + (i > 0 ? Columns[i].Columns[j].Type.replace('xs:','') : '&nbsp;') + '</td>' + (RowNumber > 0 ? '<td>' + children[k].innerHTML + '</td><td>' + children[k].innerHTML.length + '</td>': '') + '</tr>';
                  k++;
                }
              }
              else
              {
                strLayout = strLayout + '<tr class="rt0"><td>' + Columns[i].Name + '</td><td>' + (i > 0 ? Columns[i].Type.replace('xs:','') : '&nbsp;') + '</td>' + (RowNumber > 0 ? '<td>' + children[k].innerHTML + '</td><td>' + children[k].innerHTML.length + '</td>': '') + '</tr>';
                k++;
              }
            }
            strLayout = strLayout + '</table></body></html>';

                    var wnd = window.open('about:blank', 'Details_' + RowNumber, 
                                            'toolbar=0,location=0,status=1,directories=0,menubar=0,' + 
                                            'scrollbars=1, resizable=1, width=640, height=480', true);

            if (RowNumber == 0)
            {
              layoutWnd = wnd;
            }

            //wnd.document.write('Clicked row number' + RowNumber);
            wnd.document.write(strLayout);

          }
          
          var keyToLoad = '';
          var newParentName = '';
          function BrowseRoxieFile(LogicalFile)
          {
            var id = LogicalFile;
            /*if (id.indexOf('~') > -1)
            {
              id = id.substring(1, id.length);
            }*/

            /*
            document.location.href='/WsDfu/DFUSearchData?OpenLogicalName=' + id + '&Cluster=' + cluster + '&ClusterType=' + clusterType + '&File=' + file + '&Key=' + key + ( newParentName.length > 0 ? '&ParentName=' + parentName : '') + '&SelectedKey=' + selectedKey + '&RoxieSelections=' + roxieSelections;
            */
              keyToLoad = LogicalFile;
              if (keyToLoad.length < 1) {
                 keyToLoad = key;
              }
              var datacolumnsframe = document.getElementById('DataColumnsFrame');
              if (datacolumnsframe)
              {
                          datacolumnsframe.src = '/WsDfu/DFUSearchData?OpenLogicalName=' + keyToLoad + '&Cluster=' + cluster + '&ClusterType=' + clusterType + '&File=' + file + '&Key=' + key + ( newParentName.length > 0 ? '&ParentName=' + parentName : '') + '&SelectedKey=' + selectedKey + '&RoxieSelections=' + roxieSelections;
              }

            //
          }

                    ]]></xsl:text>

        </script>
            </head>
            <body onload="nof5();onLoad()" class="yui-skin-sam">
                <xsl:choose>
                    <xsl:when test="string-length(LogicalName)">
                        <h3>View Data File: <xsl:value-of select="LogicalName"/></h3>
                    </xsl:when>
                    <xsl:otherwise>
                        <h3>View Data File</h3>
                    </xsl:otherwise>
                </xsl:choose>
        <div id="dfucolumnsform">
          <xsl:if test="string-length(LogicalName)">
            <input type="hidden" name="LogicalName" id="LogicalName" value="{LogicalName}"/>
          </xsl:if>
          <table width="100%">
            <tr>
              <td>
                <table width="100%">
                  <colgroup>
                    <col width="100px"/>
                    <col/>
                  </colgroup>
                  <xsl:if test="$roxieSelections=1">
                    <tr>
                      <td>Cluster:</td>
                      <td id="ClusterCell">
                        <select id="ClusterSelect" name="ClusterSelect" style="width:100%;">
                          <option value="None">None</option>
                        </select>
                      </td>
                    </tr>
                    <tr>
                      <td>File:</td>
                      <td id="FilesCell">
                        <select id="FileSelect" name="FileSelect" style="width:100%;">
                          <option value="None">None</option>
                        </select>
                      </td>
                    </tr>
                    <tr>
                      <td>Key:</td>
                      <td id="KeysCell">
                        <select id="KeySelect" name="KeySelect" style="width:100%;">
                          <option value="None">None</option>
                        </select>
                      </td>
                    </tr>
                  </xsl:if>
                  <tr>
                    <td>Parent:</td>
                    <td id="ParentCell" >
                      <select id="ParentSelect" name="ParentSelect" style="width:100%;">
                        <option value="None">None</option>
                      </select>
                    </td>
                  </tr>
                </table>
              </td>
              <td style="width=100px;">
                <xsl:if test="$roxieSelections=1">
                  <input name="loadfields" id="loadfields" type="button" value="Load Fields" style="width=100px;height=100px;" onclick="BrowseRoxieFile(keyToLoad);" disabled="disabled"/>
                </xsl:if>
              </td>
            </tr>
          </table>
          <br/>
          <div id="DataFields">
          <xsl:if test="DFUDataKeyedColumns1/DFUDataColumn[1]">
            <table>
              <tr>
                <td valign="top" width="160">
                  <A href="javascript:void(0)" onclick="toggleElement('Keyed', 'viewkey');" id="explinkKeyed" class="viewkeycontract">
                    Keyed Columns:
                  </A>
                </td>
              </tr>
            </table>
            <div id="Keyed" style="visibility:visible;">
              <xsl:for-each select="*[contains(name(), 'DFUDataKeyedColumns')]">
                <xsl:apply-templates select="DFUDataColumn" />
              </xsl:for-each>
            </div>
          </xsl:if>
          <xsl:if test="DFUDataNonKeyedColumns1/DFUDataColumn[1]">
            <xsl:variable name="nonkeyedstate">
              <xsl:choose>
                <xsl:when test="count(//DFUDataColumn[string-length(ColumnValue)&gt;0 and contains(name(..), 'DFUDataNonKeyedColumns')])">
                  <xsl:text>viewkeycontract</xsl:text>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:text>viewkeyexpand</xsl:text>
                </xsl:otherwise>
              </xsl:choose>
            </xsl:variable>
            <table>
              <tr>
                <td valign="top" width="160">
                  <A href="javascript:void(0)" onclick="toggleElement('NonKeyed', 'viewkey');" id="explinkNonKeyed" class="{$nonkeyedstate}">
                    Non-Keyed Columns:
                  </A>
                </td>
              </tr>
            </table>

            <xsl:variable name="nonkeyedstyle">
              <xsl:choose>
                <xsl:when test="count(//DFUDataColumn[string-length(ColumnValue)&gt;0 and contains(name(..), 'DFUDataNonKeyedColumns')])">
                  <xsl:text></xsl:text>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:text>display:none; visibility:hidden;</xsl:text>
                </xsl:otherwise>
              </xsl:choose>
            </xsl:variable>

            <div id="NonKeyed" style="{$nonkeyedstyle}">
              <xsl:for-each select="*[contains(name(), 'DFUDataNonKeyedColumns')]">
                <xsl:apply-templates select="DFUDataColumn" />
              </xsl:for-each>
            </div>
          </xsl:if>
          </div>
        </div>
                <xsl:choose>
                    <xsl:when test="number(RowCount)>0">
                        <xsl:if test="DFUDataKeyedColumns1/DFUDataColumn[1] and DFUDataNonKeyedColumns1/DFUDataColumn[1] and false">
                            <table>
                                <tr>
                                    <td>Records from:
                                    </td>
                                    <td>
                                        <input type="text" value="{$rowStart}" name="StartIndex" size="5"/>
                                    </td>
                                    <td>to:
                                    </td>
                                    <td>
                                        <input type="text" value="{EndIndex}" name="EndIndex" size="5"/>
                                    </td>
                                    <td>
                                        <input type="button" value="Retrieve data" class="sbutton" onclick="onFindClick()"/>
                                    </td>
                  <td>If the first key column is not specified, it may take a long time to retrieve the data.</td>
                </tr>
                            </table>
                            <br/><br/>
                        </xsl:if>
                    </xsl:when>
                    <xsl:when test="string-length(LogicalName)">
                        <table>
                            <tr>
                                <td>There is no data in this file.</td>
                            </tr>
                        </table>
                        <br/><br/>
                    </xsl:when>
                    <xsl:otherwise>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:if test="ChooseFile = '1'">
                    <table>
                        <tr>
                            <td>File Name:</td>
                            <td>
                                <input name="OpenLogicalName" size="80" type="text" value="{LogicalName}" onKeyUp="CheckFileName(this)"/>
                            </td>
                            <td>
                                <input type="button" id="GetColumns" value="Get columns" class="sbutton"  disabled="true" onclick="onOpenClick()"/>
                            </td>
                        </tr>
                    </table>
                </xsl:if>

        <xsl:variable name="From" select="Start+1"/>
        <xsl:variable name="To" select="Start+Count"/>

        <form>
          <h4>
            <xsl:call-template name="id2string">
              <xsl:with-param name="toconvert" select="Name"/>
            </xsl:call-template>

            <xsl:if test="string-length(Wuid)">
              <input type="hidden" name="Wuid" value="{Wuid}"/>
              <input type="hidden" name="Sequence" value="{Sequence}"/>
            </xsl:if>
            <xsl:if test="string-length(LogicalName)">
              <input type="hidden" name="LogicalName" value="{LogicalName}"/>
            </xsl:if>
            <input type="hidden" name="Start" id="Start" value="{Start}"/>
            <input type="hidden" name="Count" id="Count" value="{Count}"/>
            <input type="hidden" name="FilterBy" id="FilterBy" value="{FilterBy}"/>
            <!--xsl:if test="Total > 0">
                            <xsl:if test="Total!=9223372036854775807">Total: <xsl:value-of select="Total"/> rows; 
                            display from <xsl:value-of select="$From"/> to <xsl:value-of select="$To"/>; </xsl:if>
                        </xsl:if-->
          </h4>
        </form>
        
          <div id="bodyTable" name="bodyTable">
          <xsl:if test="string-length($openLogicalName0)">
            <table cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  Page Size: <input id="_count" size="5" type="text" value="{PageSize}" onKeyUp="onRowLimitChange(this.value);checkEnter(this, event);"/>
                </td>
                <td>
                  <input type="button" id="SearchButton" value="Search" onclick="onFindClick();" style="display:none; visibility:hidden;"></input>
                </td>
                <td align="right">
                  <xsl:if test="ColumnsHidden/ColumnHidden">
                    <A href="javascript:void(0)" onclick="toggleElement('Columns', 'viewfields');" id="explinkColumns" class="viewfieldsexpand">
                      Fields
                    </A>
                  </xsl:if>
                  <A href="javascript:void(0)" onclick="viewRow(0);" id="explinkColumns" class="viewfieldsexpand">
                    Layout
                  </A>
                </td>
             </tr>
            </table>
            <table cellpadding="0" cellspacing="0">
              <tr>
                <xsl:if test="$autoUppercaseTranslation = 1">
                  <td>
                    <xsl:choose>
                      <xsl:when test="$disableUppercaseTranslation = 1">
                        <input type="checkbox" id="DisableUppercaseTranslation" style="fontsize:small;" checked="checked" onclick="disableUppercaseTranslation=this.checked;">
                          Disable Auto Uppercase?
                        </input>
                      </xsl:when>
                      <xsl:otherwise>
                      <input type="checkbox" id="DisableUppercaseTranslation" style="fontsize:small;" onclick="disableUppercaseTranslation=this.checked;">
                        Disable Auto Uppercase?
                      </input>
                      </xsl:otherwise>
                    </xsl:choose>
                  </td>
                </xsl:if>
              </tr>
            </table>
            <table cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <input type="button" id="FirstButton" value="First" checked="checked" onclick="onRBChanged(0);onFindClick();"/>
                </td>
                <td>
                  <xsl:if test="count(Result/Dataset/*) > 0 and PageSize > 0 and (Count &gt;= PageSize)">
                    <input type="button" id="NextButton" value="Next" checked="checked" onclick="onNextPage();onFindClick();"/>
                  </xsl:if>
                </td>
                <td>
                  <xsl:if test="$rowStart &gt; 0">
                  <input type="button" id="PrevButton" value="Previous" checked="checked" onclick="onPrevPage();onFindClick();"/>
                  </xsl:if>
                </td>
                <td>
                    <input type="button" id="LastButton" value="Last" onclick="onRBChanged(1);onFindClick();"></input>
                </td>
                <td>
                  <div>
                    <span id="StartLabel">Fetch rows from record number:</span>
                    <input id="_start" size="5" type="text" onKeyUp="onRBChanged(2);checkEnter(this, event)" value="{$rowStart}"/>
                  </div>
                </td>
                <td>
                    <input type="button" name="Go" value="Fetch" onclick="onRBChanged(2);onFindClick();"/>
                </td>
             </tr>
            </table>
          </xsl:if>

            <xsl:if test="ColumnsHidden/ColumnHidden">
              <form id="control">
                <div id="Columns" style="display:none; visibility:hidden; position:absolute; cursor:move; background-color:#DDDDFF; border:1px solid #333;">
                  <script type="text/javascript">
                    new Draggable('Columns', { scroll: window });
                  </script>
                  <table>
                    <tr>
                      <td>
                        <input id="CheckALL" type="button" value="Check All" onclick="selectAll(1)"/>
                        <input id="ClearAll" type="button" value="Clear All"  onclick="selectAll(0)"/>
                      </td>
                      <td align="right">
                        <A href="javascript:void(0)" onclick="toggleElement('Columns', 'viewfields');" id="columnsclose" class="columnsclose">
                          Close
                        </A>
                      </td>
                    </tr>
                    <tr>
                      <td colspan="2">
                        <table id="viewTable" width="100%">
                          <tr>
                            <xsl:apply-templates select="ColumnsHidden/ColumnHidden" mode="createCheckboxes"/>
                          </tr>
                        </table>
                      </td>
                    </tr>
                  </table>
                </div>
              </form>
            </xsl:if>

            <xsl:if test="MsgToDisplay">
              <table>
                <tr>
                  <td id="msgToDisplay">
                    Message:<xsl:value-of select="$msgToDisplay"/>
                  </td>
                </tr>
              </table>
            </xsl:if>
            <xsl:if test="$schemaOnly = 0">
            <form id="data">
              <xsl:if test="count(Result/Dataset/*) > 0">

                <xsl:apply-templates select="Result"/>
              </xsl:if>
                  <xsl:if test="Total > 0">
                    <xsl:if test="Total!=9223372036854775807">
                      <br />
                      <span class="entryprompt">Total: </span><xsl:value-of select="Total"/> rows;
                      <xsl:if test="PageSize > 0 and count(Result/Dataset/*) > 0">
                        display from <xsl:value-of select="$From"/> to <xsl:value-of select="$To"/>;
                      </xsl:if>
                      <xsl:if test="count(Result/Dataset/*) &lt; 1">
                        Search returned 0 rows of data.
                      </xsl:if>
                    </xsl:if>
                  </xsl:if>
                  <br/>
            </form>
          </xsl:if>
        </div>
        <xsl:text disable-output-escaping="yes">
           <![CDATA[
          <iframe id="ClusterFrame" name="ClusterFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="FilesFrame" name="FilesFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="KeysFrame" name="IndexFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="FileLookupFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="DataColumnsFrame" style="display:none; visibility:hidden;"></iframe>
          ]]>
        </xsl:text>
      </body>
        </html>
    </xsl:template>

  <xsl:template match="ClusterName">
    <xsl:if test="current() = $cluster">
    <option value="{current()}" selected="selected">
      <xsl:value-of select="current()"/>
    </option>
    </xsl:if>
    <xsl:if test="current() != $cluster">
      <option value="{current()}">
        <xsl:value-of select="current()"/>
      </option>
    </xsl:if>
  </xsl:template>
  
  <xsl:template match="ColumnHidden" mode="createCheckboxes">
    <xsl:variable name="index" select="position()"/>
    <xsl:if test="$index mod 5 = 0 and $index > 0">
      <xsl:text disable-output-escaping="yes"><![CDATA[</tr><tr>]]></xsl:text>
    </xsl:if>
    <td>
      <xsl:choose>
        <xsl:when test="ColumnSize=1">
          <xsl:text disable-output-escaping="yes"><![CDATA[<input ]]></xsl:text>type="checkbox" id="ShowColumns_<xsl:value-of select="$index" />" name="ShowColumns_<xsl:value-of select="$index" />" style="cursor:pointer;" onclick="menuHandler('<xsl:value-of select="$index" />', true);" checked="checked"<xsl:text disable-output-escaping="yes"><![CDATA[ ></input>]]></xsl:text>
          <xsl:value-of select="ColumnLabel"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:text disable-output-escaping="yes"><![CDATA[<input ]]></xsl:text>type="checkbox" id="ShowColumns_<xsl:value-of select="$index" />" name="ShowColumns_<xsl:value-of select="$index" />" style="cursor:pointer;" onclick="menuHandler('<xsl:value-of select="$index" />', true);"<xsl:text disable-output-escaping="yes"><![CDATA[ ></input>]]></xsl:text>
          <xsl:value-of select="ColumnLabel"/>
        </xsl:otherwise>
      </xsl:choose>
    </td>
  </xsl:template>

  <xsl:template match="node()|@*" mode="createCheckboxes"/>  

  <xsl:template match="DFUDataColumn">
    <xsl:if test="position() = 1">
      <xsl:text disable-output-escaping="yes"><![CDATA[        
   <table border="0" cellspacing="0" cellpadding="0">
    <tr>
    ]]></xsl:text>
    </xsl:if>
    <td xmlns="http://www.w3.org/1999/xhtml" nowrap="nowrap">
      <span class="searchprompt"><xsl:value-of select="ColumnLabel"/></span>
      <br/>
      <xsl:variable name="ColumnStyle">
        <xsl:choose>
          <xsl:when test="string-length(ColumnValue) &gt; 0">
            <xsl:text>background-color:lightyellow</xsl:text>
          </xsl:when>
        </xsl:choose>
      </xsl:variable>
      <xsl:choose>
        <xsl:when test="ColumnSize &gt; 50">
          <input name="{ColumnLabel}" style="{$ColumnStyle}" size="50" maxlength="{MaxSize}" type="text" value="{ColumnValue}" onKeyUp="checkFilterField(this, event)" />
        </xsl:when>
        <xsl:when test="ColumnSize &gt; 0 and ColumnSize &lt; 50">
          <input name="{ColumnLabel}" style="{$ColumnStyle}" size="{ColumnSize}" maxlength="{MaxSize}" type="text" value="{ColumnValue}" onKeyUp="checkFilterField(this, event)" />
        </xsl:when>
        <xsl:otherwise>
          <input name="{ColumnLabel}" style="{$ColumnStyle}" size="10" type="text" value="{ColumnValue}" onKeyUp="checkFilterField(this, event)" />
        </xsl:otherwise>
      </xsl:choose>
    </td>
    <xsl:if test="position() = count(../*)">
      <xsl:text disable-output-escaping="yes">      
      <![CDATA[        
    </tr>
   </table>
    ]]></xsl:text>
    </xsl:if>
  </xsl:template>

  
</xsl:stylesheet>
