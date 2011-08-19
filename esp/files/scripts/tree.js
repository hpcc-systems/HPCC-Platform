/*##############################################################################
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
############################################################################## */


//--------------------------------------
// All browser specific code goes here

function createXmlHttpRequestObject()
{
  var xmlhttp = null;

  // code for Mozilla, etc.
  if (window.XMLHttpRequest)  {
    xmlhttp=new XMLHttpRequest();
  }
  // code for IE
  else if (window.ActiveXObject)  {
    try {
      xmlhttp=new ActiveXObject("Msxml2.XMLHTTP.4.0");
    } catch (e) {
      try {
      xmlhttp=new ActiveXObject("Msxml2.XMLHTTP");
    } catch (e) {
      xmlhttp=new ActiveXObject("Microsoft.XMLHTTP");
    }
   }
  }

  if (xmlhttp == null) 
      alert("Can not create XMLHttpRequest object in your browser!");
  
  return xmlhttp;
}

get_element = function(s_id) { return document.all ? document.all[s_id] : document.getElementById(s_id) };

function createXmlDomObject()
{
  var xmlDom = null;
  if (window.ActiveXObject) {
    xmlDom = new ActiveXObject("Microsoft.XMLDOM");
  } else if (document.implementation && document.implementation.createDocument) {
    xmlDom = document.implementation.createDocument("","",null);
  }

  if (xmlDom == null)
    alert("Can not create XML DOM object in your browser!");

  return xmlDom;
}

// emulate IE selectNodes(), selectSingleNode()
if(document.implementation && document.implementation.hasFeature("XPath", "3.0"))
{
    // NodeList

    function XmlNodeList(i) {
        this.length = i;
    }

    XmlNodeList.prototype = new Array(0);

    XmlNodeList.prototype.constructor = Array;

    XmlNodeList.prototype.item = function(i) {
        return (i < 0 || i >= this.length)?null:this[i];
    };
    
    XmlNodeList.prototype.expr = "";
    XMLDocument.prototype.setProperty  = function(x,y){};

    // IE: XMLDocument.selectNodes()
    XMLDocument.prototype.selectNodes = function(sExpr, contextNode){
        var nsDoc = this;
        var nsResolver = this.createNSResolver(this.documentElement);
        var oResult = this.evaluate(sExpr,
                    (contextNode?contextNode:this),
                    nsResolver,
                    XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
        var nodeList = new XmlNodeList(oResult.snapshotLength);
        nodeList.expr = sExpr;
        for(var i=0;i<nodeList.length;i++)
            nodeList[i] = oResult.snapshotItem(i);
        return nodeList;
    }

    // IE: Element.selectNodes()
    Element.prototype.selectNodes = function(sExpr){
        var doc = this.ownerDocument;
        if(doc.selectNodes)
            return doc.selectNodes(sExpr, this);
        else
            throw "Method selectNodes is only supported by XML Elements";
    }
    
    // IE: XMLDocument.selectSingleNodes()
    XMLDocument.prototype.selectSingleNode = function(sExpr, contextNode){
        var ctx = contextNode?contextNode:null;
        sExpr = "("+sExpr+")[1]";
        var nodeList = this.selectNodes(sExpr, ctx);
        if(nodeList.length > 0)
            return nodeList.item(0);
        else
            return null;
    }

    // IE: Element.selectSingleNode()
    Element.prototype.selectSingleNode = function(sExpr){
        var doc = this.ownerDocument;
        if(doc.selectSingleNode)
            return doc.selectSingleNode(sExpr, this);
        else
            throw "Method selectNodes is only supported by XML Elements";
    }
}

function serializeDomNode(xmlDoc)
{
    if (xmlDoc.xml) {
        return xmlDoc.xml;
    } else if(window.XMLSerializer) {
        return (new XMLSerializer()).serializeToString(xmlDoc);
    } 
    
    return null;
}   

// END: browser specific code
//--------------------------------------

var ie = document.all ? 1 : 0;
var ns = document.layers ? 1 : 0;

/*
- multiple selection
- ability to dynamically add items
- context menu with both item and tree specific commands
- ability to add on_sel_changed, on_insert_child and on_expanding handlers
- find xpaths for items and find items with given xpaths
- demand loading of children
- custom menus for individual items
*/
function tree (a_items, a_template) {
    this.n_tid = trees.length;
    trees[this.n_tid] = this;
    
    this.a_tpl      = a_template;
    this.a_config   = a_items;
    this.o_root     = this;
    this.a_index    = [];
    this.a_selected = [];
    this.a_deleted  = [];
    this.b_selected = false;
    this.n_depth    = -1;
    this.select_all = select_all;
    this.select_none= select_none;
    this.upstatus   = function() {};//nothing
    this.o_parent   = null;
    this.a_custom_menus = [];
    this.a_custom_menu_ids = [];
    this.n_items    = 0;
    this.b_cache_children = true;
    this.get_item   = function(id) { return this.a_index[id]; };
    this.item_count = function() { return this.n_items; };
    this.get_first_item = get_first_item;
    this.get_next_item = get_next_item;
    this.deleteAllChildren = deleteAllChildren;
    this.action = null;
    this.border = null;
    this.a_columns= null;
    this.a_columnWidths = null;
    this.add_tree_column = add_tree_column;
    this.timeoutId = 0; 

    var o_icone = new Image();
    var o_iconl = new Image();
    o_icone.src = a_template['icon_e'];
    o_iconl.src = a_template['icon_l'];
    a_template['im_e'] = o_icone;
    a_template['im_l'] = o_iconl;

    for (var i = 0; i < 64; i++)
        if (a_template['icon_' + i]) 
        {
            var o_icon = new Image();
            a_template['im_' + i] = o_icon;
            o_icon.src = a_template['icon_' + i];
        }
    
    this.expand = item_expand;
    this.toggle = toggle;
    this.select = function (n_id, e, treeid) { return this.get_item(n_id).select(e, treeid); };
    this.mover  = mouse_over_item;
    this.mout   = mouse_left_item;
    this.oncontextmenu = function (n_id, e, treeid) { return this.get_item(n_id).oncontextmenu(e, treeid) };
    this.show_context_menu = show_context_menu;
    this.handle_command = handle_command;
    this.on_sel_changed = null;
    this.on_expanding   = null;
    this.on_context_menu= null;
    this.on_command     = null;
    
    this.get_custom_menu = function(menu_name) {
        var index = this.a_custom_menu_ids[menu_name];
        return (index != undefined) ? this.a_custom_menus[index] : null;
    }
    this.add_custom_menu = function(menu_name, a_menu) { 
        if (!this.get_custom_menu(menu_name))
        {
            var index = this.a_custom_menus.length;
            this.a_custom_menus[index] = a_menu;
            this.a_custom_menu_ids[menu_name] = index;
        }
        return index;
    }
    
    this.a_children = [];
    for (var i = 0; i < a_items.length; i++)
        new tree_item(this, i, this.a_config[i + (this.n_depth + 1 ? 2 : 0)]);

    var n_children = this.a_children.length;
    if (n_children) {
        var div = document.getElementById('tree_' + this.n_tid);
        var a_html = [];
        for (var i=0; i<n_children; i++) {
            var child = this.a_children[i];
            a_html[i] = child.init();
            child.expand();
        }
        div.innerHTML = a_html.join('');
    }
}

function loadXMLDoc(url)
{   
    var xmlhttp = createXmlHttpRequestObject();
    if (xmlhttp)
    {
        xmlhttp.open("GET", url, false);
        xmlhttp.send(null);
    }
    return xmlhttp;
}

function tree_item (o_parent, n_position, config) {

    this.n_depth  = o_parent.n_depth + 1;
    this.a_config = config;
    if (!this.a_config) return;

    this.o_root = o_parent.o_root;
    this.o_parent  = o_parent;
    this.n_position   = n_position;
    this.b_expanded  = !this.n_depth;
    this.b_selected= false;
    this.b_load_on_demand = false;
    this.b_cache_children = this.o_root.b_cache_children;
    this.action = null;
    this.a_columns = null;
    this.add_column = add_column;
    this.b_checkbox = false;
    this.b_checked = false;

    if (this.o_root.a_deleted.length > 0)//reclaim a previously deleted id
        this.n_id = this.o_root.a_deleted.shift();
    else
        this.n_id = this.o_root.item_count(); //a_children is contiguous so pick next index
    this.o_root.a_index[this.n_id] = this;
    this.o_root.n_items++;
    o_parent.a_children[n_position] = this;

    this.a_children = [];
    for (var i = 0; i < this.a_config.length - 2; i++)
        new tree_item(this, i, this.a_config[i + (this.n_depth + 1 ? 2 : 0)]);

    this.get_icon = item_get_icon;
    this.expand  = item_expand;
    this.select   = handle_selection;
    this.init    = item_init;
    this.upstatus = item_upstatus;
    this.oncontextmenu=context_menu;
    this.name    = function () { return this.a_config[0]; }
    this.data    = null;
    this.on_insert_child= null;
    this.is_last  = 
        function () {
            return this.n_position == this.o_parent.a_children.length - 1 
        };
    if (o_parent.on_insert_child)
        o_parent.on_insert_child(this);
}

function item_init () {
    var o_tree = this.o_root;
    var treeId = o_tree.n_tid;
    var itemId = this.n_id;
    var tree = 'trees[' + treeId + ']';

    var s = [];
    var i = 0;  
    s[i++] = '<table cellpadding="0" cellspacing="0"'
    if (o_tree.border)
        s[i++] = ' style="border-bottom:1px groove lightgray"';
    s[i++] = '>';
    if (o_tree.a_columns)
        s[i++] = get_column_header_html(o_tree, false);
    s[i++] = '<tr><td nowrap valign="bottom">';
    
    if (this.n_depth)
    {
        var o_current_item = this.o_parent;
        for (var j = this.n_depth; j > 1; j--) {
            s[j+i] = '<img src="' + this.o_root.a_tpl[o_current_item.is_last() ? 'icon_e' : 'icon_l'] + '" border="0" align="absbottom">';
            o_current_item = o_current_item.o_parent;
        }
        i = s.length;

        if (this.b_load_on_demand || this.a_children.length)
        {
            s[i++] = '<a class="tree" href="javascript:';
            s[i++] = tree;
            s[i++] = '.toggle(';
            s[i++] = itemId;
//          s[i++] = ')" onmouseover="';
//          s[i++] = tree;
//          s[i++] = '.mover(';
//          s[i++] = itemId;
//          s[i++] = ')" onmouseout="';
//          s[i++] = tree;
//          s[i++] = '.mout(';
//          s[i++] = itemId;
            s[i++] = ')"><img src="';
            s[i++] = this.get_icon(true);
            s[i++] = '" border="0" align="absbottom" name="j_img';
            s[i++] = treeId;
            s[i++] = '_';
            s[i++] = itemId;
            s[i++] = '"></a>';
        }
        else
        {
            s[i++] = '<img src="';
            s[i++] = this.get_icon(true);
            s[i++] = '" border="0" align="absbottom">';
        }
    }

    if (this.b_checkbox)
    {
        s[i++] = '<input type="checkbox" style="zoom:0.8"';
        if (this.b_checked)
            s[i++] = ' checked="true"'
        s[i++] = '>';
    }

    s[i++] = '<a href="';
    s[i++] = this.a_config[1] == null ? 'javascript:void(0)' : this.a_config[1];
    s[i++] = '" target="';
    s[i++] = this.o_root.a_tpl['target']=='_self' ? 'javacript:void(0)' : this.o_root.a_tpl['target'];
    s[i++] = '" onclick="return ';
    s[i++] = tree;
    s[i++] = '.select(';
    s[i++] = itemId;
    s[i++] = ', event, ';
    s[i++] = '\'i_txt';
    s[i++] = treeId;
    s[i++] = '_';
    s[i++] = itemId;
    s[i++] = '\')" ondblclick="';
    s[i++] = tree;
    s[i++] = '.toggle(';
    s[i++] = itemId;
    //  s[i++] = ')" onmouseover="alert(\'here\');" ';
    //  s[i++] = tree;
    //  s[i++] = '.mover(';
    //  s[i++] = itemId;
    //  s[i++] = ')" onmouseout="';
    //  s[i++] = tree;
    //  s[i++] = '.mout(';
    //  s[i++] = itemId;
    s[i++] = ')" oncontextmenu="return ';
    s[i++] = tree;
    s[i++] = '.oncontextmenu(';
    s[i++] = itemId;
    s[i++] = ', event, ';
    s[i++] = '\'i_txt';
    s[i++] = treeId;
    s[i++] = '_';
    s[i++] = itemId;    
    s[i++] = '\')" class="t';
    s[i++] = treeId;
    s[i++] = 'i" id="i_txt';
    s[i++] = treeId;
    s[i++] = '_';
    s[i++] = itemId;
    s[i++] = '"';
    if (this.n_depth == 0)
        s[i++] = ' style="font-weight:bold"';   
    s[i++] = '>';
    s[i++] = '<img src="';
    s[i++] = this.get_icon();
    s[i++] = '" border="0" align="absbottom" name="i_img';
    s[i++] = treeId;
    s[i++] = '_';
    s[i++] = itemId;
    s[i++] = '" class="t';
    s[i++] = treeId;
    s[i++] = 'im"/>';
    s[i++] = this.name();
    s[i++] = '</a></td>';
    if (o_tree.a_columns)
    {
        //the a_columns array of item only holds non-empty
        //column contents for columns after first column and
        //may not have any columns even though the tree may 
        //have more columns defined
        var n_columns = this.a_columns ? this.a_columns.length : 0;
        for (var c=0; c<n_columns; c++)
        {
            s[i++] = '<td class="small">';
            s[i++] = this.a_columns[c];
            s[i++] = '</td>';
        }
        //now create table cells for empty columns
        n_columns = o_tree.a_columns.length;
        c++;
        for (; c<n_columns; c++)
            s[i++] = '<td/>';
    }
    s[i++] = '</tr></table>';
   
    s[i++] = '<div id="i_div';
    s[i++] = treeId;
    s[i++] = '_';
    s[i++] = itemId;
    s[i++] = '" style="display:';
    s[i++] = this.b_expanded ? 'block' : 'none';
    s[i++] = '">';

    if (this.b_expanded)
        for (var j = 0; j < this.a_children.length; j++)
            s[i++] = this.a_children[j].init();

    s[i++] = '</div>';
    return s.join('');
}

//add column for a tree
//
function add_tree_column(column)
{
    if (this.a_columns == null)
    {
        this.a_columns = new Array();
        this.a_columnWidths = new Array();
    }
    this.a_columns.push( column );
    var width = column['width'];
    this.a_columnWidths[ this.a_columnWidths.length ] = typeof width != 'undefined' ? width : 300;
}

//add column for a tree item
//
function add_column(innerHTML)
{
    if (this.a_columns == null)
        this.a_columns = new Array();
    this.a_columns.push( innerHTML );
}

function load_children_on_demand(item)
{
    var rc = false;
    if (item.b_load_on_demand && item.a_children.length==0)
    {
        window.status = 'Loading child items ...';
   
        var url = '/esp/navdata';
        if (item.params)
            url += '?' + item.params;
        
        var xmlHttp = loadXMLDoc(url);       
        if (xmlHttp.status==200)
        {
            addChildItemsFromNavData(xmlHttp.responseXML.documentElement, item);
            rc = true;
        }
        else
            alert("Error in dynamically loading the children:\n" + xmlHttp.statusText);
    }
    
    window.status = '';
    return rc;//don't expand if error occurred
}

function item_expand (b_expand, b_recursive) {

    if (this.a_children.length == 0)
        return;

    var treeId = this.o_root.n_tid;
    var o_idiv = get_element('i_div' + treeId + '_' + this.n_id);
    if (o_idiv)
    {
        if (!o_idiv.innerHTML) {
            var a_children = [];
            for (var i = 0; i < this.a_children.length; i++)
                a_children[i] = this.a_children[i].init();
            o_idiv.innerHTML = a_children.join('');
        }
        o_idiv.style.display = b_expand ? 'block' : 'none';

        this.b_expanded = b_expand;
        var o_jicon = document.images['j_img' + treeId + '_' + this.n_id],
            o_iicon = document.images['i_img' + treeId + '_' + this.n_id];
            
        if (o_jicon) 
            o_jicon.src = this.get_icon(true);
            
        if (o_iicon && this.get_icon) 
            o_iicon.src = this.get_icon();

        if (this.upstatus)
            this.upstatus();
    }

    if (b_recursive)
        for (var i = 0; i < this.a_children.length; i++)
            this.a_children[i].expand(b_expand, b_recursive);
}

function get_column_header_html(table, bCaption)
{
    var a_columns = table.a_columns;
    var n_columns = a_columns ? a_columns.length : 0;
    if (!n_columns)
        return '';

    var a_html = [];
    a_html.push(bCaption ? '<table><thead class="sort-table"><tr>' : '<colgroup>');
    for (var i=0; i<n_columns; i++)
    {
        var column = a_columns[i];
        a_html.push(bCaption ? '<th class="sort-table"' : '<col');
        for (attrib in column)
            if (attrib != 'innerHTML')
                a_html.push(' ' + attrib + '="' + column[attrib] + '"');
        a_html.push('>');
        if (bCaption)
        {
            var innerHTML = column['innerHTML'];
            if (typeof innerHTML != 'undefined')
                a_html.push(innerHTML);
        }
        a_html.push(bCaption ? '</th>' : '</col>');
    }
    a_html.push(bCaption ? '</tr></thead></table>' : '</colgroup>');
    return a_html.join('');
}

function redo_item(item)
{
    var o_tree = item.o_root;
    if (item == o_tree)
    {
        var a_html = [];
        var n_children = item.a_children.length;
        for (var i = 0; i < n_children; i++)
        {
            var child = item.a_children[i];
            a_html[i] = child.init();
            child.expand();
        }
        var div = get_element('tree_'+item.n_tid);
        if (o_tree.a_columns)
        {
            a_html.unshift(get_column_header_html(item, true));

            var width = 0;
            var n_columns = o_tree.a_columns.length
            for (i=0; i<n_columns; i++)
                width += Number(o_tree.a_columnWidths[i]);
            div.style.width = width;
        }
        div.innerHTML = a_html.join('');
    }
    else
    {
        var o_idiv = get_element('i_div' + o_tree.n_tid + '_' + item.n_id);
        if (o_idiv)
        {
            var a_html = [];
            for (var i = 0; i < item.a_children.length; i++)
                a_html[i] = item.a_children[i].init();

            o_idiv.innerHTML = a_html.join('');
            return o_idiv.innerHTML;
        }
    }
}

function insert_item(parent, item_name, b_load_on_demand)
{
    if (!parent.a_children)
        parent.a_children = new Array();

    var pos = parent.a_children.length;
    var new_item = new tree_item(parent, pos, [item_name, null]);
    new_item.b_load_on_demand = b_load_on_demand;
    //window.status = '';

    redo_item(parent.o_parent != parent.o_root ? parent.o_parent : parent);

    if (!parent.b_expanded)
        parent.expand(true);

    new_item.select(false);
    return new_item;
}

function add_item(tree, parent_id, item_type, name, tooltip, menu, params)
{
    var parent = parent_id == -1 ? tree.o_root : tree.a_index[parent_id];
    var pos = parent.a_children.length;
    var new_item = new tree_item(parent, pos, [name, null]);
    
    if (tooltip != '')
        new_item.tooltip = tooltip;
        
    if (item_type == 'DynamicFolder')
        new_item.b_load_on_demand = true;
        
    if (menu != '')
    {
        var menu_id = tree.a_custom_menu_ids[menu];
        if (menu_id != -1)
            new_item.n_custom_menu = menu_id;
    }
        
    if (params != '')
        new_item.params = params; //unescape(params);
    
    return new_item;
}

function delete_item(tree, item_id, b_refresh)
{
    if (typeof b_refresh == 'undefined')
        b_refresh = true;

    //remove children of this item first, if any
    //
    var item = tree.get_item(item_id);
    var children = item.a_children;
    var n_children = children.length;
    for (var i=0; i<n_children; i++)
        delete_item(tree, children[i].n_id, false);

    if (item.b_selected)
    {
        selected = tree.a_selected;
        n_selected = selected.length;
        for (i=0; i<n_selected; i++)
            if (selected[i] == item)
            {
                selected.splice(i, 1);
                break;
            }
    }

    tree.a_deleted[ tree.a_deleted.length ] = item_id;
    tree.a_index[ item_id ] = null;
    tree.n_items--;

    //update this item's parent's a_children - only for top most item getting deleted
    //
    var o_parent = item.o_parent;
    if (b_refresh)
    {
        //remove this item from its parent's a_children array
        //
        children = o_parent.a_children;
        n_children = children.length;
        for (i=0; i<n_children; i++)
            if (children[i] == item)
            {
                children.splice(i, 1);
                break;
            }

        redo_item(o_parent.o_parent != o_parent.o_root ? o_parent.o_parent : o_parent);
    }
} 

function deleteAllChildren(item)
{
    var children = item.a_children;
    var n_children = children.length;
    for (var i=0; i<n_children; i++)
        delete_item(this, children[i].n_id, false);
    children.length=0;
}

function refresh_item(tree, item_id)
{
    var item = tree.get_item(item_id);
    if (!item.b_load_on_demand)
        return;

    var b_expanded = item.b_expanded;
    if (b_expanded)
        item.expand(false);

    tree.deleteAllChildren(item);

    if (b_expanded && load_children_on_demand(item))
        item.expand(true);
    return true;
}

function import_xml_nodeset(parent, objNodes)
{
    var len = objNodes.length;
    for (var i=0; i<len; i++)
    {
        var pos = parent.a_children.length;
        var item_name = objNodes[i].text;
        new tree_item(parent, pos, [item_name, null]);
    }

    redo_item(parent.o_parent != parent.o_root ? parent.o_parent : parent);
}

function get_xml_attrib(xmlNode, attribName, defaultValue)
{
   var attr = xmlNode.attributes.getNamedItem(attribName);
   return attr ? attr.nodeValue : defaultValue;
}


function import_xml(parent, objNode)
{
    var a_nodes = objNode.childNodes;
    var n_nodes = a_nodes.length;

    for (var i=0; i<n_nodes; i++)
    {       
        var name = get_xml_attrib(a_nodes[i], 'name', 'unknown')
        var pos = parent.a_children.length;
        var item = new tree_item(parent, pos, [name, null]);

        if (a_nodes[i].tagName == 'Folder')
            import_xml(item, a_nodes[i]);
    }

    redo_item(parent.o_parent != parent.o_root ? parent.o_parent : parent);
}

function get_first_item()
{
    return get_next_item(-1);
}

function get_next_item(item_id)
{
    var n_items = this.item_count();
    if (n_items > 0)
    {
        var size = this.a_children.length;
        for (var i = item_id+1; i<size; )
            if (this.a_children[i])
                return i;
    }
    return -1;
}

function select_all()
{
    this.o_root.expand(true, true);

    this.o_root.a_selected.length = 0;
    
    var i=0;
    for ( var item_id = this.o_root.get_first_item(); 
            item_id != -1; 
            item_id = this.o_root.get_next_item(item_id))
    {
        this.o_root.a_selected[i] = this.o_root.get_item(item_id);
        select_item(this.o_root.a_selected[i++], true);
    }
    this.o_root.upstatus();
}

function select_none()
{
    var n_selected = this.a_selected.length;
    for (var i=0; i<n_selected; i++)
        select_item(this.a_selected[i], false);

    this.a_selected.length = 0;

    this.o_root.upstatus();
}

function handle_selection(e)
{
    var n_selected = this.o_root.a_selected.length;
    var b_multiselect;
    var b_select;
    var o_tree = this.o_root;

    //if either the control key is pressed or an already selected item is clicked 
    //upon with right mouse button then this is the case of multiple selection 
    //
    if (!e) {
        e = window.event;
    }
    if ((e && e.ctrlKey) || (this.b_selected && o_tree.b_rightMouseButton))
    {
        b_multiselect = true;

        //select this item unless the control key is pressed without right mouse button 
        //and object is already selected
        b_select = !(e.ctrlKey && this.b_selected);
    }
    else
    {
        b_multiselect = false;
        b_select = true;

        //if either multiple items are currently selected or some other item is then unselect all
        if (n_selected > 1 || (n_selected==1 && !this.b_selected))
            o_tree.select_none();
    }
    
    if (b_select != this.b_selected)
    {
        if (b_select)
            o_tree.a_selected.push(this);
        else
        {
            //delete this object from the root's a_selected array
            var n_selected = o_tree.a_selected.length;
            for (var i=0; i<n_selected; i++)
                if (o_tree.a_selected[i] == this)
                {
                    o_tree.a_selected.splice(i, 1);
                    break;
                }
        }
        select_item(this, b_select);

        if (o_tree.on_sel_changed)
          o_tree.on_sel_changed(o_tree);

        this.upstatus();
    }

    if ((o_tree.action || this.action) && !o_tree.b_rightMouseButton && !b_multiselect)
        o_tree.handle_command(o_tree.n_tid, this, 'click', this.action ? this.action : o_tree.action);

    return Boolean(this.a_config[1]);
}

function select_item (item, b_select) {

    item.b_selected = b_select;
    var o_iicon = document.images['i_img' + item.o_root.n_tid + '_' + item.n_id];
    if (o_iicon) 
        o_iicon.src = item.get_icon();

    var obj = get_element('i_txt' + item.o_root.n_tid + '_' + item.n_id);
    obj.style.backgroundColor = b_select ? 'highlight' : 'window';
    obj.style.color = b_select ? 'white' : 'black';
}

function item_upstatus (b_clear) {
    //window.setTimeout('window.status="' + (b_clear ? '' : this.name() + 
    //  (this.a_config[1] ? ' ('+ this.a_config[1] + ')' : '')) + '"', 10);
}

function item_get_icon (b_junction) {
    var b_has_children = this.b_load_on_demand || this.a_children.length;
    var icon_id =  this.n_depth ? 0 : 32;

    if (b_has_children)
    {
        icon_id += 16;

        if (this.b_expanded)
            icon_id += 8;
    }

    if (b_junction)
    {
        icon_id += 2;
        
        if (this.is_last())
            icon_id++;
    }
    //else
    //  if (this.b_selected)
    //      icon_id += 4;
    
    return this.o_root.a_tpl['icon_' + icon_id];
}
/* not used yet...
function get_xpath(item)
{
    var xpath = null;
    while (item != item.o_root)
    {
        xpath = xpath ? item.name() + '/' + xpath : item.name();
        item = item.o_parent;
    }
    return xpath ? xpath : "";
}
*/

function get_items_with_name(parent, name)
{
    var a_children = [];

    var nChildren = parent.a_children.length;
    for (var i = 0; i < nChildren; i++)
        if (name == '*' || parent.a_children[i].name() == name)
            a_children.push(parent.a_children[i]);

    return a_children;
}
/* unused for now...
function get_items_at_xpath(parent, xpath)
{
    var items = [];
    var ndx = xpath.indexOf('/');
    var prefix;
    var suffix;

    if (ndx == -1)// no slash found in xpath
    {
        prefix = xpath;
        suffix = '';
    }
    else
    {   
        prefix = xpath.substring(0, ndx);
        suffix = xpath.substring(ndx+1);
    }

    if (prefix.length)
    {
        var found = get_items_with_name(parent, prefix);

        if (suffix == '')
            return found;
        else
            for (var i=0; i<found.length; i++)
            {
                var found2 = get_items_at_xpath(found[i], suffix);
                for (var j=0; j<found2.length; j++)
                    items.push( found2[j] );
            }
    }
   return items;
}
*/

function handle_command(tree_id, focus_item_id, cmd, action) {
    //hide_popup_menu();
    var tree = this.o_root;

    if (cmd == 'Select All')
    {
        tree.select_all();
        if (tree.on_sel_changed)
            tree.on_sel_changed(tree);          
    }
    else if (cmd == 'Select None')
    {
        tree.select_none();
        if (tree.on_sel_changed)
            tree.on_sel_changed(tree);          
    }
    else if (cmd == 'Expand All')
    {
        tree.expand(true, true);
    }
    else if (cmd == 'Collapse All')
    {
        tree.expand(false, true);
    }
    else if (cmd == 'Expand')
    {
        for (var i=0; i<tree.a_selected.length; i++)
            tree.a_selected[i].expand(true, true);
    }
    else if (cmd == 'Collapse')
    {
        for (var i=0; i<tree.a_selected.length; i++)
            tree.a_selected[i].expand(false, true);
    }
    else if (cmd == 'New')
    {
        var item_name = prompt('Enter name of new item:', 'New Item');
        if (item_name)
        {
            var item = tree.get_item(focus_item_id);
            insert_item(item, item_name, true);
        }
    }
    else if (cmd == 'Delete')
    {
        delete_item(tree, focus_item_id);
    }
    else if (cmd == 'Refresh')
    {
        refresh_item(tree, focus_item_id);
    }
    else if (tree.on_command)
    {
        tree.on_command(tree, cmd, action);
    }

    return true;
}

function search_array(a_array, item)
{
    var len = a_array.length;
    for (var i=0; i<len; i++)
        if (a_array[i] == item)
            return i;
    return -1;
}

function context_menu(e, treeid)
{
    //document.all.menu.innerHTML = '';
    //document.all.src.value = '';
    //document.all.src.value = document.body.innerHTML;
    //debugger;
    //hack: the web page does not remember that right mouse button
    //is pressed to remember it so item selection can use that fact
    this.o_root.b_rightMouseButton = true;
    this.select(e);
    
    if (!this.b_selected)
        this.select(e);
    this.o_root.b_rightMouseButton = false;

    /* define a menu as an array of menu items.  Each menu item is as follows:
    null - separator
    command [, action [, caption [, handler [, showCheckbox]]] //only command is required
    where:
    command: the command that is passed to the handler
    action : the parameter string needed to run the command like a url
    caption: the menu item text.  If this is not supplied then command is shown as text
    handler: of the form function handler(item_id, command, action) {...} or null to disable
    showCheckbox: if null, no checkbox is shown, true/false indicate check states otherwise

    for instance:
    var menu=[["cmd_1", "action_1", "Caption 1", "menuHandler", true], //true for checked item
                 null, //separator
                  ["cmd_2", "action_2", "Caption 2", null, false]];//false for unchecked item
    */
  //do a dedup of all custom menus of selected items
    var a_custom_menu_ids = [];
    var n_custom_menu_id = -1;
    var b_all_dynamic_folders = true;
    for (var i=0; i<this.o_root.a_selected.length; i++)
    {
        var sel_item = this.o_root.a_selected[i]; 
        if (!sel_item.b_load_on_demand)
            b_all_dynamic_folders = false;

        var menu_id = sel_item.n_custom_menu;
        if (menu_id != undefined)
        {
            if (a_custom_menu_ids.length == 0)//none set yet
                n_custom_menu_id = menu_id;
            else
                if (n_custom_menu_id != -1)//multiple selected items already had different menus
                {
                    if (n_custom_menu_id != menu_id)
                        n_custom_menu_id = -1;
                }
                    
            if (search_array(a_custom_menu_ids, menu_id) == -1)
                a_custom_menu_ids.push(menu_id);
        }
        else//some item does not have custom menu so disallow all custom menus
            break;
    }
    //define default menu...
    var default_menu = [
        /*
        ["New"],
        ["Delete"],
        ["Refresh"],
        null,
        ["Expand"],
        ["Collapse"],
        null,
        ["Select All"],
        ["Select None"],
        ["Expand All"],
        ["Collapse All"]
        */
    ];
        
   var menu;
    if (n_custom_menu_id != -1)//all selected items had same menu
    {
       //menu = this.o_root.a_custom_menus[n_custom_menu_id].concat([null], default_menu);
       menu = this.o_root.a_custom_menus[n_custom_menu_id].concat(default_menu);
    }
    else
    {
        if (a_custom_menu_ids.length == 0)//none of the selected items had any custom menus 
            menu = default_menu;
        else
        {
            //the selected items had different custom menus 
            //so make a menu with items that are intersection of these menus        
            var n_first_id = a_custom_menu_ids[0];
            var a_first_menu = this.o_root.a_custom_menus[n_first_id];//start with first menu
            var new_custom_menu = [];

            for (var i=0; i<a_first_menu.length; i++)//enumerate first menu's items
            {
                var menu_item_cmd = a_first_menu[i][0];
                var b_add = true;
                
                for (var j=1; j<a_custom_menu_ids.length; j++)
                {                   
                    var a_menu = this.o_root.a_custom_menus[ a_custom_menu_ids[j] ];
                    var b_found = false;
                    for (var k=0; k<a_menu.length; k++)
                        if (a_menu[k][0] == menu_item_cmd)
                        {
                            b_found = true;
                            break;
                        }
                        
                    if (!b_found)
                    {
                        b_add = false;
                        break;
                    }
                }
                
                if (b_add)
                    new_custom_menu.push( a_first_menu[i] );
            }
           menu = new_custom_menu;
        }       
    }
        
    if (b_all_dynamic_folders)
    {
        var hasRefreshCmd = false;
        for (var i=0; i<menu.length; i++)//enumerate first menu's items
            if (menu[i][0] == 'Refresh')
            {
                hasRefreshCmd = true;
                break;
            }
        if (!hasRefreshCmd)
        {
            if (menu.length)
                menu.push(null);
            menu.push(['Refresh']);
        }
    }

    if (this.o_root.on_context_menu)
        this.o_root.on_context_menu(this.o_root, menu); //allow callback to override the default menu

    if (menu.length)
    {
        this.o_root.show_context_menu(this.n_id, menu, null, false, 1, treeid);
        return false; //don't let even propagate up to allow browser's menu
    }
    return true;//let event propagate to show browser's menu
}

var oMenu;
var menuItems;

function show_context_menu(item_id, menu, popup_caption, addCloseButton, itemsPerLine, treeid)//"View Columns:"
{
    
    var s = new Array();
    var i = 0;

    s[i++] = '<table style="font:bold 8pt verdana, arial, helvetica, sans-serif" id="tab" width="10" oncontextmenu="return false" onselectstart="return false">';
    //s[i++] = 'onmouseover="parent.clear_popup_timer()" onmouseout="parent.set_popup_timer()">';
    var iCaption = i; //leave an item as place holder for possible caption to be added later below
    s[i++] = ' '; //caption
    s[i++] = '<tr>';
    var horizLine = false;
    var columns = 0;

    for(var item in menu)
    if(menu[item])
    {
        var menuItem= menu[item];
        var cmd  = menuItem[0];
        var action  = menuItem[1];
        var caption = menuItem[2] ? menuItem[2] : menuItem[0];
        var handler = menuItem[3] ? menuItem[3] : 'trees['+this.o_root.n_tid+'].handle_command';
        var checked = menuItem[4];
        var disabled=!menuItem[1];

        if ((columns++ % itemsPerLine == 0) && columns>0)
            s[i++] = '</tr><tr>';

        s[i++] = '<td nowrap style="paddiyesng:0';
        if (horizLine)
        {
            s[i++] = ';border-top:solid 1px gray';
            horizLine = false;
        }
        s[i++] = '" onmouseover=\'this.runtimeStyle.cssText="background-color:highlight; color:';
        s[i++] = disabled ? 'graytext' : 'highlighttext';
        s[i++] = ';";\' onmouseout=\'this.runtimeStyle.cssText=""\'';

        if (checked != null)
        {
            s[i++] = '>'; //close <td> tag
            s[i++] = '  <input type="checkbox" ';
            if (checked)
                s[i++] = 'checked="true" ';
            if (!disabled)
            {
                s[i++] = 'onclick="return parent.';
                s[i++] = handler;
                s[i++] = '(';
                s[i++] = this.n_tid;
                s[i++] = ',';
                s[i++] = item_id;
                s[i++] = ',\'';
                s[i++] = cmd;
                s[i++] = '\', ';
                s[i++] = action;
                s[i++] = '\')"';
            }
            s[i++] = ' style="background-color:transparent;cursor:default" valign="center" >';
            s[i++] = caption;
            s[i++] = '</input>';
        }
        else
        {
            s[i++] = ' onclick="return parent.' + handler + '(' + this.n_tid + ',' + item_id + ',\'' + cmd + '\', \'' + action + '\')">';
            s[i++] = caption;
        }
        s[i++] = '</td>';
    }
    else
        horizLine = true;
    
    var numColumns = columns > itemsPerLine ? itemsPerLine : columns;
    if (popup_caption)
        s[iCaption] = '<tr><th align="center" colspan="' + numColumns + '">' + popup_caption + '<hr/></th></tr>';
    if (addCloseButton)
        s[i++] = '<tr><th align="center" colspan="' + numColumns + '"><hr/>' + 
    '<input type="button" value="Close" onclick="parent.contextMenu.hide()"/>' +
    '</th></tr>'
    s[i++] ='</tr></table>';

    //alert(s);
    var xypos = YAHOO.util.Dom.getXY(treeid);
    xypos[0] = 0;
    if (oMenu) {
        oMenu.destroy();
    }
    oMenu = new YAHOO.widget.Menu("treemenu", { position: "dynamic", xy: xypos });
    oMenu.clearContent();

    menuItems = null;
    menuItems = new Array();
    for (var item in menu) {
        if (menu[item]) {
            var menuItem = menu[item];
            var ocmd = menuItem[0];
            var oaction = menuItem[1];
            var ocaption = menuItem[2] ? menuItem[2] : menuItem[0];
            var ohandler = menuItem[3] ? menuItem[3] : 'trees[' + this.o_root.n_tid + '].handle_command';
            menuItems[menuItems.length] = { text: ocaption.toString(), onclick: { fn: function() { context_Menu_Select(this.index); } }, treeid: this.n_tid, itemid: item_id, command: ocmd, action: oaction };
        }
    }



    function context_Menu_Select(Menu_Index) {
        trees[menuItems[Menu_Index].treeid].handle_command(menuItems[Menu_Index].treeid, menuItems[Menu_Index].itemid, menuItems[Menu_Index].command, menuItems[Menu_Index].action);
    }
    
    oMenu.addItems(menuItems);
    oMenu.render("menu");
    oMenu.show();
    return;        

}

function test ()
{
    alert('test');
}
/* this function takes a string of the form "prefix{javascript}suffix" and 
   returns prefixXYZsuffix, where XYZ is the result of execution of javascript 
   within braces after any 'this' occurrences have been substituted with 'item'.
   Multiple {javascript} blocks may be embedded within the string.
  */
/* unused for now...
function process_embedded_scripts(url, item)
{
    if (url.length == 0)
        return '';
        
    var open_brace = url.indexOf('{');
    if (open_brace == -1)
        return url;
    
    var close_brace = url.indexOf('}', open_brace+1);
    if (close_brace == -1)
        close_brace = url.length+1;
        
    var raw_script = url.substring(open_brace+1, close_brace);
    var output;
    if (raw_script.length)
    {
        var script = raw_script.replace('this.', 'item.');
        var Fn = new Function('item', 'return ' + script);
        output = Fn(item);
    }
    return url.substring(0, open_brace) + output + process_embedded_scripts( url.substring(close_brace+1), item );
}
*/

function addToXmlDoc(xmlDoc, item)
{
    if (item.o_parent == item.o_root)
        parentNode = xmlDoc.documentElement;
    else
        parentNode = addToXmlDoc(xmlDoc, item.o_parent);
    
    var name = item.name();
    var node = parentNode.selectSingleNode( "*[@name='"+name+"']" );
    if (!node)
    {
        var nodeName = item.b_load_on_demand ? 'DynamicFolder' : (item.a_children.length ? 'Folder' : 'Link');
        node = xmlDoc.createElement( nodeName );
        parentNode.appendChild(node);
        
        var attr = xmlDoc.createAttribute("name");
        attr.value = name;
        node.setAttributeNode(attr);
        
        if (item.params)
        {
            attr = xmlDoc.createAttribute("params");
            //attr.value = escape(item.params);
            attr.value = item.params;
            node.setAttributeNode(attr);
        }
    }   
    return node;
}

function selectionToXmlDoc(tree)
{
    var xmlDoc = createXmlDomObject();
    var docElement = xmlDoc.createElement('EspNavigationData');
    xmlDoc.appendChild(docElement);
    xmlDoc.async="false";
    
    var nSelected = tree.o_root.a_selected.length;
    for (var i=0; i<nSelected; i++)
    {
        var item = tree.o_root.a_selected[i];
        var node = addToXmlDoc(xmlDoc, item);
        var attr = xmlDoc.createAttribute("selected");
        attr.value = 'true';
        node.setAttributeNode(attr);
    }    
    return xmlDoc;
} 

function postForm(xmlDoc, url) 
{ 
    var xmlhttp = createXmlHttpRequestObject();
    xmlhttp.open("POST", url, false); 
    xmlhttp.SetRequestHeader("Content-Type", "text/xml");
    xmlhttp.send(xmlDoc);
    
    //Confirm HTTP request succeeded.
    var resp = '';
    if ((xmlhttp.status == 200) || (xmlhttp.status == 300 /*multiple choices*/))
        resp = xmlhttp.responseText;
    else
        alert('Failed to post data to service!');
        
    return resp;
}   
/*
<EspNavigationData>
    <Folder name="Attribute Servers" tooltip="Attribute Servers">
        <DynamicFolder menu="rcmenu1"
            name="DefaultAttrServer"
            params="type=repository&amp;subtype=as&amp;name=DefaultAttrServer&amp;netAddress=http://10.150.29.202:8145"
            tooltip="DefaultAttrServer"/>
        <DynamicFolder menu="rcmenu1"
            name="Configured Attribute Server"
            params="type=repository&amp;subtype=as&amp;name=Configured Attribute Server&amp;netAddress=http://10.150.29.202:8145"
            tooltip="Configured Attribute Server"/>
        <DynamicFolder menu="rcmenu1"
            name="attrSvr1"
            params="type=repository&amp;subtype=as&amp;name=attrSvr1&amp;netAddress=http://10.150.64.208:8145"
            tooltip="attrSvr1"/>
    </Folder>
    <Folder name="Roxie Clusters" tooltip="Roxie Clusters">
        <DynamicFolder menu="rcmenu1"
            name="roxie1"
            params="type=repository&amp;subtype=rc&amp;name=roxie1&amp;netAddress=roxieAddr1"
            tooltip="roxie1"/>
    </Folder>
    <Menu name="rcmenu1">
        <MenuItem action="/ws_roxieconfig/NavMenuPublishEvent?parm1=y" name="Publish" tooltip="Publish"/>
    </Menu>
</EspNavigationData>
*/

function addChildItemsFromNavData(xmlNode, parent)
{
    var tree = parent.o_root;

    var a_nodes = xmlNode.getElementsByTagName('Menu');
    var n_nodes = a_nodes.length;
    for (var i=0; i<n_nodes; i++)
    {
        var node = a_nodes[i];
        var menuName = get_xml_attrib(node, 'name', null);
        if (menuName)
        {
            var o_menu = new Array;
            
            var a_menu_items = node.getElementsByTagName('MenuItem');
            var n_menu_items = a_menu_items.length;         
            for (var j=0; j<n_menu_items; j++)
            {
                var menu_item = a_menu_items[j];
                var item_name   = get_xml_attrib(menu_item, 'name', null);
                var item_tip    = get_xml_attrib(menu_item, 'tooltip', null);
                var item_action = get_xml_attrib(menu_item, 'action', null);//handler
                if (item_tip == null)
                    item_tip = item_name;
                o_menu_item = new Array;
                o_menu_item.push(item_name, item_action, item_tip);
                o_menu.push( o_menu_item );
            }           
            tree.add_custom_menu(menu_name, o_menu);
        }
    }

    a_nodes = xmlNode.childNodes;
    n_nodes = a_nodes.length;
    var n_children = n_nodes;
    for (var i=0; i<n_nodes; i++)
    {
        var node = a_nodes[i];
        if (node.nodeName == 'Folder' || node.nodeName == 'DynamicFolder' || node.nodeName == 'Link')
        {
            var item;
            var name      = get_xml_attrib(node, 'name', 'unknown');
            var tooltip = get_xml_attrib(node, 'tooltip', '');
            var params  = get_xml_attrib(node, 'params', '');
            var menu_name = get_xml_attrib(node, 'menu', '');

            //item.nodeName can either be DynamicFolder, Folder or Link
            var item = add_item(tree, parent.n_id, node.nodeName, name, tooltip, menu_name, params);
            if (node.nodeName == 'Folder')
                addChildItemsFromNavData(node, item);
            
            if (menu_name)
            {
                var menu_id = tree.a_custom_menu_ids[menu_name];
                if (menu_id != undefined)
                    item.menu_id = menu_id;
            }
        }
    }//for

    redo_item(parent);

    a_nodes = xmlNode.getElementsByTagName('Exception');
    n_nodes = a_nodes.length;
    if (n_nodes > 0)
    {
        var s = new Array();
        var j = 0;
        s[j++] = 'Exception';
        if (n_nodes>1)
            s[j++] = 's';
        s[j++] = ' encountered:\n';
        for (var i=0; i<n_nodes; i++)
        {
            var node = a_nodes[i];
            var msg  = get_xml_attrib(node, 'message', null);
            if (msg)
            {
                var source = get_xml_attrib(node, 'source', null);
                if (source)
                {
                    s[j++] = source;
                    s[j++] = ': ';
                }
                s[j++] = msg;
                s[j++] = '\n';
            }
        }
        alert(s.join(''));
    }
    return n_children;
}


function toggle(n_id) 
{
    var o_item = this.get_item(n_id);
    var b_expand = !o_item.b_expanded;

    if (this.on_expanding && !this.on_expanding(o_item, b_expand))
        return;

    if (b_expand && o_item.b_load_on_demand)
    {
        if (o_item.a_children.length && !o_item.b_cache_children)
            this.deleteAllChildren(o_item);

        load_children_on_demand(o_item);
    }
    o_item.expand(b_expand);
    //document.all['textarea'].value = document.all['tree_0'].outerHTML;
}

function set_popup_timer()
{
/*
    if (this.timeoutId)
        clearTimeout(this.timeoutId);
    this.timeoutId = window.setTimeout('hide_popup_menu()', 2000);
 */
}

function clear_popup_timer()
{
/*
    if (this.timeoutId)
        clearTimeout(this.timeoutId);
*/
}

function hide_popup_menu()
{
    if (ie)
    {
        if (parent.contextMenu && parent.contextMenu.isOpen)
            parent.contextMenu.hide();
        else
            if (contextMenu && contextMenu.isOpen)
                contextMenu.hide();
    }
    else
        if (ns)
            contextMenu.style.visibility = 'hidden';
}

function get_screen_coords(obj, xy)
{
    if (obj == document.body)
    {
        xy[0] += window.screenLeft;
        xy[1] += window.screenTop;
    }
    else
    {
        get_screen_coords(obj.parentNode, xy);
        xy[0] += obj.offsetLeft;
        xy[1] += obj.offsetTop;
    }
}

function mouse_over_item(n_id) 
{
    if (!window.event)   
        return;
    var item = this.get_item(n_id);
    var  img = document.getElementById('i_img' + this.n_tid + '_' + n_id);
    var link = document.getElementById('i_txt' + this.n_tid + '_' + n_id);
    var tree_div = document.getElementById('tree_' + this.n_tid);
    
    var parentOfTree = tree_div.parentNode;
    if (link.offsetLeft + link.offsetWidth > parentOfTree.offsetWidth)
    {
        var xy = [0, 0];
        get_screen_coords(link, xy)

        EnterContent('ToolTip',null, link.innerText, true); 
        Activate(null, xy[0], xy[1]);
    }
    else
        deActivate();
        
    /*
    if (!contextMenu || !contextMenu.isOpen)
        this.oncontextmenu(n_id);
    else
    {
        this.select(n_id, e);
        var popupBody = contextMenu.document.body;
        contextMenu.show(link.offsetWidth, 0, popupBody.offsetWidth, popupBody.offsetHeight, link);
    }  
    set_popup_timer();
    */
    item.upstatus(true);
}

function mouse_left_item(n_id) 
{
    if (!window.event)   
        return;
    deActivate()
    set_popup_timer();
    this.get_item(n_id).upstatus();
}


var contextMenu = null;
var trees = [];
