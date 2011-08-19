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

//Here's the code that will set up our TabView instance.  We'll
//write this function and then tell Loader to fire it once it's done
//loading TabView into the page.
(function(){
    
   var tree = new YAHOO.widget.TreeView("espNavTree");
   var root = tree.getRoot();
   var tmpNode = new YAHOO.widget.TextNode({label: "mylabel1", expanded: false}, root);
   var tmpNode2 = new YAHOO.widget.TextNode({label: "mylabel1-1", expanded: false}, tmpNode);

   tree.render();

})();