/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
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