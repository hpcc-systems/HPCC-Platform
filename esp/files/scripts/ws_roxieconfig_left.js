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

//Here's the code that will set up our Left Nav instance. 

var tree, currentIconMode;

(function(){


    function changeIconMode() {
        var newVal = parseInt(this.value);
        if (newVal != currentIconMode) {
            currentIconMode = newVal;
        }
        buildTree();
    }
    
   tree = new YAHOO.widget.TreeView("espNavTree");
   tree.setDynamicLoad(loadNodeData, currentIconMode);   

   var root = tree.getRoot();
   var attributeServersNode = new YAHOO.widget.TextNode({label: "Attribute Servers", expanded:true, dynamicLoadComplete:true}, root);
   var tmpNode2 = new YAHOO.widget.TextNode({label: "dataland"}, attributeServersNode);
   tmpNode2.labelStyle = "icon-doc";  
   var tmpNode3 = new YAHOO.widget.TextNode({label: "prod"}, attributeServersNode);
   tmpNode3.labelStyle = "icon-doc";  
   var tmpNode4 = new YAHOO.widget.TextNode({label: "qa", expanded: false}, attributeServersNode);
   tmpNode4.labelStyle = "icon-doc";  

   var roxieClustersNode = new YAHOO.widget.TextNode({label: "Roxie Clusters", expanded:true, dynamicLoadComplete:true}, root);

   var tmpNode5 = new YAHOO.widget.TextNode({label: "QA-Roxie"}, roxieClustersNode);
   tmpNode5.labelStyle = "icon-doc";  
   var tmpNode6 = new YAHOO.widget.TextNode({label: "Staging", expanded: false}, roxieClustersNode);
   tmpNode6.labelStyle = "icon-doc";  
   var tmpNode7 = new YAHOO.widget.TextNode({label: "40-Way"}, roxieClustersNode);
   tmpNode7.labelStyle = "icon-doc";  

   tree.render();

})();

function loadNodeData(node, fnLoadComplete)  {
    
    //We'll load node data based on what we get back when we
    //use Connection Manager topass the text label of the 
    //expanding node to the Yahoo!
    //Search "related suggestions" API.  Here, we're at the 
    //first part of the request -- we'll make the request to the
    //server.  In our success handler, we'll build our new children
    //and then return fnLoadComplete back to the tree.
    
    //Get the node's label and urlencode it; this is the word/s
    //on which we'll search for related words:
    var nodeLabel = encodeURI(node.label);
    
    //prepare URL for XHR request:
    var sUrl = "assets/ysuggest_proxy.php?query=" + nodeLabel;
    
    //prepare our callback object
    var callback = {
    
        //if our XHR call is successful, we want to make use
        //of the returned data and create child nodes.
        success: function(oResponse) {
            YAHOO.log("XHR transaction was successful.", "info", "example");
            //YAHOO.log(oResponse.responseText);
            var oResults = eval("(" + oResponse.responseText + ")");
            if((oResults.ResultSet.Result) && (oResults.ResultSet.Result.length)) {
                //Result is an array if more than one result, string otherwise
                if(YAHOO.lang.isArray(oResults.ResultSet.Result)) {
                    for (var i=0, j=oResults.ResultSet.Result.length; i<j; i++) {
                        var tempNode = new YAHOO.widget.TextNode(oResults.ResultSet.Result[i], node, false);
                    }
                } else {
                    //there is only one result; comes as string:
                    var tempNode = new YAHOO.widget.TextNode(oResults.ResultSet.Result, node, false)
                }
            }
            
            //When we're done creating child nodes, we execute the node's
            //loadComplete callback method which comes in via the argument
            //in the response object (we could also access it at node.loadComplete,
            //if necessary):
            oResponse.argument.fnLoadComplete();
        },
        
        //if our XHR call is not successful, we want to
        //fire the TreeView callback and let the Tree
        //proceed with its business.
        failure: function(oResponse) {
            YAHOO.log("Failed to process XHR transaction.", "info", "example");
            oResponse.argument.fnLoadComplete();
        },
        
        //our handlers for the XHR response will need the same
        //argument information we got to loadNodeData, so
        //we'll pass those along:
        argument: {
            "node": node,
            "fnLoadComplete": fnLoadComplete
        },
        
        //timeout -- if more than 7 seconds go by, we'll abort
        //the transaction and assume there are no children:
        timeout: 7000
    };
    
    //With our callback object ready, it's now time to 
    //make our XHR call using Connection Manager's
    //asyncRequest method:
    YAHOO.util.Connect.asyncRequest('GET', sUrl, callback);
}

