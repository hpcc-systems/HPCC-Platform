/*
 * Copyright (c) 2007, Yahoo! Inc. All rights reserved.
 * Code licensed under the BSD License:
 * http://developer.yahoo.net/yui/license.txt
 * version: 2.2.0
*/
/* Specialized Autocomplete widget used to feed a DataTable Row Filter;
 * Copyright (c) 2007, Victor Morales. All rights reserved.
 * Code licensed under the BSD License.
*/

YAHOO.namespace("dpu.widget");


YAHOO.dpu.widget.RowFilter = function( elInput,elContainer,oDataTable,fnFilter,oConfigs) {
        if (arguments.length > 0) {
            YAHOO.dpu.widget.RowFilter.superclass.constructor.call(this, elInput,elContainer,fnFilter,oConfigs);
        }
        this.Filter=fnFilter;
        this._oDataTable=oDataTable;
        this.itemSelectEvent.subscribe(this.myOnSelect);
        this.dataReturnEvent.subscribe(this.myOnDataReturn);
        this._oDataTable.subscribe("columnSortEvent",this.updateFilter,this._oDataTable,this)
}
            
// Inherit from YAHOO.widget.DataTable
YAHOO.lang.extend(YAHOO.dpu.widget.RowFilter, YAHOO.widget.AutoComplete);

YAHOO.dpu.widget.RowFilter.prototype.updateFilter=function(oColumn,oDataTable) {
    var records=oDataTable.getRecordSet().getRecords();
    var startRow=this.filterCount;

    this.Filter._aData=records;
    
    if (oDataTable.isFiltered) {
        this.hideColumns(); 
    }   
};

YAHOO.dpu.widget.RowFilter.prototype.queryDelay=0;
YAHOO.dpu.widget.RowFilter.prototype.formatResult = function(aResultItem, sQuery) {
    var output;
    //start index of match
    var m= aResultItem[1].matchIndex;
    if(m!=undefined) {
        //Query length
        var ql=sQuery.length;
        //The result itself
        var r= aResultItem[1].value;
        var pm= r.substring(0,m);
        var q= r.substr(m,ql);
        var nm=r.substring(m+ql,r.length);
        var aMarkup = ["<div id='ysearchresult'>",
        pm,  
        "<span style='font-weight:bold'>",
        q,
        "</span>",
        nm,
        "</div>"];
        output=aMarkup.join("")
    }
    return output
};

YAHOO.dpu.widget.RowFilter.prototype._oDataTable=null;

// Define Custom Event handlers
YAHOO.dpu.widget.RowFilter.prototype.myOnDataReturn= function(sType, aArgs) {
    var oAutoComp = aArgs[0];
    var sQuery = aArgs[1];
    var aResults = aArgs[2];
    
    if(aResults.length == 0) {
        oAutoComp.setBody("<div id=\"container_default\">No matching results</div>");
    }
    
    this.reset();
}

YAHOO.dpu.widget.RowFilter.prototype.reset= function() {
    var isReset=false
    var oDataTable=this._oDataTable
    
    if (oDataTable.isFiltered) {
        oDataTable.filterRows();
        this.Filter._aData=oDataTable.getRecordSet().getRecords()
        this.hideColumns();        
        isReset=true;
    }
    return isReset;
}
YAHOO.dpu.widget.RowFilter.prototype.hideColumns=function() {
    var oDataTable=this._oDataTable
    var colArray=oDataTable.aColState
    var startRow=this.filterCount;
    for (var i=0; i<colArray.length;i++) {
        if(colArray[i]===1) {
            oDataTable.hideSwap(i,'none',startRow) 
        }
    }
}
YAHOO.dpu.widget.RowFilter.prototype.filterCount=0;

YAHOO.dpu.widget.RowFilter.prototype.myOnSelect= function(sType, aArgs) {
    var objResult = aArgs[2][1];
    this._oDataTable.filterRows(objResult.matchedRows)
    this.filterCount=objResult.matchedRows.length
}


YAHOO.namespace("dpu.util");

YAHOO.dpu.util.StringFilter=function(aRecords, sFieldName, oConfigs) {
    if(typeof oConfigs == "object") {
        for(var sConfig in oConfigs) {
            this[sConfig] = oConfigs[sConfig];
        }
    }
    this._aData=aRecords;
    this.schemaItem=sFieldName;
    this._init();
};

YAHOO.dpu.util.StringFilter.prototype = new YAHOO.util.DataSource();

YAHOO.dpu.util.StringFilter.prototype.doQuery = function(oCallbackFn, sQuery, oParent) {
    
    var aResults = [];
    
    aResults = this.fnFilter(sQuery);
    if(aResults === null) {
        this.dataErrorEvent.fire(this, oParent, sQuery, YAHOO.util.DataSource.ERROR_DATANULL);
        return;
    }
    
    var resultObj = {};
    resultObj.query = decodeURIComponent(sQuery);
    resultObj.results = aResults;
    this._addCacheElem(resultObj);
    
    this.getResultsEvent.fire(this, oParent, sQuery, aResults);
    oCallbackFn(sQuery, aResults, oParent);
    return;
};
YAHOO.dpu.util.StringFilter.prototype.schemaItem=null;
YAHOO.dpu.util.StringFilter.prototype.fnFilter=function(sQuery) {
    sQuery=unescape(sQuery);
    var aResults = [];
    var aData= this._aData;
    var fName= this.schemaItem
    if(sQuery && sQuery.length > 0) {
        var q= sQuery.toLowerCase();
        var updateResult=false;
        var elHashTable={}              
        for (var i=0; i<aData.length; i++) {
            var field=aData[i]._oData[fName]
            var updateResult=false;
          
            if(elHashTable[field]) {
                //Update Hashtable entry with the additional row matched 
                 elHashTable[field].rows.push(i)
                 updateResult=true;
            }
            else {
                 elHashTable[field]= {rows:[i], resultIndex:-1};       
            }
             
                //Save the index of the match
            var mIndex=field.toLowerCase().indexOf(q)
            var objResult={value:field, matchIndex:mIndex, matchedRows:[i]}
              
            if (mIndex<0) { continue;}
              
            if(updateResult){
                var ri= elHashTable[field].resultIndex;
                objResult.matchedRows=elHashTable[field].rows;
                aResults[ri]=[objResult.value,objResult]    
            }
            else {
                  aResults.push([objResult.value,objResult]);  
                  //Update the hashtable resultIndex   
                  elHashTable[field].resultIndex=aResults.length-1
                  var ri= elHashTable[field].resultIndex;
            }
         
        }
    }
        return aResults;

}

