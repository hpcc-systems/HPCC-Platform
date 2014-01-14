//>>built
require({cache:{"url:dojox/calendar/templates/MatrixView.html":"<div data-dojo-attach-events=\"keydown:_onKeyDown\">\n\t<div  class=\"dojoxCalendarYearColumnHeader\" data-dojo-attach-point=\"yearColumnHeader\">\n\t\t<table><tr><td><span data-dojo-attach-point=\"yearColumnHeaderContent\"></span></td></tr></table>\t\t\n\t</div>\t\n\t<div data-dojo-attach-point=\"columnHeader\" class=\"dojoxCalendarColumnHeader\">\n\t\t<table data-dojo-attach-point=\"columnHeaderTable\" class=\"dojoxCalendarColumnHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\t\n\t<div dojoAttachPoint=\"rowHeader\" class=\"dojoxCalendarRowHeader\">\n\t\t<table data-dojo-attach-point=\"rowHeaderTable\" class=\"dojoxCalendarRowHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\n\t<div dojoAttachPoint=\"grid\" class=\"dojoxCalendarGrid\">\n\t\t<table data-dojo-attach-point=\"gridTable\" class=\"dojoxCalendarGridTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\n\t<div data-dojo-attach-point=\"itemContainer\" class=\"dojoxCalendarContainer\" data-dojo-attach-event=\"mousedown:_onGridMouseDown,mouseup:_onGridMouseUp,ondblclick:_onGridDoubleClick,touchstart:_onGridTouchStart,touchmove:_onGridTouchMove,touchend:_onGridTouchEnd\">\n\t\t<table data-dojo-attach-point=\"itemContainerTable\" class=\"dojoxCalendarContainerTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t</div>\t\n</div>\n"}});
define("dojox/calendar/MatrixView",["dojo/_base/declare","dojo/_base/array","dojo/_base/event","dojo/_base/lang","dojo/_base/sniff","dojo/_base/fx","dojo/_base/html","dojo/on","dojo/dom","dojo/dom-class","dojo/dom-style","dojo/dom-geometry","dojo/dom-construct","dojo/query","dojox/html/metrics","dojo/i18n","./ViewBase","dojo/text!./templates/MatrixView.html","dijit/_TemplatedMixin"],function(_1,_2,_3,_4,_5,fx,_6,on,_7,_8,_9,_a,_b,_c,_d,_e,_f,_10,_11){
return _1("dojox.calendar.MatrixView",[_f,_11],{templateString:_10,baseClass:"dojoxCalendarMatrixView",_setTabIndexAttr:"domNode",viewKind:"matrix",renderData:null,startDate:null,refStartTime:null,refEndTime:null,columnCount:7,rowCount:5,horizontalRenderer:null,labelRenderer:null,expandRenderer:null,percentOverlap:0,verticalGap:2,horizontalRendererHeight:17,labelRendererHeight:14,expandRendererHeight:15,cellPaddingTop:16,expandDuration:300,expandEasing:null,layoutDuringResize:false,roundToDay:true,showCellLabel:true,scrollable:false,resizeCursor:"e-resize",constructor:function(){
this.invalidatingProperties=["columnCount","rowCount","startDate","horizontalRenderer","labelRenderer","expandRenderer","rowHeaderDatePattern","columnHeaderLabelLength","cellHeaderShortPattern","cellHeaderLongPattern","percentOverlap","verticalGap","horizontalRendererHeight","labelRendererHeight","expandRendererHeight","cellPaddingTop","roundToDay","itemToRendererKindFunc","layoutPriorityFunction","formatItemTimeFunc","textDir","items"];
this._ddRendererList=[];
this._ddRendererPool=[];
this._rowHeaderHandles=[];
},destroy:function(_12){
this._cleanupRowHeader();
this.inherited(arguments);
},postCreate:function(){
this.inherited(arguments);
this._initialized=true;
if(!this.invalidRendering){
this.refreshRendering();
}
},_createRenderData:function(){
var rd={};
rd.dateLocaleModule=this.dateLocaleModule;
rd.dateClassObj=this.dateClassObj;
rd.dateModule=this.dateModule;
rd.dates=[];
rd.columnCount=this.get("columnCount");
rd.rowCount=this.get("rowCount");
rd.sheetHeight=this.itemContainer.offsetHeight;
this._computeRowsHeight(rd);
var d=this.get("startDate");
if(d==null){
d=new rd.dateClassObj();
}
d=this.floorToDay(d,false,rd);
this.startDate=d;
for(var row=0;row<rd.rowCount;row++){
rd.dates.push([]);
for(var col=0;col<rd.columnCount;col++){
rd.dates[row].push(d);
d=rd.dateModule.add(d,"day",1);
d=this.floorToDay(d,false,rd);
}
}
rd.startTime=this.newDate(rd.dates[0][0],rd);
rd.endTime=this.newDate(rd.dates[rd.rowCount-1][rd.columnCount-1],rd);
rd.endTime=rd.dateModule.add(rd.endTime,"day",1);
rd.endTime=this.floorToDay(rd.endTime,true);
if(this.displayedItemsInvalidated&&!this._isEditing){
this.displayedItemsInvalidated=false;
this._computeVisibleItems(rd);
}else{
if(this.renderData){
rd.items=this.renderData.items;
}
}
rd.rtl=!this.isLeftToRight();
return rd;
},_validateProperties:function(){
this.inherited(arguments);
if(this.columnCount<1||isNaN(this.columnCount)){
this.columnCount=1;
}
if(this.rowCount<1||isNaN(this.rowCount)){
this.rowCount=1;
}
if(isNaN(this.percentOverlap)||this.percentOverlap<0||this.percentOverlap>100){
this.percentOverlap=0;
}
if(isNaN(this.verticalGap)||this.verticalGap<0){
this.verticalGap=2;
}
if(isNaN(this.horizontalRendererHeight)||this.horizontalRendererHeight<1){
this.horizontalRendererHeight=17;
}
if(isNaN(this.labelRendererHeight)||this.labelRendererHeight<1){
this.labelRendererHeight=14;
}
if(isNaN(this.expandRendererHeight)||this.expandRendererHeight<1){
this.expandRendererHeight=15;
}
},_setStartDateAttr:function(_13){
this.displayedItemsInvalidated=true;
this._set("startDate",_13);
},_setColumnCountAttr:function(_14){
this.displayedItemsInvalidated=true;
this._set("columnCount",_14);
},_setRowCountAttr:function(_15){
this.displayedItemsInvalidated=true;
this._set("rowCount",_15);
},__fixEvt:function(e){
e.sheet="primary";
e.source=this;
return e;
},_formatRowHeaderLabel:function(d){
if(this.rowHeaderDatePattern){
return this.renderData.dateLocaleModule.format(d,{selector:"date",datePattern:this.rowHeaderDatePattern});
}else{
return this.getWeekNumberLabel(d);
}
},_formatColumnHeaderLabel:function(d){
return this.renderData.dateLocaleModule.getNames("days",this.columnHeaderLabelLength?this.columnHeaderLabelLength:"wide","standAlone")[d.getDay()];
},_formatGridCellLabel:function(d,row,col){
var _16=row==0&&col==0||d.getDate()==1;
var _17,rb;
if(_16){
if(this.cellHeaderLongPattern){
_17=this.cellHeaderLongPattern;
}else{
rb=_e.getLocalization("dojo.cldr",this._calendar);
_17=rb["dateFormatItem-MMMd"];
}
}else{
if(this.cellHeaderShortPattern){
_17=this.cellHeaderShortPattern;
}else{
rb=_e.getLocalization("dojo.cldr",this._calendar);
_17=rb["dateFormatItem-d"];
}
}
return this.renderData.dateLocaleModule.format(d,{selector:"date",datePattern:_17});
},refreshRendering:function(){
this.inherited(arguments);
if(!this.domNode){
return;
}
this._validateProperties();
var _18=this.renderData;
this.renderData=this._createRenderData();
this._createRendering(this.renderData,_18);
this._layoutRenderers(this.renderData);
},_createRendering:function(_19,_1a){
if(_19.rowHeight<=0){
_19.columnCount=1;
_19.rowCount=1;
_19.invalidRowHeight=true;
return;
}
if(_1a){
if(this.itemContainerTable){
var _1b=_c(".dojoxCalendarItemContainerRow",this.itemContainerTable);
_1a.rowCount=_1b.length;
}
}
this._buildColumnHeader(_19,_1a);
this._buildRowHeader(_19,_1a);
this._buildGrid(_19,_1a);
this._buildItemContainer(_19,_1a);
if(this.buttonContainer&&this.owner!=null&&this.owner.currentView==this){
_9.set(this.buttonContainer,{"right":0,"left":0});
}
},_buildColumnHeader:function(_1c,_1d){
var _1e=this.columnHeaderTable;
if(!_1e){
return;
}
var _1f=_1c.columnCount-(_1d?_1d.columnCount:0);
if(_5("ie")==8){
if(this._colTableSave==null){
this._colTableSave=_4.clone(_1e);
}else{
if(_1f<0){
this.columnHeader.removeChild(_1e);
_b.destroy(_1e);
_1e=_4.clone(this._colTableSave);
this.columnHeaderTable=_1e;
this.columnHeader.appendChild(_1e);
_1f=_1c.columnCount;
}
}
}
var _20=_c("tbody",_1e);
var trs=_c("tr",_1e);
var _21,tr,td;
if(_20.length==1){
_21=_20[0];
}else{
_21=_6.create("tbody",null,_1e);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_b.create("tr",null,_21);
}
if(_1f>0){
for(var i=0;i<_1f;i++){
td=_b.create("td",null,tr);
}
}else{
_1f=-_1f;
for(var i=0;i<_1f;i++){
tr.removeChild(tr.lastChild);
}
}
_c("td",_1e).forEach(function(td,i){
td.className="";
var d=_1c.dates[0][i];
this._setText(td,this._formatColumnHeaderLabel(d));
if(i==0){
_8.add(td,"first-child");
}else{
if(i==this.renderData.columnCount-1){
_8.add(td,"last-child");
}
}
this.styleColumnHeaderCell(td,d,_1c);
},this);
if(this.yearColumnHeaderContent){
var d=_1c.dates[0][0];
this._setText(this.yearColumnHeaderContent,_1c.dateLocaleModule.format(d,{selector:"date",datePattern:"yyyy"}));
}
},styleColumnHeaderCell:function(_22,_23,_24){
_8.add(_22,this._cssDays[_23.getDay()]);
if(this.isWeekEnd(_23)){
_8.add(_22,"dojoxCalendarWeekend");
}
},_rowHeaderHandles:null,_cleanupRowHeader:function(){
while(this._rowHeaderHandles.length>0){
var _25=this._rowHeaderHandles.pop();
while(_25.length>0){
_25.pop().remove();
}
}
},_rowHeaderClick:function(e){
var _26=_c("td",this.rowHeaderTable).indexOf(e.currentTarget);
this._onRowHeaderClick({index:_26,date:this.renderData.dates[_26][0],triggerEvent:e});
},_buildRowHeader:function(_27,_28){
var _29=this.rowHeaderTable;
if(!_29){
return;
}
var _2a=_c("tbody",_29);
var _2b,tr,td;
if(_2a.length==1){
_2b=_2a[0];
}else{
_2b=_b.create("tbody",null,_29);
}
var _2c=_27.rowCount-(_28?_28.rowCount:0);
if(_2c>0){
for(var i=0;i<_2c;i++){
tr=_b.create("tr",null,_2b);
td=_b.create("td",null,tr);
var h=[];
h.push(on(td,"click",_4.hitch(this,this._rowHeaderClick)));
if(!_5("touch")){
h.push(on(td,"mousedown",function(e){
_8.add(e.currentTarget,"Active");
}));
h.push(on(td,"mouseup",function(e){
_8.remove(e.currentTarget,"Active");
}));
h.push(on(td,"mouseover",function(e){
_8.add(e.currentTarget,"Hover");
}));
h.push(on(td,"mouseout",function(e){
_8.remove(e.currentTarget,"Hover");
}));
}
this._rowHeaderHandles.push(h);
}
}else{
_2c=-_2c;
for(var i=0;i<_2c;i++){
_2b.removeChild(_2b.lastChild);
var _2d=this._rowHeaderHandles.pop();
while(_2d.length>0){
_2d.pop().remove();
}
}
}
_c("tr",_29).forEach(function(tr,i){
_9.set(tr,"height",this._getRowHeight(i)+"px");
var d=_27.dates[i][0];
var td=_c("td",tr)[0];
td.className="";
if(i==0){
_8.add(td,"first-child");
}
if(i==this.renderData.rowCount-1){
_8.add(td,"last-child");
}
this.styleRowHeaderCell(td,d,_27);
this._setText(td,this._formatRowHeaderLabel(d));
},this);
},styleRowHeaderCell:function(_2e,_2f,_30){
},_buildGrid:function(_31,_32){
var _33=this.gridTable;
if(!_33){
return;
}
var _34=_c("tr",_33);
var _35=_31.rowCount-_34.length;
var _36=_35>0;
var _37=_31.columnCount-(_32?_32.columnCount:0);
if(_5("ie")==8){
if(this._gridTableSave==null){
this._gridTableSave=_4.clone(_33);
}else{
if(_37<0){
this.grid.removeChild(_33);
_b.destroy(_33);
_33=_4.clone(this._gridTableSave);
this.gridTable=_33;
this.grid.appendChild(_33);
_37=_31.columnCount;
_35=_31.rowCount;
_36=true;
}
}
}
var _38=_c("tbody",_33);
var _39;
if(_38.length==1){
_39=_38[0];
}else{
_39=_b.create("tbody",null,_33);
}
if(_36){
for(var i=0;i<_35;i++){
_b.create("tr",null,_39);
}
}else{
_35=-_35;
for(var i=0;i<_35;i++){
_39.removeChild(_39.lastChild);
}
}
var _3a=_31.rowCount-_35;
var _3b=_36||_37>0;
_37=_3b?_37:-_37;
_c("tr",_33).forEach(function(tr,i){
if(_3b){
var len=i>=_3a?_31.columnCount:_37;
for(var i=0;i<len;i++){
var td=_b.create("td",null,tr);
_b.create("span",null,td);
}
}else{
for(var i=0;i<_37;i++){
tr.removeChild(tr.lastChild);
}
}
});
_c("tr",_33).forEach(function(tr,row){
_9.set(tr,"height",this._getRowHeight(row)+"px");
tr.className="";
if(row==0){
_8.add(tr,"first-child");
}
if(row==_31.rowCount-1){
_8.add(tr,"last-child");
}
_c("td",tr).forEach(function(td,col){
td.className="";
if(col==0){
_8.add(td,"first-child");
}
if(col==_31.columnCount-1){
_8.add(td,"last-child");
}
var d=_31.dates[row][col];
var _3c=_c("span",td)[0];
this._setText(_3c,this.showCellLabel?this._formatGridCellLabel(d,row,col):null);
this.styleGridCell(td,d,_31);
},this);
},this);
},styleGridCellFunc:null,defaultStyleGridCell:function(_3d,_3e,_3f){
_8.add(_3d,this._cssDays[_3e.getDay()]);
var cal=this.dateModule;
if(this.isToday(_3e)){
_8.add(_3d,"dojoxCalendarToday");
}else{
if(this.refStartTime!=null&&this.refEndTime!=null&&(cal.compare(_3e,this.refEndTime)>=0||cal.compare(cal.add(_3e,"day",1),this.refStartTime)<=0)){
_8.add(_3d,"dojoxCalendarDayDisabled");
}else{
if(this.isWeekEnd(_3e)){
_8.add(_3d,"dojoxCalendarWeekend");
}
}
}
},styleGridCell:function(_40,_41,_42){
if(this.styleGridCellFunc){
this.styleGridCellFunc(_40,_41,_42);
}else{
this.defaultStyleGridCell(_40,_41,_42);
}
},_buildItemContainer:function(_43,_44){
var _45=this.itemContainerTable;
if(!_45){
return;
}
var _46=[];
var _47=_43.rowCount-(_44?_44.rowCount:0);
if(_5("ie")==8){
if(this._itemTableSave==null){
this._itemTableSave=_4.clone(_45);
}else{
if(_47<0){
this.itemContainer.removeChild(_45);
this._recycleItemRenderers(true);
this._recycleExpandRenderers(true);
_b.destroy(_45);
_45=_4.clone(this._itemTableSave);
this.itemContainerTable=_45;
this.itemContainer.appendChild(_45);
_47=_43.columnCount;
}
}
}
var _48=_c("tbody",_45);
var _49,tr,td,div;
if(_48.length==1){
_49=_48[0];
}else{
_49=_b.create("tbody",null,_45);
}
if(_47>0){
for(var i=0;i<_47;i++){
tr=_b.create("tr",null,_49);
_8.add(tr,"dojoxCalendarItemContainerRow");
td=_b.create("td",null,tr);
div=_b.create("div",null,td);
_8.add(div,"dojoxCalendarContainerRow");
}
}else{
_47=-_47;
for(var i=0;i<_47;i++){
_49.removeChild(_49.lastChild);
}
}
_c(".dojoxCalendarItemContainerRow",_45).forEach(function(tr,i){
_9.set(tr,"height",this._getRowHeight(i)+"px");
_46.push(tr.childNodes[0].childNodes[0]);
},this);
_43.cells=_46;
},resize:function(_4a){
this.inherited(arguments);
this._resizeHandler(null,false);
},_resizeHandler:function(e,_4b){
var rd=this.renderData;
if(rd==null){
this.refreshRendering();
return;
}
if(rd.sheetHeight!=this.itemContainer.offsetHeight){
rd.sheetHeight=this.itemContainer.offsetHeight;
var _4c=this.getExpandedRowIndex();
if(_4c==-1){
this._computeRowsHeight();
this._resizeRows();
}else{
this.expandRow(rd.expandedRow,rd.expandedRowCol,0,null,true);
}
if(rd.invalidRowHeight){
delete rd.invalidRowHeight;
this.renderData=null;
this.displayedItemsInvalidated=true;
this.refreshRendering();
return;
}
}
if(this.layoutDuringResize||_4b){
setTimeout(_4.hitch(this,function(){
this._layoutRenderers(this.renderData);
}),20);
}else{
_9.set(this.itemContainer,"opacity",0);
this._recycleItemRenderers();
this._recycleExpandRenderers();
if(this._resizeTimer!=undefined){
clearTimeout(this._resizeTimer);
}
this._resizeTimer=setTimeout(_4.hitch(this,function(){
delete this._resizeTimer;
this._resizeRowsImpl(this.itemContainer,"tr");
this._layoutRenderers(this.renderData);
if(this.resizeAnimationDuration==0){
_9.set(this.itemContainer,"opacity",1);
}else{
fx.fadeIn({node:this.itemContainer,curve:[0,1]}).play(this.resizeAnimationDuration);
}
}),200);
}
},resizeAnimationDuration:0,getExpandedRowIndex:function(){
return this.renderData.expandedRow==null?-1:this.renderData.expandedRow;
},collapseRow:function(_4d,_4e,_4f){
var rd=this.renderData;
if(_4f==undefined){
_4f=true;
}
if(_4d==undefined){
_4d=this.expandDuration;
}
if(rd&&rd.expandedRow!=null&&rd.expandedRow!=-1){
if(_4f&&_4d){
var _50=rd.expandedRow;
var _51=rd.expandedRowHeight;
delete rd.expandedRow;
this._computeRowsHeight(rd);
var _52=this._getRowHeight(_50);
rd.expandedRow=_50;
this._recycleExpandRenderers();
this._recycleItemRenderers();
_9.set(this.itemContainer,"display","none");
this._expandAnimation=new fx.Animation({curve:[_51,_52],duration:_4d,easing:_4e,onAnimate:_4.hitch(this,function(_53){
this._expandRowImpl(Math.floor(_53));
}),onEnd:_4.hitch(this,function(_54){
this._expandAnimation=null;
this._collapseRowImpl(false);
this._resizeRows();
_9.set(this.itemContainer,"display","block");
setTimeout(_4.hitch(this,function(){
this._layoutRenderers(rd);
}),100);
this.onExpandAnimationEnd(false);
})});
this._expandAnimation.play();
}else{
this._collapseRowImpl(_4f);
}
}
},_collapseRowImpl:function(_55){
var rd=this.renderData;
delete rd.expandedRow;
delete rd.expandedRowHeight;
this._computeRowsHeight(rd);
if(_55==undefined||_55){
this._resizeRows();
this._layoutRenderers(rd);
}
},expandRow:function(_56,_57,_58,_59,_5a){
var rd=this.renderData;
if(!rd||_56<0||_56>=rd.rowCount){
return -1;
}
if(_57==undefined||_57<0||_57>=rd.columnCount){
_57=-1;
}
if(_5a==undefined){
_5a=true;
}
if(_58==undefined){
_58=this.expandDuration;
}
if(_59==undefined){
_59=this.expandEasing;
}
var _5b=this._getRowHeight(_56);
var _5c=rd.sheetHeight-Math.ceil(this.cellPaddingTop*(rd.rowCount-1));
rd.expandedRow=_56;
rd.expandedRowCol=_57;
rd.expandedRowHeight=_5c;
if(_5a){
if(_58){
this._recycleExpandRenderers();
this._recycleItemRenderers();
_9.set(this.itemContainer,"display","none");
this._expandAnimation=new fx.Animation({curve:[_5b,_5c],duration:_58,delay:50,easing:_59,onAnimate:_4.hitch(this,function(_5d){
this._expandRowImpl(Math.floor(_5d));
}),onEnd:_4.hitch(this,function(){
this._expandAnimation=null;
_9.set(this.itemContainer,"display","block");
setTimeout(_4.hitch(this,function(){
this._expandRowImpl(_5c,true);
}),100);
this.onExpandAnimationEnd(true);
})});
this._expandAnimation.play();
}else{
this._expandRowImpl(_5c);
}
}
},_expandRowImpl:function(_5e,_5f){
var rd=this.renderData;
rd.expandedRowHeight=_5e;
this._computeRowsHeight(rd,rd.sheetHeight-_5e);
this._resizeRows();
if(_5f){
this._layoutRenderers(rd);
}
},onExpandAnimationEnd:function(_60){
},_resizeRows:function(){
if(this._getRowHeight(0)<=0){
return;
}
if(this.rowHeaderTable){
this._resizeRowsImpl(this.rowHeaderTable,"tr");
}
if(this.gridTable){
this._resizeRowsImpl(this.gridTable,"tr");
}
if(this.itemContainerTable){
this._resizeRowsImpl(this.itemContainerTable,"tr");
}
},_computeRowsHeight:function(_61,max){
var rd=_61==null?this.renderData:_61;
max=max||rd.sheetHeight;
max--;
if(_5("ie")==7){
max-=rd.rowCount;
}
if(rd.rowCount==1){
rd.rowHeight=max;
rd.rowHeightFirst=max;
rd.rowHeightLast=max;
return;
}
var _62=rd.expandedRow==null?rd.rowCount:rd.rowCount-1;
var rhx=max/_62;
var rhf,rhl,rh;
var _63=max-(Math.floor(rhx)*_62);
var _64=Math.abs(max-(Math.ceil(rhx)*_62));
var _65;
var _66=1;
if(_63<_64){
rh=Math.floor(rhx);
_65=_63;
}else{
_66=-1;
rh=Math.ceil(rhx);
_65=_64;
}
rhf=rh+_66*Math.floor(_65/2);
rhl=rhf+_66*(_65%2);
rd.rowHeight=rh;
rd.rowHeightFirst=rhf;
rd.rowHeightLast=rhl;
},_getRowHeight:function(_67){
var rd=this.renderData;
if(_67==rd.expandedRow){
return rd.expandedRowHeight;
}else{
if(rd.expandedRow==0&&_67==1||_67==0){
return rd.rowHeightFirst;
}else{
if(rd.expandedRow==this.renderData.rowCount-1&&_67==this.renderData.rowCount-2||_67==this.renderData.rowCount-1){
return rd.rowHeightLast;
}else{
return rd.rowHeight;
}
}
}
},_resizeRowsImpl:function(_68,_69){
dojo.query(_69,_68).forEach(function(tr,i){
_9.set(tr,"height",this._getRowHeight(i)+"px");
},this);
},_setHorizontalRendererAttr:function(_6a){
this._destroyRenderersByKind("horizontal");
this._set("horizontalRenderer",_6a);
},_setLabelRendererAttr:function(_6b){
this._destroyRenderersByKind("label");
this._set("labelRenderer",_6b);
},_destroyExpandRenderer:function(_6c){
if(_6c["destroyRecursive"]){
_6c.destroyRecursive();
}
_6.destroy(_6c.domNode);
},_setExpandRendererAttr:function(_6d){
while(this._ddRendererList.length>0){
this._destroyExpandRenderer(this._ddRendererList.pop());
}
var _6e=this._ddRendererPool;
if(_6e){
while(_6e.length>0){
this._destroyExpandRenderer(_6e.pop());
}
}
this._set("expandRenderer",_6d);
},_ddRendererList:null,_ddRendererPool:null,_getExpandRenderer:function(_6f,_70,_71,_72,_73){
if(this.expandRenderer==null){
return null;
}
var ir=this._ddRendererPool.pop();
if(ir==null){
ir=new this.expandRenderer();
}
this._ddRendererList.push(ir);
ir.set("owner",this);
ir.set("date",_6f);
ir.set("items",_70);
ir.set("rowIndex",_71);
ir.set("columnIndex",_72);
ir.set("expanded",_73);
return ir;
},_recycleExpandRenderers:function(_74){
for(var i=0;i<this._ddRendererList.length;i++){
var ir=this._ddRendererList[i];
ir.set("Up",false);
ir.set("Down",false);
if(_74){
ir.domNode.parentNode.removeChild(ir.domNode);
}
_9.set(ir.domNode,"display","none");
}
this._ddRendererPool=this._ddRendererPool.concat(this._ddRendererList);
this._ddRendererList=[];
},_defaultItemToRendererKindFunc:function(_75){
var dur=Math.abs(this.renderData.dateModule.difference(_75.startTime,_75.endTime,"minute"));
return dur>=1440?"horizontal":"label";
},naturalRowsHeight:null,_roundItemToDay:function(_76){
var s=_76.startTime,e=_76.endTime;
if(!this.isStartOfDay(s)){
s=this.floorToDay(s,false,this.renderData);
}
if(!this.isStartOfDay(e)){
e=this.renderData.dateModule.add(e,"day",1);
e=this.floorToDay(e,true);
}
return {startTime:s,endTime:e};
},_sortItemsFunction:function(a,b){
if(this.roundToDay){
a=this._roundItemToDay(a);
b=this._roundItemToDay(b);
}
var res=this.dateModule.compare(a.startTime,b.startTime);
if(res==0){
res=-1*this.dateModule.compare(a.endTime,b.endTime);
}
return res;
},_overlapLayoutPass3:function(_77){
var pos=0,_78=0;
var res=[];
var _79=_a.position(this.gridTable).x;
for(var col=0;col<this.renderData.columnCount;col++){
var _7a=false;
var _7b=_a.position(this._getCellAt(0,col));
pos=_7b.x-_79;
_78=pos+_7b.w;
for(var _7c=_77.length-1;_7c>=0&&!_7a;_7c--){
for(var i=0;i<_77[_7c].length;i++){
var _7d=_77[_7c][i];
_7a=_7d.start<_78&&pos<_7d.end;
if(_7a){
res[col]=_7c+1;
break;
}
}
}
if(!_7a){
res[col]=0;
}
}
return res;
},applyRendererZIndex:function(_7e,_7f,_80,_81,_82,_83){
_9.set(_7f.container,{"zIndex":_82||_81?_7f.renderer.mobile?100:0:_7e.lane==undefined?1:_7e.lane+1});
},_layoutRenderers:function(_84){
if(_84==null||_84.items==null||_84.rowHeight<=0){
return;
}
if(!this.gridTable||this._expandAnimation!=null||(this.horizontalRenderer==null&&this.labelRenderer==null)){
this._recycleItemRenderers();
return;
}
this.renderData.gridTablePosX=_a.position(this.gridTable).x;
this._layoutStep=_84.columnCount;
this._recycleExpandRenderers();
this._hiddenItems=[];
this._offsets=[];
this.naturalRowsHeight=[];
this.inherited(arguments);
},_offsets:null,_layoutInterval:function(_85,_86,_87,end,_88){
if(this.renderData.cells==null){
return;
}
var _89=[];
var _8a=[];
for(var i=0;i<_88.length;i++){
var _8b=_88[i];
var _8c=this._itemToRendererKind(_8b);
if(_8c=="horizontal"){
_89.push(_8b);
}else{
if(_8c=="label"){
_8a.push(_8b);
}
}
}
var _8d=this.getExpandedRowIndex();
if(_8d!=-1&&_8d!=_86){
return;
}
var _8e;
var _8f=[];
var _90=null;
var _91=[];
if(_89.length>0&&this.horizontalRenderer){
var _90=this._createHorizontalLayoutItems(_86,_87,end,_89);
var _92=this._computeHorizontalOverlapLayout(_90,_91);
}
var _93;
var _94=[];
if(_8a.length>0&&this.labelRenderer){
_93=this._createLabelLayoutItems(_86,_87,end,_8a);
this._computeLabelOffsets(_93,_94);
}
var _95=this._computeColHasHiddenItems(_86,_91,_94);
if(_90!=null){
this._layoutHorizontalItemsImpl(_86,_90,_92,_95,_8f);
}
if(_93!=null){
this._layoutLabelItemsImpl(_86,_93,_95,_8f,_91);
}
this._layoutExpandRenderers(_86,_95,_8f);
this._hiddenItems[_86]=_8f;
},_createHorizontalLayoutItems:function(_96,_97,_98,_99){
if(this.horizontalRenderer==null){
return;
}
var rd=this.renderData;
var cal=rd.dateModule;
var _9a=rd.rtl?-1:1;
var _9b=[];
for(var i=0;i<_99.length;i++){
var _9c=_99[i];
var _9d=this.computeRangeOverlap(rd,_9c.startTime,_9c.endTime,_97,_98);
var _9e=cal.difference(_97,this.floorToDay(_9d[0],false,rd),"day");
var _9f=rd.dates[_96][_9e];
var _a0=_a.position(this._getCellAt(_96,_9e,false));
var _a1=_a0.x-rd.gridTablePosX;
if(rd.rtl){
_a1+=_a0.w;
}
if(!this.roundToDay&&!_9c.allDay){
_a1+=_9a*this.computeProjectionOnDate(rd,_9f,_9d[0],_a0.w);
}
_a1=Math.ceil(_a1);
var _a2=cal.difference(_97,this.floorToDay(_9d[1],false,rd),"day");
var end;
if(_a2>rd.columnCount-1){
_a0=_a.position(this._getCellAt(_96,rd.columnCount-1,false));
if(rd.rtl){
end=_a0.x-rd.gridTablePosX;
}else{
end=_a0.x-rd.gridTablePosX+_a0.w;
}
}else{
_9f=rd.dates[_96][_a2];
_a0=_a.position(this._getCellAt(_96,_a2,false));
end=_a0.x-rd.gridTablePosX;
if(rd.rtl){
end+=_a0.w;
}
if(this.roundToDay){
if(!this.isStartOfDay(_9d[1])){
end+=_9a*_a0.w;
}
}else{
end+=_9a*this.computeProjectionOnDate(rd,_9f,_9d[1],_a0.w);
}
}
end=Math.floor(end);
if(rd.rtl){
var t=end;
end=_a1;
_a1=t;
}
if(end>_a1){
var _a3=_4.mixin({start:_a1,end:end,range:_9d,item:_9c,startOffset:_9e,endOffset:_a2},_9c);
_9b.push(_a3);
}
}
return _9b;
},_computeHorizontalOverlapLayout:function(_a4,_a5){
var rd=this.renderData;
var _a6=this.horizontalRendererHeight;
var _a7=this.computeOverlapping(_a4,this._overlapLayoutPass3);
var _a8=this.percentOverlap/100;
for(var i=0;i<rd.columnCount;i++){
var _a9=_a7.addedPassRes[i];
var _aa=rd.rtl?rd.columnCount-i-1:i;
if(_a8==0){
_a5[_aa]=_a9==0?0:_a9==1?_a6:_a6+(_a9-1)*(_a6+this.verticalGap);
}else{
_a5[_aa]=_a9==0?0:_a9*_a6-(_a9-1)*(_a8*_a6)+this.verticalGap;
}
_a5[_aa]+=this.cellPaddingTop;
}
return _a7;
},_createLabelLayoutItems:function(_ab,_ac,_ad,_ae){
if(this.labelRenderer==null){
return;
}
var d;
var rd=this.renderData;
var cal=rd.dateModule;
var _af=[];
for(var i=0;i<_ae.length;i++){
var _b0=_ae[i];
d=this.floorToDay(_b0.startTime,false,rd);
var _b1=this.dateModule.compare;
while(_b1(d,_b0.endTime)==-1&&_b1(d,_ad)==-1){
var _b2=cal.add(d,"day",1);
_b2=this.floorToDay(_b2,true);
var _b3=this.computeRangeOverlap(rd,_b0.startTime,_b0.endTime,d,_b2);
var _b4=cal.difference(_ac,this.floorToDay(_b3[0],false,rd),"day");
if(_b4>=this.columnCount){
break;
}
if(_b4>=0){
var _b5=_af[_b4];
if(_b5==null){
_b5=[];
_af[_b4]=_b5;
}
_b5.push(_4.mixin({startOffset:_b4,range:_b3,item:_b0},_b0));
}
d=cal.add(d,"day",1);
this.floorToDay(d,true);
}
}
return _af;
},_computeLabelOffsets:function(_b6,_b7){
for(var i=0;i<this.renderData.columnCount;i++){
_b7[i]=_b6[i]==null?0:_b6[i].length*(this.labelRendererHeight+this.verticalGap);
}
},_computeColHasHiddenItems:function(_b8,_b9,_ba){
var res=[];
var _bb=this._getRowHeight(_b8);
var h;
var _bc=0;
for(var i=0;i<this.renderData.columnCount;i++){
h=_b9==null||_b9[i]==null?this.cellPaddingTop:_b9[i];
h+=_ba==null||_ba[i]==null?0:_ba[i];
if(h>_bc){
_bc=h;
}
res[i]=h>_bb;
}
this.naturalRowsHeight[_b8]=_bc;
return res;
},_layoutHorizontalItemsImpl:function(_bd,_be,_bf,_c0,_c1){
var rd=this.renderData;
var _c2=rd.cells[_bd];
var _c3=this._getRowHeight(_bd);
var _c4=this.horizontalRendererHeight;
var _c5=this.percentOverlap/100;
for(var i=0;i<_be.length;i++){
var _c6=_be[i];
var _c7=_c6.lane;
var _c8=this.cellPaddingTop;
if(_c5==0){
_c8+=_c7*(_c4+this.verticalGap);
}else{
_c8+=_c7*(_c4-_c5*_c4);
}
var exp=false;
var _c9=_c3;
if(this.expandRenderer){
for(var off=_c6.startOffset;off<=_c6.endOffset;off++){
if(_c0[off]){
exp=true;
break;
}
}
_c9=exp?_c3-this.expandRendererHeight:_c3;
}
if(_c8+_c4<=_c9){
var ir=this._createRenderer(_c6,"horizontal",this.horizontalRenderer,"dojoxCalendarHorizontal");
var _ca=this.isItemBeingEdited(_c6)&&!this.liveLayout&&this._isEditing;
var h=_ca?_c3-this.cellPaddingTop:_c4;
var w=_c6.end-_c6.start;
if(_5("ie")>=9&&_c6.start+w<this.itemContainer.offsetWidth){
w++;
}
_9.set(ir.container,{"top":(_ca?this.cellPaddingTop:_c8)+"px","left":_c6.start+"px","width":w+"px","height":h+"px"});
this._applyRendererLayout(_c6,ir,_c2,w,h,"horizontal");
}else{
for(var d=_c6.startOffset;d<_c6.endOffset;d++){
if(_c1[d]==null){
_c1[d]=[_c6.item];
}else{
_c1[d].push(_c6.item);
}
}
}
}
},_layoutLabelItemsImpl:function(_cb,_cc,_cd,_ce,_cf){
var _d0,_d1;
var rd=this.renderData;
var _d2=rd.cells[_cb];
var _d3=this._getRowHeight(_cb);
var _d4=this.labelRendererHeight;
var _d5=_a.getMarginBox(this.itemContainer).w;
for(var i=0;i<_cc.length;i++){
_d0=_cc[i];
if(_d0!=null){
var _d6=this.expandRenderer?(_cd[i]?_d3-this.expandRendererHeight:_d3):_d3;
_d1=_cf==null||_cf[i]==null?this.cellPaddingTop:_cf[i]+this.verticalGap;
var _d7=_a.position(this._getCellAt(_cb,i));
var _d8=_d7.x-rd.gridTablePosX;
for(var j=0;j<_d0.length;j++){
if(_d1+_d4+this.verticalGap<=_d6){
var _d9=_d0[j];
_4.mixin(_d9,{start:_d8,end:_d8+_d7.w});
var ir=this._createRenderer(_d9,"label",this.labelRenderer,"dojoxCalendarLabel");
var _da=this.isItemBeingEdited(_d9)&&!this.liveLayout&&this._isEditing;
var h=_da?this._getRowHeight(_cb)-this.cellPaddingTop:_d4;
if(rd.rtl){
_d9.start=_d5-_d9.end;
_d9.end=_d9.start+_d7.w;
}
_9.set(ir.container,{"top":(_da?this.cellPaddingTop:_d1)+"px","left":_d9.start+"px","width":_d7.w+"px","height":h+"px"});
this._applyRendererLayout(_d9,ir,_d2,_d7.w,h,"label");
}else{
break;
}
_d1+=_d4+this.verticalGap;
}
for(var j;j<_d0.length;j++){
if(_ce[i]==null){
_ce[i]=[_d0[j]];
}else{
_ce[i].push(_d0[j]);
}
}
}
}
},_applyRendererLayout:function(_db,ir,_dc,w,h,_dd){
var _de=this.isItemBeingEdited(_db);
var _df=this.isItemSelected(_db);
var _e0=this.isItemHovered(_db);
var _e1=this.isItemFocused(_db);
var _e2=ir.renderer;
_e2.set("hovered",_e0);
_e2.set("selected",_df);
_e2.set("edited",_de);
_e2.set("focused",this.showFocus?_e1:false);
_e2.set("moveEnabled",this.isItemMoveEnabled(_db._item,_dd));
_e2.set("storeState",this.getItemStoreState(_db));
if(_dd!="label"){
_e2.set("resizeEnabled",this.isItemResizeEnabled(_db,_dd));
}
this.applyRendererZIndex(_db,ir,_e0,_df,_de,_e1);
if(_e2.updateRendering){
_e2.updateRendering(w,h);
}
_b.place(ir.container,_dc);
_9.set(ir.container,"display","block");
},_getCellAt:function(_e3,_e4,rtl){
if((rtl==undefined||rtl==true)&&!this.isLeftToRight()){
_e4=this.renderData.columnCount-1-_e4;
}
return this.gridTable.childNodes[0].childNodes[_e3].childNodes[_e4];
},_layoutExpandRenderers:function(_e5,_e6,_e7){
if(!this.expandRenderer){
return;
}
var rd=this.renderData;
if(rd.expandedRow==_e5){
if(rd.expandedRowCol!=null&&rd.expandedRowCol!=-1){
this._layoutExpandRendererImpl(rd.expandedRow,rd.expandedRowCol,null,true);
}
}else{
if(rd.expandedRow==null){
for(var i=0;i<rd.columnCount;i++){
if(_e6[i]){
this._layoutExpandRendererImpl(_e5,rd.rtl?rd.columnCount-1-i:i,_e7[i],false);
}
}
}
}
},_layoutExpandRendererImpl:function(_e8,_e9,_ea,_eb){
var rd=this.renderData;
var d=_4.clone(rd.dates[_e8][_e9]);
var ir=null;
var _ec=rd.cells[_e8];
ir=this._getExpandRenderer(d,_ea,_e8,_e9,_eb);
var dim=_a.position(this._getCellAt(_e8,_e9));
dim.x-=rd.gridTablePosX;
this.layoutExpandRenderer(ir,d,_ea,dim,this.expandRendererHeight);
_b.place(ir.domNode,_ec);
_9.set(ir.domNode,"display","block");
},layoutExpandRenderer:function(_ed,_ee,_ef,_f0,_f1){
_9.set(_ed.domNode,{"left":_f0.x+"px","width":_f0.w+"px","height":_f1+"px","top":(_f0.h-_f1-1)+"px"});
},_onItemEditBeginGesture:function(e){
var p=this._edProps;
var _f2=p.editedItem;
var _f3=e.dates;
var _f4=this.newDate(p.editKind=="resizeEnd"?_f2.endTime:_f2.startTime);
if(p.rendererKind=="label"){
}else{
if(e.editKind=="move"&&(_f2.allDay||this.roundToDay)){
var cal=this.renderData.dateModule;
p.dayOffset=cal.difference(this.floorToDay(_f3[0],false,this.renderData),_f4,"day");
}
}
this.inherited(arguments);
},_computeItemEditingTimes:function(_f5,_f6,_f7,_f8,_f9){
var cal=this.renderData.dateModule;
var p=this._edProps;
if(_f7=="label"){
}else{
if(_f5.allDay||this.roundToDay){
var _fa=this.isStartOfDay(_f8[0]);
switch(_f6){
case "resizeEnd":
if(!_fa&&_f5.allDay){
_f8[0]=cal.add(_f8[0],"day",1);
}
case "resizeStart":
if(!_fa){
_f8[0]=this.floorToDay(_f8[0],true);
}
break;
case "move":
_f8[0]=cal.add(_f8[0],"day",p.dayOffset);
break;
case "resizeBoth":
if(!_fa){
_f8[0]=this.floorToDay(_f8[0],true);
}
if(!this.isStartOfDay(_f8[1])){
_f8[1]=this.floorToDay(cal.add(_f8[1],"day",1),true);
}
break;
}
}else{
_f8=this.inherited(arguments);
}
}
return _f8;
},getTime:function(e,x,y,_fb){
var rd=this.renderData;
if(e!=null){
var _fc=_a.position(this.itemContainer,true);
if(e.touches){
_fb=_fb==undefined?0:_fb;
x=e.touches[_fb].pageX-_fc.x;
y=e.touches[_fb].pageY-_fc.y;
}else{
x=e.pageX-_fc.x;
y=e.pageY-_fc.y;
}
}
var r=_a.getContentBox(this.itemContainer);
if(x<0){
x=0;
}else{
if(x>r.w){
x=r.w-1;
}
}
if(y<0){
y=0;
}else{
if(y>r.h){
y=r.h-1;
}
}
var w=_a.getMarginBox(this.itemContainer).w;
var _fd=w/rd.columnCount;
var row;
if(rd.expandedRow==null){
row=Math.floor(y/(_a.getMarginBox(this.itemContainer).h/rd.rowCount));
}else{
row=rd.expandedRow;
}
var r=_a.getContentBox(this.itemContainer);
if(rd.rtl){
x=r.w-x;
}
var col=Math.floor(x/_fd);
var tm=Math.floor((x-(col*_fd))*1440/_fd);
var _fe=null;
if(row<rd.dates.length&&col<this.renderData.dates[row].length){
_fe=this.newDate(this.renderData.dates[row][col]);
_fe=this.renderData.dateModule.add(_fe,"minute",tm);
}
return _fe;
},_onGridMouseUp:function(e){
this.inherited(arguments);
if(this._gridMouseDown){
this._gridMouseDown=false;
this._onGridClick({date:this.getTime(e),triggerEvent:e});
}
},_onGridTouchEnd:function(e){
this.inherited(arguments);
var g=this._gridProps;
if(g){
if(!this._isEditing){
if(!g.fromItem&&!g.editingOnStart){
this.selectFromEvent(e,null,null,true);
}
if(!g.fromItem){
if(this._pendingDoubleTap&&this._pendingDoubleTap.grid){
this._onGridDoubleClick({date:this.getTime(this._gridProps.event),triggerEvent:this._gridProps.event});
clearTimeout(this._pendingDoubleTap.timer);
delete this._pendingDoubleTap;
}else{
this._onGridClick({date:this.getTime(this._gridProps.event),triggerEvent:this._gridProps.event});
this._pendingDoubleTap={grid:true,timer:setTimeout(_4.hitch(this,function(){
delete this._pendingDoubleTap;
}),this.doubleTapDelay)};
}
}
}
this._gridProps=null;
}
},_onRowHeaderClick:function(e){
this._dispatchCalendarEvt(e,"onRowHeaderClick");
},onRowHeaderClick:function(e){
},expandRendererClickHandler:function(e,_ff){
_3.stop(e);
var ri=_ff.get("rowIndex");
var ci=_ff.get("columnIndex");
this._onExpandRendererClick(_4.mixin(this._createItemEditEvent(),{rowIndex:ri,columnIndex:ci,renderer:_ff,triggerEvent:e,date:this.renderData.dates[ri][ci]}));
},onExpandRendererClick:function(e){
},_onExpandRendererClick:function(e){
this._dispatchCalendarEvt(e,"onExpandRendererClick");
if(!e.isDefaultPrevented()){
if(this.getExpandedRowIndex()!=-1){
this.collapseRow();
}else{
this.expandRow(e.rowIndex,e.columnIndex);
}
}
},snapUnit:"minute",snapSteps:15,minDurationUnit:"minute",minDurationSteps:15,triggerExtent:3,liveLayout:false,stayInView:true,allowStartEndSwap:true,allowResizeLessThan24H:false});
});
