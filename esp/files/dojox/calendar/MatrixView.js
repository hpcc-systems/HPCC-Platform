//>>built
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
if(this.displayedItemsInvalidated){
this.displayedItemsInvalidated=false;
this._computeVisibleItems(rd);
if(this._isEditing){
this._endItemEditing(null,false);
}
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
_19.columnCount=0;
_19.rowCount=0;
return;
}
this._buildColumnHeader(_19,_1a);
this._buildRowHeader(_19,_1a);
this._buildGrid(_19,_1a);
this._buildItemContainer(_19,_1a);
if(this.buttonContainer&&this.owner!=null&&this.owner.currentView==this){
_9.set(this.buttonContainer,{"right":0,"left":0});
}
},_buildColumnHeader:function(_1b,_1c){
var _1d=this.columnHeaderTable;
if(!_1d){
return;
}
var _1e=_1b.columnCount-(_1c?_1c.columnCount:0);
if(_5("ie")==8){
if(this._colTableSave==null){
this._colTableSave=_4.clone(_1d);
}else{
if(_1e<0){
this.columnHeader.removeChild(_1d);
_b.destroy(_1d);
_1d=_4.clone(this._colTableSave);
this.columnHeaderTable=_1d;
this.columnHeader.appendChild(_1d);
_1e=_1b.columnCount;
}
}
}
var _1f=_c("tbody",_1d);
var trs=_c("tr",_1d);
var _20,tr,td;
if(_1f.length==1){
_20=_1f[0];
}else{
_20=_6.create("tbody",null,_1d);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_b.create("tr",null,_20);
}
if(_1e>0){
for(var i=0;i<_1e;i++){
td=_b.create("td",null,tr);
}
}else{
_1e=-_1e;
for(var i=0;i<_1e;i++){
tr.removeChild(tr.lastChild);
}
}
_c("td",_1d).forEach(function(td,i){
td.className="";
var d=_1b.dates[0][i];
this._setText(td,this._formatColumnHeaderLabel(d));
if(i==0){
_8.add(td,"first-child");
}else{
if(i==this.renderData.columnCount-1){
_8.add(td,"last-child");
}
}
this.styleColumnHeaderCell(td,d,_1b);
},this);
if(this.yearColumnHeaderContent){
var d=_1b.dates[0][0];
this._setText(this.yearColumnHeaderContent,_1b.dateLocaleModule.format(d,{selector:"date",datePattern:"yyyy"}));
}
},styleColumnHeaderCell:function(_21,_22,_23){
_8.add(_21,this._cssDays[_22.getDay()]);
if(this.isWeekEnd(_22)){
_8.add(_21,"dojoxCalendarWeekend");
}
},_rowHeaderHandles:null,_cleanupRowHeader:function(){
while(this._rowHeaderHandles.length>0){
var _24=this._rowHeaderHandles.pop();
while(_24.length>0){
_24.pop().remove();
}
}
},_rowHeaderClick:function(e){
var _25=_c("td",this.rowHeaderTable).indexOf(e.currentTarget);
this._onRowHeaderClick({index:_25,date:this.renderData.dates[_25][0],triggerEvent:e});
},_buildRowHeader:function(_26,_27){
var _28=this.rowHeaderTable;
if(!_28){
return;
}
var _29=_c("tbody",_28);
var _2a,tr,td;
if(_29.length==1){
_2a=_29[0];
}else{
_2a=_b.create("tbody",null,_28);
}
var _2b=_26.rowCount-_c("tr",_28).length;
if(_2b>0){
for(var i=0;i<_2b;i++){
tr=_b.create("tr",null,_2a);
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
_2b=-_2b;
for(var i=0;i<_2b;i++){
_2a.removeChild(_2a.lastChild);
var _2c=this._rowHeaderHandles.pop();
while(_2c.length>0){
_2c.pop().remove();
}
}
}
_c("tr",_28).forEach(function(tr,i){
_9.set(tr,"height",this._getRowHeight(i)+"px");
var d=_26.dates[i][0];
var td=_c("td",tr)[0];
td.className="";
if(i==0){
_8.add(td,"first-child");
}
if(i==this.renderData.rowCount-1){
_8.add(td,"last-child");
}
this.styleRowHeaderCell(td,d,_26);
this._setText(td,this._formatRowHeaderLabel(d));
},this);
},styleRowHeaderCell:function(_2d,_2e,_2f){
},_buildGrid:function(_30,_31){
var _32=this.gridTable;
if(!_32){
return;
}
var _33=_c("tr",_32);
var _34=_30.rowCount-_33.length;
var _35=_34>0;
var _36=_30.columnCount-(_33?_c("td",_33[0]).length:0);
if(_5("ie")==8){
if(this._gridTableSave==null){
this._gridTableSave=_4.clone(_32);
}else{
if(_36<0){
this.grid.removeChild(_32);
_b.destroy(_32);
_32=_4.clone(this._gridTableSave);
this.gridTable=_32;
this.grid.appendChild(_32);
_36=_30.columnCount;
_34=_30.rowCount;
_35=true;
}
}
}
var _37=_c("tbody",_32);
var _38;
if(_37.length==1){
_38=_37[0];
}else{
_38=_b.create("tbody",null,_32);
}
if(_35){
for(var i=0;i<_34;i++){
_b.create("tr",null,_38);
}
}else{
_34=-_34;
for(var i=0;i<_34;i++){
_38.removeChild(_38.lastChild);
}
}
var _39=_30.rowCount-_34;
var _3a=_35||_36>0;
_36=_3a?_36:-_36;
_c("tr",_32).forEach(function(tr,i){
if(_3a){
var len=i>=_39?_30.columnCount:_36;
for(var i=0;i<len;i++){
var td=_b.create("td",null,tr);
_b.create("span",null,td);
}
}else{
for(var i=0;i<_36;i++){
tr.removeChild(tr.lastChild);
}
}
});
_c("tr",_32).forEach(function(tr,row){
_9.set(tr,"height",this._getRowHeight(row)+"px");
tr.className="";
if(row==0){
_8.add(tr,"first-child");
}
if(row==_30.rowCount-1){
_8.add(tr,"last-child");
}
_c("td",tr).forEach(function(td,col){
td.className="";
if(col==0){
_8.add(td,"first-child");
}
if(col==_30.columnCount-1){
_8.add(td,"last-child");
}
var d=_30.dates[row][col];
var _3b=_c("span",td)[0];
this._setText(_3b,this.showCellLabel?this._formatGridCellLabel(d,row,col):null);
this.styleGridCell(td,d,_30);
},this);
},this);
},styleGridCellFunc:null,defaultStyleGridCell:function(_3c,_3d,_3e){
_8.add(_3c,this._cssDays[_3d.getDay()]);
var cal=this.dateModule;
if(this.isToday(_3d)){
_8.add(_3c,"dojoxCalendarToday");
}else{
if(this.refStartTime!=null&&this.refEndTime!=null&&(cal.compare(_3d,this.refEndTime)>=0||cal.compare(cal.add(_3d,"day",1),this.refStartTime)<=0)){
_8.add(_3c,"dojoxCalendarDayDisabled");
}else{
if(this.isWeekEnd(_3d)){
_8.add(_3c,"dojoxCalendarWeekend");
}
}
}
},styleGridCell:function(_3f,_40,_41){
if(this.styleGridCellFunc){
this.styleGridCellFunc(_3f,_40,_41);
}else{
this.defaultStyleGridCell(_3f,_40,_41);
}
},_buildItemContainer:function(_42,_43){
var _44=this.itemContainerTable;
if(!_44){
return;
}
var _45=[];
var _46=_42.rowCount-(_43?_43.rowCount:0);
if(_5("ie")==8){
if(this._itemTableSave==null){
this._itemTableSave=_4.clone(_44);
}else{
if(_46<0){
this.itemContainer.removeChild(_44);
this._recycleItemRenderers(true);
this._recycleExpandRenderers(true);
_b.destroy(_44);
_44=_4.clone(this._itemTableSave);
this.itemContainerTable=_44;
this.itemContainer.appendChild(_44);
_46=_42.columnCount;
}
}
}
var _47=_c("tbody",_44);
var _48,tr,td,div;
if(_47.length==1){
_48=_47[0];
}else{
_48=_b.create("tbody",null,_44);
}
if(_46>0){
for(var i=0;i<_46;i++){
tr=_b.create("tr",null,_48);
_8.add(tr,"dojoxCalendarItemContainerRow");
td=_b.create("td",null,tr);
div=_b.create("div",null,td);
_8.add(div,"dojoxCalendarContainerRow");
}
}else{
_46=-_46;
for(var i=0;i<_46;i++){
_48.removeChild(_48.lastChild);
}
}
_c(".dojoxCalendarItemContainerRow",_44).forEach(function(tr,i){
_9.set(tr,"height",this._getRowHeight(i)+"px");
_45.push(tr.childNodes[0].childNodes[0]);
},this);
_42.cells=_45;
},resize:function(e){
this._resizeHandler(e);
},_resizeHandler:function(e,_49){
var rd=this.renderData;
if(rd==null){
this.refreshRendering();
return;
}
if(rd.sheetHeight!=this.itemContainer.offsetHeight){
rd.sheetHeight=this.itemContainer.offsetHeight;
var _4a=this.getExpandedRowIndex();
if(_4a==-1){
this._computeRowsHeight();
this._resizeRows();
}else{
this.expandRow(rd.expandedRow,rd.expandedRowCol,0,null,true);
}
}
if(this.layoutDuringResize||_49){
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
},collapseRow:function(_4b,_4c,_4d){
var rd=this.renderData;
if(_4d==undefined){
_4d=true;
}
if(_4b==undefined){
_4b=this.expandDuration;
}
if(rd&&rd.expandedRow!=null&&rd.expandedRow!=-1){
if(_4d&&_4b){
var _4e=rd.expandedRow;
var _4f=rd.expandedRowHeight;
delete rd.expandedRow;
this._computeRowsHeight(rd);
var _50=this._getRowHeight(_4e);
rd.expandedRow=_4e;
this._recycleExpandRenderers();
this._recycleItemRenderers();
_9.set(this.itemContainer,"display","none");
this._expandAnimation=new fx.Animation({curve:[_4f,_50],duration:_4b,easing:_4c,onAnimate:_4.hitch(this,function(_51){
this._expandRowImpl(Math.floor(_51));
}),onEnd:_4.hitch(this,function(_52){
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
this._collapseRowImpl(_4d);
}
}
},_collapseRowImpl:function(_53){
var rd=this.renderData;
delete rd.expandedRow;
delete rd.expandedRowHeight;
this._computeRowsHeight(rd);
if(_53==undefined||_53){
this._resizeRows();
this._layoutRenderers(rd);
}
},expandRow:function(_54,_55,_56,_57,_58){
var rd=this.renderData;
if(!rd||_54<0||_54>=rd.rowCount){
return -1;
}
if(_55==undefined||_55<0||_55>=rd.columnCount){
_55=-1;
}
if(_58==undefined){
_58=true;
}
if(_56==undefined){
_56=this.expandDuration;
}
if(_57==undefined){
_57=this.expandEasing;
}
var _59=this._getRowHeight(_54);
var _5a=rd.sheetHeight-Math.ceil(this.cellPaddingTop*(rd.rowCount-1));
rd.expandedRow=_54;
rd.expandedRowCol=_55;
rd.expandedRowHeight=_5a;
if(_58){
if(_56){
this._recycleExpandRenderers();
this._recycleItemRenderers();
_9.set(this.itemContainer,"display","none");
this._expandAnimation=new fx.Animation({curve:[_59,_5a],duration:_56,delay:50,easing:_57,onAnimate:_4.hitch(this,function(_5b){
this._expandRowImpl(Math.floor(_5b));
}),onEnd:_4.hitch(this,function(){
this._expandAnimation=null;
_9.set(this.itemContainer,"display","block");
setTimeout(_4.hitch(this,function(){
this._expandRowImpl(_5a,true);
}),100);
this.onExpandAnimationEnd(true);
})});
this._expandAnimation.play();
}else{
this._expandRowImpl(_5a);
}
}
},_expandRowImpl:function(_5c,_5d){
var rd=this.renderData;
rd.expandedRowHeight=_5c;
this._computeRowsHeight(rd,rd.sheetHeight-_5c);
this._resizeRows();
if(_5d){
this._layoutRenderers(rd);
}
},onExpandAnimationEnd:function(_5e){
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
},_computeRowsHeight:function(_5f,max){
var rd=_5f==null?this.renderData:_5f;
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
var _60=rd.expandedRow==null?rd.rowCount:rd.rowCount-1;
var rhx=max/_60;
var rhf,rhl,rh;
var _61=max-(Math.floor(rhx)*_60);
var _62=Math.abs(max-(Math.ceil(rhx)*_60));
var _63;
var _64=1;
if(_61<_62){
rh=Math.floor(rhx);
_63=_61;
}else{
_64=-1;
rh=Math.ceil(rhx);
_63=_62;
}
rhf=rh+_64*Math.floor(_63/2);
rhl=rhf+_64*(_63%2);
rd.rowHeight=rh;
rd.rowHeightFirst=rhf;
rd.rowHeightLast=rhl;
},_getRowHeight:function(_65){
var rd=this.renderData;
if(_65==rd.expandedRow){
return rd.expandedRowHeight;
}else{
if(rd.expandedRow==0&&_65==1||_65==0){
return rd.rowHeightFirst;
}else{
if(rd.expandedRow==this.renderData.rowCount-1&&_65==this.renderData.rowCount-2||_65==this.renderData.rowCount-1){
return rd.rowHeightLast;
}else{
return rd.rowHeight;
}
}
}
},_resizeRowsImpl:function(_66,_67){
dojo.query(_67,_66).forEach(function(tr,i){
_9.set(tr,"height",this._getRowHeight(i)+"px");
},this);
},_setHorizontalRendererAttr:function(_68){
this._destroyRenderersByKind("horizontal");
this._set("horizontalRenderer",_68);
},_setLabelRendererAttr:function(_69){
this._destroyRenderersByKind("label");
this._set("labelRenderer",_69);
},_destroyExpandRenderer:function(_6a){
if(_6a["destroyRecursive"]){
_6a.destroyRecursive();
}
_6.destroy(_6a.domNode);
},_setExpandRendererAttr:function(_6b){
while(this._ddRendererList.length>0){
this._destroyExpandRenderer(this._ddRendererList.pop());
}
var _6c=this._ddRendererPool;
if(_6c){
while(_6c.length>0){
this._destroyExpandRenderer(_6c.pop());
}
}
this._set("expandRenderer",_6b);
},_ddRendererList:null,_ddRendererPool:null,_getExpandRenderer:function(_6d,_6e,_6f,_70,_71){
if(this.expandRenderer==null){
return null;
}
var ir=this._ddRendererPool.pop();
if(ir==null){
ir=new this.expandRenderer();
}
this._ddRendererList.push(ir);
ir.set("owner",this);
ir.set("date",_6d);
ir.set("items",_6e);
ir.set("rowIndex",_6f);
ir.set("columnIndex",_70);
ir.set("expanded",_71);
return ir;
},_recycleExpandRenderers:function(_72){
for(var i=0;i<this._ddRendererList.length;i++){
var ir=this._ddRendererList[i];
ir.set("Up",false);
ir.set("Down",false);
if(_72){
ir.domNode.parentNode.removeChild(ir.domNode);
}
_9.set(ir.domNode,"display","none");
}
this._ddRendererPool=this._ddRendererPool.concat(this._ddRendererList);
this._ddRendererList=[];
},_defaultItemToRendererKindFunc:function(_73){
var dur=Math.abs(this.renderData.dateModule.difference(_73.startTime,_73.endTime,"minute"));
return dur>=1440?"horizontal":"label";
},naturalRowsHeight:null,_roundItemToDay:function(_74){
var s=_74.startTime,e=_74.endTime;
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
},_overlapLayoutPass3:function(_75){
var pos=0,_76=0;
var res=[];
var _77=_a.position(this.gridTable).x;
for(var col=0;col<this.renderData.columnCount;col++){
var _78=false;
var _79=_a.position(this._getCellAt(0,col));
pos=_79.x-_77;
_76=pos+_79.w;
for(var _7a=_75.length-1;_7a>=0&&!_78;_7a--){
for(var i=0;i<_75[_7a].length;i++){
var _7b=_75[_7a][i];
_78=_7b.start<_76&&pos<_7b.end;
if(_78){
res[col]=_7a+1;
break;
}
}
}
if(!_78){
res[col]=0;
}
}
return res;
},applyRendererZIndex:function(_7c,_7d,_7e,_7f,_80,_81){
_9.set(_7d.container,{"zIndex":_80||_7f?_7d.renderer.mobile?100:0:_7c.lane==undefined?1:_7c.lane+1});
},_layoutRenderers:function(_82){
if(_82==null||_82.items==null||_82.rowHeight<=0){
return;
}
if(!this.gridTable||this._expandAnimation!=null||(this.horizontalRenderer==null&&this.labelRenderer==null)){
this._recycleItemRenderers();
return;
}
this.renderData.gridTablePosX=_a.position(this.gridTable).x;
this._layoutStep=_82.columnCount;
this._recycleExpandRenderers();
this._hiddenItems=[];
this._offsets=[];
this.naturalRowsHeight=[];
this.inherited(arguments);
},_offsets:null,_layoutInterval:function(_83,_84,_85,end,_86){
if(this.renderData.cells==null){
return;
}
var _87=[];
var _88=[];
for(var i=0;i<_86.length;i++){
var _89=_86[i];
var _8a=this._itemToRendererKind(_89);
if(_8a=="horizontal"){
_87.push(_89);
}else{
if(_8a=="label"){
_88.push(_89);
}
}
}
var _8b=this.getExpandedRowIndex();
if(_8b!=-1&&_8b!=_84){
return;
}
var _8c;
var _8d=[];
var _8e=null;
var _8f=[];
if(_87.length>0&&this.horizontalRenderer){
var _8e=this._createHorizontalLayoutItems(_84,_85,end,_87);
var _90=this._computeHorizontalOverlapLayout(_8e,_8f);
}
var _91;
var _92=[];
if(_88.length>0&&this.labelRenderer){
_91=this._createLabelLayoutItems(_84,_85,end,_88);
this._computeLabelOffsets(_91,_92);
}
var _93=this._computeColHasHiddenItems(_84,_8f,_92);
if(_8e!=null){
this._layoutHorizontalItemsImpl(_84,_8e,_90,_93,_8d);
}
if(_91!=null){
this._layoutLabelItemsImpl(_84,_91,_93,_8d,_8f);
}
this._layoutExpandRenderers(_84,_93,_8d);
this._hiddenItems[_84]=_8d;
},_createHorizontalLayoutItems:function(_94,_95,_96,_97){
if(this.horizontalRenderer==null){
return;
}
var rd=this.renderData;
var cal=rd.dateModule;
var _98=rd.rtl?-1:1;
var _99=[];
for(var i=0;i<_97.length;i++){
var _9a=_97[i];
var _9b=this.computeRangeOverlap(rd,_9a.startTime,_9a.endTime,_95,_96);
var _9c=cal.difference(_95,this.floorToDay(_9b[0],false,rd),"day");
var _9d=rd.dates[_94][_9c];
var _9e=_a.position(this._getCellAt(_94,_9c,false));
var _9f=_9e.x-rd.gridTablePosX;
if(rd.rtl){
_9f+=_9e.w;
}
if(!this.roundToDay&&!_9a.allDay){
_9f+=_98*this.computeProjectionOnDate(rd,_9d,_9b[0],_9e.w);
}
_9f=Math.ceil(_9f);
var _a0=cal.difference(_95,this.floorToDay(_9b[1],false,rd),"day");
var end;
if(_a0>rd.columnCount-1){
_9e=_a.position(this._getCellAt(_94,rd.columnCount-1,false));
if(rd.rtl){
end=_9e.x-rd.gridTablePosX;
}else{
end=_9e.x-rd.gridTablePosX+_9e.w;
}
}else{
_9d=rd.dates[_94][_a0];
_9e=_a.position(this._getCellAt(_94,_a0,false));
end=_9e.x-rd.gridTablePosX;
if(rd.rtl){
end+=_9e.w;
}
if(this.roundToDay){
if(!this.isStartOfDay(_9b[1])){
end+=_98*_9e.w;
}
}else{
end+=_98*this.computeProjectionOnDate(rd,_9d,_9b[1],_9e.w);
}
}
end=Math.floor(end);
if(rd.rtl){
var t=end;
end=_9f;
_9f=t;
}
if(end>_9f){
var _a1=_4.mixin({start:_9f,end:end,range:_9b,item:_9a,startOffset:_9c,endOffset:_a0},_9a);
_99.push(_a1);
}
}
return _99;
},_computeHorizontalOverlapLayout:function(_a2,_a3){
var rd=this.renderData;
var _a4=this.horizontalRendererHeight;
var _a5=this.computeOverlapping(_a2,this._overlapLayoutPass3);
var _a6=this.percentOverlap/100;
for(var i=0;i<rd.columnCount;i++){
var _a7=_a5.addedPassRes[i];
var _a8=rd.rtl?rd.columnCount-i-1:i;
if(_a6==0){
_a3[_a8]=_a7==0?0:_a7==1?_a4:_a4+(_a7-1)*(_a4+this.verticalGap);
}else{
_a3[_a8]=_a7==0?0:_a7*_a4-(_a7-1)*(_a6*_a4)+this.verticalGap;
}
_a3[_a8]+=this.cellPaddingTop;
}
return _a5;
},_createLabelLayoutItems:function(_a9,_aa,_ab,_ac){
if(this.labelRenderer==null){
return;
}
var d;
var rd=this.renderData;
var cal=rd.dateModule;
var _ad=[];
for(var i=0;i<_ac.length;i++){
var _ae=_ac[i];
d=this.floorToDay(_ae.startTime,false,rd);
var _af=this.dateModule.compare;
while(_af(d,_ae.endTime)==-1&&_af(d,_ab)==-1){
var _b0=cal.add(d,"day",1);
_b0=this.floorToDay(_b0,true);
var _b1=this.computeRangeOverlap(rd,_ae.startTime,_ae.endTime,d,_b0);
var _b2=cal.difference(_aa,this.floorToDay(_b1[0],false,rd),"day");
if(_b2>=this.columnCount){
break;
}
if(_b2>=0){
var _b3=_ad[_b2];
if(_b3==null){
_b3=[];
_ad[_b2]=_b3;
}
_b3.push(_4.mixin({startOffset:_b2,range:_b1,item:_ae},_ae));
}
d=cal.add(d,"day",1);
this.floorToDay(d,true);
}
}
return _ad;
},_computeLabelOffsets:function(_b4,_b5){
for(var i=0;i<this.renderData.columnCount;i++){
_b5[i]=_b4[i]==null?0:_b4[i].length*(this.labelRendererHeight+this.verticalGap);
}
},_computeColHasHiddenItems:function(_b6,_b7,_b8){
var res=[];
var _b9=this._getRowHeight(_b6);
var h;
var _ba=0;
for(var i=0;i<this.renderData.columnCount;i++){
h=_b7==null||_b7[i]==null?this.cellPaddingTop:_b7[i];
h+=_b8==null||_b8[i]==null?0:_b8[i];
if(h>_ba){
_ba=h;
}
res[i]=h>_b9;
}
this.naturalRowsHeight[_b6]=_ba;
return res;
},_layoutHorizontalItemsImpl:function(_bb,_bc,_bd,_be,_bf){
var rd=this.renderData;
var _c0=rd.cells[_bb];
var _c1=this._getRowHeight(_bb);
var _c2=this.horizontalRendererHeight;
var _c3=this.percentOverlap/100;
for(var i=0;i<_bc.length;i++){
var _c4=_bc[i];
var _c5=_c4.lane;
var _c6=this.cellPaddingTop;
if(_c3==0){
_c6+=_c5*(_c2+this.verticalGap);
}else{
_c6+=_c5*(_c2-_c3*_c2);
}
var exp=false;
var _c7=_c1;
if(this.expandRenderer){
for(var off=_c4.startOffset;off<=_c4.endOffset;off++){
if(_be[off]){
exp=true;
break;
}
}
_c7=exp?_c1-this.expandRendererHeight:_c1;
}
if(_c6+_c2<=_c7){
var ir=this._createRenderer(_c4,"horizontal",this.horizontalRenderer,"dojoxCalendarHorizontal");
var _c8=this.isItemBeingEdited(_c4)&&!this.liveLayout&&this._isEditing;
var h=_c8?_c1-this.cellPaddingTop:_c2;
var w=_c4.end-_c4.start;
if(_5("ie")>=9&&_c4.start+w<this.itemContainer.offsetWidth){
w++;
}
_9.set(ir.container,{"top":(_c8?this.cellPaddingTop:_c6)+"px","left":_c4.start+"px","width":w+"px","height":h+"px"});
this._applyRendererLayout(_c4,ir,_c0,w,h,"horizontal");
}else{
for(var d=_c4.startOffset;d<_c4.endOffset;d++){
if(_bf[d]==null){
_bf[d]=[_c4.item];
}else{
_bf[d].push(_c4.item);
}
}
}
}
},_layoutLabelItemsImpl:function(_c9,_ca,_cb,_cc,_cd){
var _ce,_cf;
var rd=this.renderData;
var _d0=rd.cells[_c9];
var _d1=this._getRowHeight(_c9);
var _d2=this.labelRendererHeight;
var _d3=_a.getMarginBox(this.itemContainer).w;
for(var i=0;i<_ca.length;i++){
_ce=_ca[i];
if(_ce!=null){
var _d4=this.expandRenderer?(_cb[i]?_d1-this.expandRendererHeight:_d1):_d1;
_cf=_cd==null||_cd[i]==null?this.cellPaddingTop:_cd[i]+this.verticalGap;
var _d5=_a.position(this._getCellAt(_c9,i));
var _d6=_d5.x-rd.gridTablePosX;
for(var j=0;j<_ce.length;j++){
if(_cf+_d2+this.verticalGap<=_d4){
var _d7=_ce[j];
_4.mixin(_d7,{start:_d6,end:_d6+_d5.w});
var ir=this._createRenderer(_d7,"label",this.labelRenderer,"dojoxCalendarLabel");
var _d8=this.isItemBeingEdited(_d7)&&!this.liveLayout&&this._isEditing;
var h=_d8?this._getRowHeight(_c9)-this.cellPaddingTop:_d2;
if(rd.rtl){
_d7.start=_d3-_d7.end;
_d7.end=_d7.start+_d5.w;
}
_9.set(ir.container,{"top":(_d8?this.cellPaddingTop:_cf)+"px","left":_d7.start+"px","width":_d5.w+"px","height":h+"px"});
this._applyRendererLayout(_d7,ir,_d0,_d5.w,h,"label");
}else{
break;
}
_cf+=_d2+this.verticalGap;
}
for(var j;j<_ce.length;j++){
if(_cc[i]==null){
_cc[i]=[_ce[j]];
}else{
_cc[i].push(_ce[j]);
}
}
}
}
},_applyRendererLayout:function(_d9,ir,_da,w,h,_db){
var _dc=this.isItemBeingEdited(_d9);
var _dd=this.isItemSelected(_d9);
var _de=this.isItemHovered(_d9);
var _df=this.isItemFocused(_d9);
var _e0=ir.renderer;
_e0.set("hovered",_de);
_e0.set("selected",_dd);
_e0.set("edited",_dc);
_e0.set("focused",this.showFocus?_df:false);
_e0.set("moveEnabled",this.isItemMoveEnabled(_d9._item,_db));
_e0.set("storeState",this.getItemStoreState(_d9));
if(_db!="label"){
_e0.set("resizeEnabled",this.isItemResizeEnabled(_d9,_db));
}
this.applyRendererZIndex(_d9,ir,_de,_dd,_dc,_df);
if(_e0.updateRendering){
_e0.updateRendering(w,h);
}
_b.place(ir.container,_da);
_9.set(ir.container,"display","block");
},_getCellAt:function(_e1,_e2,rtl){
if((rtl==undefined||rtl==true)&&!this.isLeftToRight()){
_e2=this.renderData.columnCount-1-_e2;
}
return this.gridTable.childNodes[0].childNodes[_e1].childNodes[_e2];
},_layoutExpandRenderers:function(_e3,_e4,_e5){
if(!this.expandRenderer){
return;
}
var rd=this.renderData;
if(rd.expandedRow==_e3){
if(rd.expandedRowCol!=null&&rd.expandedRowCol!=-1){
this._layoutExpandRendererImpl(rd.expandedRow,rd.expandedRowCol,null,true);
}
}else{
if(rd.expandedRow==null){
for(var i=0;i<rd.columnCount;i++){
if(_e4[i]){
this._layoutExpandRendererImpl(_e3,rd.rtl?rd.columnCount-1-i:i,_e5[i],false);
}
}
}
}
},_layoutExpandRendererImpl:function(_e6,_e7,_e8,_e9){
var rd=this.renderData;
var d=_4.clone(rd.dates[_e6][_e7]);
var ir=null;
var _ea=rd.cells[_e6];
ir=this._getExpandRenderer(d,_e8,_e6,_e7,_e9);
var dim=_a.position(this._getCellAt(_e6,_e7));
dim.x-=rd.gridTablePosX;
this.layoutExpandRenderer(ir,d,_e8,dim,this.expandRendererHeight);
_b.place(ir.domNode,_ea);
_9.set(ir.domNode,"display","block");
},layoutExpandRenderer:function(_eb,_ec,_ed,_ee,_ef){
_9.set(_eb.domNode,{"left":_ee.x+"px","width":_ee.w+"px","height":_ef+"px","top":(_ee.h-_ef-1)+"px"});
},_onItemEditBeginGesture:function(e){
var p=this._edProps;
var _f0=p.editedItem;
var _f1=e.dates;
var _f2=this.newDate(p.editKind=="resizeEnd"?_f0.endTime:_f0.startTime);
if(p.rendererKind=="label"){
}else{
if(e.editKind=="move"&&(_f0.allDay||this.roundToDay)){
var cal=this.renderData.dateModule;
p.dayOffset=cal.difference(this.floorToDay(_f1[0],false,this.renderData),_f2,"day");
}
}
this.inherited(arguments);
},_computeItemEditingTimes:function(_f3,_f4,_f5,_f6,_f7){
var cal=this.renderData.dateModule;
var p=this._edProps;
if(_f5=="label"){
}else{
if(_f3.allDay||this.roundToDay){
var _f8=this.isStartOfDay(_f6[0]);
switch(_f4){
case "resizeEnd":
if(!_f8&&_f3.allDay){
_f6[0]=cal.add(_f6[0],"day",1);
}
case "resizeStart":
if(!_f8){
_f6[0]=this.floorToDay(_f6[0],true);
}
break;
case "move":
_f6[0]=cal.add(_f6[0],"day",p.dayOffset);
break;
case "resizeBoth":
if(!_f8){
_f6[0]=this.floorToDay(_f6[0],true);
}
if(!this.isStartOfDay(_f6[1])){
_f6[1]=this.floorToDay(cal.add(_f6[1],"day",1),true);
}
break;
}
}else{
_f6=this.inherited(arguments);
}
}
return _f6;
},getTime:function(e,x,y,_f9){
var rd=this.renderData;
if(e!=null){
var _fa=_a.position(this.itemContainer,true);
if(e.touches){
_f9=_f9==undefined?0:_f9;
x=e.touches[_f9].pageX-_fa.x;
y=e.touches[_f9].pageY-_fa.y;
}else{
x=e.pageX-_fa.x;
y=e.pageY-_fa.y;
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
var _fb=w/rd.columnCount;
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
var col=Math.floor(x/_fb);
var tm=Math.floor((x-(col*_fb))*1440/_fb);
var _fc=null;
if(row<rd.dates.length&&col<this.renderData.dates[row].length){
_fc=this.newDate(this.renderData.dates[row][col]);
_fc=this.renderData.dateModule.add(_fc,"minute",tm);
}
return _fc;
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
},expandRendererClickHandler:function(e,_fd){
_3.stop(e);
var ri=_fd.get("rowIndex");
var ci=_fd.get("columnIndex");
this._onExpandRendererClick(_4.mixin(this._createItemEditEvent(),{rowIndex:ri,columnIndex:ci,renderer:_fd,triggerEvent:e,date:this.renderData.dates[ri][ci]}));
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
require({cache:{"url:dojox/calendar/templates/MatrixView.html":"<div data-dojo-attach-events=\"keydown:_onKeyDown\">\n\t<div  class=\"dojoxCalendarYearColumnHeader\" data-dojo-attach-point=\"yearColumnHeader\">\n\t\t<table><tr><td><span data-dojo-attach-point=\"yearColumnHeaderContent\"></span></td></tr></table>\t\t\n\t</div>\t\n\t<div data-dojo-attach-point=\"columnHeader\" class=\"dojoxCalendarColumnHeader\">\n\t\t<table data-dojo-attach-point=\"columnHeaderTable\" class=\"dojoxCalendarColumnHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\t\n\t<div dojoAttachPoint=\"rowHeader\" class=\"dojoxCalendarRowHeader\">\n\t\t<table data-dojo-attach-point=\"rowHeaderTable\" class=\"dojoxCalendarRowHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\n\t<div dojoAttachPoint=\"grid\" class=\"dojoxCalendarGrid\">\n\t\t<table data-dojo-attach-point=\"gridTable\" class=\"dojoxCalendarGridTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\n\t<div data-dojo-attach-point=\"itemContainer\" class=\"dojoxCalendarContainer\" data-dojo-attach-event=\"mousedown:_onGridMouseDown,mouseup:_onGridMouseUp,ondblclick:_onGridDoubleClick,touchstart:_onGridTouchStart,touchmove:_onGridTouchMove,touchend:_onGridTouchEnd\">\n\t\t<table data-dojo-attach-point=\"itemContainerTable\" class=\"dojoxCalendarContainerTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t</div>\t\n</div>\n"}});
