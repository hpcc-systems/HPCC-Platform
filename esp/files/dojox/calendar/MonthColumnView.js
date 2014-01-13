//>>built
require({cache:{"url:dojox/calendar/templates/MonthColumnView.html":"<div data-dojo-attach-events=\"keydown:_onKeyDown\">\t\t\n\t<div data-dojo-attach-point=\"columnHeader\" class=\"dojoxCalendarColumnHeader\">\n\t\t<table data-dojo-attach-point=\"columnHeaderTable\" class=\"dojoxCalendarColumnHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t</div>\t\n\t<div data-dojo-attach-point=\"vScrollBar\" class=\"dojoxCalendarVScrollBar\">\n\t\t<div data-dojo-attach-point=\"vScrollBarContent\" style=\"visibility:hidden;position:relative; width:1px; height:1px;\" ></div>\n\t</div>\t\n\t<div data-dojo-attach-point=\"scrollContainer\" class=\"dojoxCalendarScrollContainer\">\n\t\t<div data-dojo-attach-point=\"sheetContainer\" style=\"position:relative;left:0;right:0;margin:0;padding:0\">\t\t\t\n\t\t\t<div data-dojo-attach-point=\"grid\" class=\"dojoxCalendarGrid\">\n\t\t\t\t<table data-dojo-attach-point=\"gridTable\" class=\"dojoxCalendarGridTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t\t\t</div>\n\t\t\t<div data-dojo-attach-point=\"itemContainer\" class=\"dojoxCalendarContainer\" data-dojo-attach-event=\"mousedown:_onGridMouseDown,mouseup:_onGridMouseUp,ondblclick:_onGridDoubleClick,touchstart:_onGridTouchStart,touchmove:_onGridTouchMove,touchend:_onGridTouchEnd\">\n\t\t\t\t<table data-dojo-attach-point=\"itemContainerTable\" class=\"dojoxCalendarContainerTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t\t\t</div>\n\t\t</div> \n\t</div>\t\n</div>\n"}});
define("dojox/calendar/MonthColumnView",["./ViewBase","dijit/_TemplatedMixin","./_VerticalScrollBarBase","dojo/text!./templates/MonthColumnView.html","dojo/_base/declare","dojo/_base/event","dojo/_base/lang","dojo/_base/array","dojo/_base/sniff","dojo/_base/fx","dojo/_base/html","dojo/on","dojo/dom","dojo/dom-class","dojo/dom-style","dojo/dom-geometry","dojo/dom-construct","dojo/mouse","dojo/query","dojo/i18n","dojox/html/metrics"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,fx,_a,on,_b,_c,_d,_e,_f,_10,_11,_12,_13){
return _5("dojox.calendar.MonthColumnView",[_1,_2],{baseClass:"dojoxCalendarMonthColumnView",templateString:_4,viewKind:"monthColumns",_setTabIndexAttr:"domNode",renderData:null,startDate:null,columnCount:6,daySize:30,showCellLabel:true,showHiddenItems:true,verticalRenderer:null,percentOverlap:0,horizontalGap:4,columnHeaderFormatLength:null,gridCellDatePattern:null,roundToDay:true,_layoutUnit:"month",_columnHeaderHandlers:null,constructor:function(){
this.invalidatingProperties=["columnCount","startDate","daySize","percentOverlap","verticalRenderer","columnHeaderDatePattern","horizontalGap","scrollBarRTLPosition","itemToRendererKindFunc","layoutPriorityFunction","textDir","items","showCellLabel","showHiddenItems"];
this._columnHeaderHandlers=[];
},postCreate:function(){
this.inherited(arguments);
this.keyboardUpDownUnit="day";
this.keyboardUpDownSteps=1;
this.keyboardLeftRightUnit="month";
this.keyboardLeftRightSteps=1;
this.allDayKeyboardUpDownUnit="day";
this.allDayKeyboardUpDownSteps=1;
this.allDayKeyboardLeftRightUnit="month";
this.allDayKeyboardLeftRightSteps=1;
},destroy:function(_14){
this._cleanupColumnHeader();
if(this.scrollBar){
this.scrollBar.destroy(_14);
}
this.inherited(arguments);
},_scrollBar_onScroll:function(_15){
this.scrollContainer.scrollTop=_15;
},buildRendering:function(){
this.inherited(arguments);
if(this.vScrollBar){
this.scrollBar=new _3({content:this.vScrollBarContent},this.vScrollBar);
this.scrollBar.on("scroll",_7.hitch(this,this._scrollBar_onScroll));
this._viewHandles.push(on(this.scrollContainer,_10.wheel,dojo.hitch(this,this._mouseWheelScrollHander)));
}
},postscript:function(){
this.inherited(arguments);
this._initialized=true;
if(!this.invalidRendering){
this.refreshRendering();
}
},_setVerticalRendererAttr:function(_16){
this._destroyRenderersByKind("vertical");
this._set("verticalRenderer",_16);
},_createRenderData:function(){
var rd={};
rd.daySize=this.get("daySize");
rd.scrollbarWidth=_13.getScrollbar().w+1;
rd.dateLocaleModule=this.dateLocaleModule;
rd.dateClassObj=this.dateClassObj;
rd.dateModule=this.dateModule;
rd.dates=[];
rd.columnCount=this.get("columnCount");
var d=this.get("startDate");
if(d==null){
d=new rd.dateClassObj();
}
d=this.floorToMonth(d,false,rd);
this.startDate=d;
var _17=d.getMonth();
var _18=0;
for(var col=0;col<rd.columnCount;col++){
var _19=[];
rd.dates.push(_19);
while(d.getMonth()==_17){
_19.push(d);
d=rd.dateModule.add(d,"day",1);
d=this.floorToDay(d,false,rd);
}
_17=d.getMonth();
if(_18<_19.length){
_18=_19.length;
}
}
rd.startTime=new rd.dateClassObj(rd.dates[0][0]);
rd.endTime=new rd.dateClassObj(_19[_19.length-1]);
rd.endTime=rd.dateModule.add(rd.endTime,"day",1);
rd.maxDayCount=_18;
rd.sheetHeight=rd.daySize*_18;
if(this.displayedItemsInvalidated&&!this._isEditing){
this.displayedItemsInvalidated=false;
this._computeVisibleItems(rd);
}else{
if(this.renderData){
rd.items=this.renderData.items;
}
}
return rd;
},_validateProperties:function(){
this.inherited(arguments);
if(this.columnCount<1||isNaN(this.columnCount)){
this.columnCount=1;
}
if(this.daySize<5||isNaN(this.daySize)){
this.daySize=5;
}
},_setStartDateAttr:function(_1a){
this.displayedItemsInvalidated=true;
this._set("startDate",_1a);
},_setColumnCountAttr:function(_1b){
this.displayedItemsInvalidated=true;
this._set("columnCount",_1b);
},__fixEvt:function(e){
e.sheet="primary";
e.source=this;
return e;
},_formatColumnHeaderLabel:function(d){
var len="wide";
if(this.columnHeaderFormatLength){
len=this.columnHeaderFormatLength;
}
var _1c=this.renderData.dateLocaleModule.getNames("months",len,"standAlone");
return _1c[d.getMonth()];
},_formatGridCellLabel:function(d,row,col){
var _1d,rb;
if(d==null){
return "";
}
if(this.gridCellPattern){
return this.renderData.dateLocaleModule.format(d,{selector:"date",datePattern:this.gridCellDatePattern});
}else{
rb=_12.getLocalization("dojo.cldr",this._calendar);
_1d=rb["dateFormatItem-d"];
var _1e=this.renderData.dateLocaleModule.getNames("days","abbr","standAlone");
return _1e[d.getDay()].substring(0,1)+" "+this.renderData.dateLocaleModule.format(d,{selector:"date",datePattern:_1d});
}
},scrollPosition:null,scrollBarRTLPosition:"left",_setScrollPositionAttr:function(_1f){
this._setScrollPosition(_1f.date,_1f.duration,_1f.easing);
},_getScrollPositionAttr:function(){
return {date:(this.scrollContainer.scrollTop/this.daySize)+1};
},_setScrollPosition:function(_20,_21,_22){
if(_20<1){
_20=1;
}else{
if(_20>31){
_20=31;
}
}
var _23=(_20-1)*this.daySize;
if(_21){
if(this._scrollAnimation){
this._scrollAnimation.stop();
}
var _24=Math.abs(((_23-this.scrollContainer.scrollTop)*_21)/this.renderData.sheetHeight);
this._scrollAnimation=new fx.Animation({curve:[this.scrollContainer.scrollTop,_23],duration:_24,easing:_22,onAnimate:_7.hitch(this,function(_25){
this._setScrollImpl(_25);
})});
this._scrollAnimation.play();
}else{
this._setScrollImpl(_23);
}
},_setScrollImpl:function(v){
this.scrollContainer.scrollTop=v;
if(this.scrollBar){
this.scrollBar.set("value",v);
}
},ensureVisibility:function(_26,end,_27,_28,_29){
_28=_28==undefined?1:_28;
if(this.scrollable&&this.autoScroll){
var s=_26.getDate()-_28;
if(this.isStartOfDay(end)){
end=this._waDojoxAddIssue(end,"day",-1);
}
var e=end.getDate()+_28;
var _2a=this.get("scrollPosition").date;
var r=_e.getContentBox(this.scrollContainer);
var _2b=(this.get("scrollPosition").date+(r.h/this.daySize));
var _2c=false;
var _2d=null;
switch(_27){
case "start":
_2c=s>=_2a&&s<=_2b;
_2d=s;
break;
case "end":
_2c=e>=_2a&&e<=_2b;
_2d=e-(_2b-_2a);
break;
case "both":
_2c=s>=_2a&&e<=_2b;
_2d=s;
break;
}
if(!_2c){
this._setScrollPosition(_2d,_29);
}
}
},scrollView:function(dir){
var pos=this.get("scrollPosition").date+dir;
this._setScrollPosition(pos);
},_mouseWheelScrollHander:function(e){
this.scrollView(e.wheelDelta>0?-1:1);
},refreshRendering:function(){
if(!this._initialized){
return;
}
this._validateProperties();
var _2e=this.renderData;
var rd=this._createRenderData();
this.renderData=rd;
this._createRendering(rd,_2e);
this._layoutRenderers(rd);
},_createRendering:function(_2f,_30){
_d.set(this.sheetContainer,"height",_2f.sheetHeight+"px");
this._configureScrollBar(_2f);
this._buildColumnHeader(_2f,_30);
this._buildGrid(_2f,_30);
this._buildItemContainer(_2f,_30);
},_configureScrollBar:function(_31){
if(_9("ie")&&this.scrollBar){
_d.set(this.scrollBar.domNode,"width",(_31.scrollbarWidth+1)+"px");
}
var _32=this.isLeftToRight()?true:this.scrollBarRTLPosition=="right";
var _33=_32?"right":"left";
var _34=_32?"left":"right";
if(this.scrollBar){
this.scrollBar.set("maximum",_31.sheetHeight);
_d.set(this.scrollBar.domNode,_33,0);
_d.set(this.scrollBar.domNode,_34,"auto");
}
_d.set(this.scrollContainer,_33,_31.scrollbarWidth+"px");
_d.set(this.scrollContainer,_34,"0");
_d.set(this.columnHeader,_33,_31.scrollbarWidth+"px");
_d.set(this.columnHeader,_34,"0");
if(this.buttonContainer&&this.owner!=null&&this.owner.currentView==this){
_d.set(this.buttonContainer,_33,_31.scrollbarWidth+"px");
_d.set(this.buttonContainer,_34,"0");
}
},_columnHeaderClick:function(e){
_6.stop(e);
var _35=_11("td",this.columnHeaderTable).indexOf(e.currentTarget);
this._onColumnHeaderClick({index:_35,date:this.renderData.dates[_35][0],triggerEvent:e});
},_buildColumnHeader:function(_36,_37){
var _38=this.columnHeaderTable;
if(!_38){
return;
}
var _39=_36.columnCount-(_37?_37.columnCount:0);
if(_9("ie")==8){
if(this._colTableSave==null){
this._colTableSave=_7.clone(_38);
}else{
if(_39<0){
this._cleanupColumnHeader();
this.columnHeader.removeChild(_38);
_f.destroy(_38);
_38=_7.clone(this._colTableSave);
this.columnHeaderTable=_38;
this.columnHeader.appendChild(_38);
_39=_36.columnCount;
}
}
}
var _3a=_11("tbody",_38);
var trs=_11("tr",_38);
var _3b,tr,td;
if(_3a.length==1){
_3b=_3a[0];
}else{
_3b=_a.create("tbody",null,_38);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_f.create("tr",null,_3b);
}
if(_39>0){
for(var i=0;i<_39;i++){
td=_f.create("td",null,tr);
var h=[];
h.push(on(td,"click",_7.hitch(this,this._columnHeaderClick)));
if(_9("touch")){
h.push(on(td,"touchstart",function(e){
_6.stop(e);
_c.add(e.currentTarget,"Active");
}));
h.push(on(td,"touchend",function(e){
_6.stop(e);
_c.remove(e.currentTarget,"Active");
}));
}else{
h.push(on(td,"mousedown",function(e){
_6.stop(e);
_c.add(e.currentTarget,"Active");
}));
h.push(on(td,"mouseup",function(e){
_6.stop(e);
_c.remove(e.currentTarget,"Active");
}));
h.push(on(td,"mouseover",function(e){
_6.stop(e);
_c.add(e.currentTarget,"Hover");
}));
h.push(on(td,"mouseout",function(e){
_6.stop(e);
_c.remove(e.currentTarget,"Hover");
}));
}
this._columnHeaderHandlers.push(h);
}
}else{
_39=-_39;
for(var i=0;i<_39;i++){
td=tr.lastChild;
tr.removeChild(td);
_f.destroy(td);
var _3c=this._columnHeaderHandlers.pop();
while(_3c.length>0){
_3c.pop().remove();
}
}
}
_11("td",_38).forEach(function(td,i){
td.className="";
if(i==0){
_c.add(td,"first-child");
}else{
if(i==this.renderData.columnCount-1){
_c.add(td,"last-child");
}
}
var d=_36.dates[i][0];
this._setText(td,this._formatColumnHeaderLabel(d));
this.styleColumnHeaderCell(td,d,_36);
},this);
},_cleanupColumnHeader:function(){
while(this._columnHeaderHandlers.length>0){
var _3d=this._columnHeaderHandlers.pop();
while(_3d.length>0){
_3d.pop().remove();
}
}
},styleColumnHeaderCell:function(_3e,_3f,_40){
},_buildGrid:function(_41,_42){
var _43=this.gridTable;
if(!_43){
return;
}
_d.set(_43,"height",_41.sheetHeight+"px");
var _44=_41.maxDayCount-(_42?_42.maxDayCount:0);
var _45=_44>0;
var _46=_41.columnCount-(_42?_42.columnCount:0);
if(_9("ie")==8){
if(this._gridTableSave==null){
this._gridTableSave=_7.clone(_43);
}else{
if(_46<0){
this.grid.removeChild(_43);
_f.destroy(_43);
_43=_7.clone(this._gridTableSave);
this.gridTable=_43;
this.grid.appendChild(_43);
_46=_41.columnCount;
_44=_41.maxDayCount;
_45=true;
}
}
}
var _47=_11("tbody",_43);
var _48;
if(_47.length==1){
_48=_47[0];
}else{
_48=_f.create("tbody",null,_43);
}
if(_45){
for(var i=0;i<_44;i++){
_f.create("tr",null,_48);
}
}else{
_44=-_44;
for(var i=0;i<_44;i++){
_48.removeChild(_48.lastChild);
}
}
var _49=_41.maxDayCount-_44;
var _4a=_45||_46>0;
_46=_4a?_46:-_46;
_11("tr",_43).forEach(function(tr,i){
if(_4a){
var len=i>=_49?_41.columnCount:_46;
for(var i=0;i<len;i++){
var td=_f.create("td",null,tr);
_f.create("span",null,td);
}
}else{
for(var i=0;i<_46;i++){
tr.removeChild(tr.lastChild);
}
}
});
_11("tr",_43).forEach(function(tr,row){
tr.className="";
if(row==0){
_c.add(tr,"first-child");
}
if(row==_41.maxDayCount-1){
_c.add(tr,"last-child");
}
_11("td",tr).forEach(function(td,col){
td.className="";
if(col==0){
_c.add(td,"first-child");
}
if(col==_41.columnCount-1){
_c.add(td,"last-child");
}
var d=null;
if(row<_41.dates[col].length){
d=_41.dates[col][row];
}
var _4b=_11("span",td)[0];
this._setText(_4b,this.showCellLabel?this._formatGridCellLabel(d,row,col):null);
this.styleGridCell(td,d,col,row,_41);
},this);
},this);
},styleGridCellFunc:null,defaultStyleGridCell:function(_4c,_4d,col,row,_4e){
if(_4d==null){
return;
}
_c.add(_4c,this._cssDays[_4d.getDay()]);
if(this.isToday(_4d)){
_c.add(_4c,"dojoxCalendarToday");
}else{
if(this.isWeekEnd(_4d)){
_c.add(_4c,"dojoxCalendarWeekend");
}
}
},styleGridCell:function(_4f,_50,col,row,_51){
if(this.styleGridCellFunc){
this.styleGridCellFunc(_4f,_50,col,row,_51);
}else{
this.defaultStyleGridCell(_4f,_50,col,row,_51);
}
},_buildItemContainer:function(_52,_53){
var _54=this.itemContainerTable;
if(!_54){
return;
}
var _55=[];
_d.set(_54,"height",_52.sheetHeight+"px");
var _56=_52.columnCount-(_53?_53.columnCount:0);
if(_9("ie")==8){
if(this._itemTableSave==null){
this._itemTableSave=_7.clone(_54);
}else{
if(_56<0){
this.itemContainer.removeChild(_54);
this._recycleItemRenderers(true);
_f.destroy(_54);
_54=_7.clone(this._itemTableSave);
this.itemContainerTable=_54;
this.itemContainer.appendChild(_54);
_56=_52.columnCount;
}
}
}
var _57=_11("tbody",_54);
var trs=_11("tr",_54);
var _58,tr,td;
if(_57.length==1){
_58=_57[0];
}else{
_58=_f.create("tbody",null,_54);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_f.create("tr",null,_58);
}
if(_56>0){
for(var i=0;i<_56;i++){
td=_f.create("td",null,tr);
_f.create("div",{"className":"dojoxCalendarContainerColumn"},td);
}
}else{
_56=-_56;
for(var i=0;i<_56;i++){
tr.removeChild(tr.lastChild);
}
}
_11("td>div",_54).forEach(function(div,i){
_d.set(div,{"height":_52.sheetHeight+"px"});
_55.push(div);
},this);
_52.cells=_55;
},_overlapLayoutPass2:function(_59){
var i,j,_5a,_5b;
_5a=_59[_59.length-1];
for(j=0;j<_5a.length;j++){
_5a[j].extent=1;
}
for(i=0;i<_59.length-1;i++){
_5a=_59[i];
for(var j=0;j<_5a.length;j++){
_5b=_5a[j];
if(_5b.extent==-1){
_5b.extent=1;
var _5c=0;
var _5d=false;
for(var k=i+1;k<_59.length&&!_5d;k++){
var _5e=_59[k];
for(var l=0;l<_5e.length&&!_5d;l++){
var _5f=_5e[l];
if(_5b.start<_5f.end&&_5f.start<_5b.end){
_5d=true;
}
}
if(!_5d){
_5c++;
}
}
_5b.extent+=_5c;
}
}
}
},_defaultItemToRendererKindFunc:function(_60){
if(_60.allDay){
return "vertical";
}
var dur=Math.abs(this.renderData.dateModule.difference(_60.startTime,_60.endTime,"minute"));
return dur>=1440?"vertical":null;
},_layoutRenderers:function(_61){
this.hiddenEvents={};
this.inherited(arguments);
},_layoutInterval:function(_62,_63,_64,end,_65){
var _66=[];
var _67=[];
_62.colW=this.itemContainer.offsetWidth/_62.columnCount;
for(var i=0;i<_65.length;i++){
var _68=_65[i];
if(this._itemToRendererKind(_68)=="vertical"){
_66.push(_68);
}else{
if(this.showHiddenItems){
_67.push(_68);
}
}
}
if(_66.length>0){
this._layoutVerticalItems(_62,_63,_64,end,_66);
}
if(_67.length>0){
this._layoutBgItems(_62,_63,_64,end,_67);
}
},_dateToYCoordinate:function(_69,d,_6a){
var pos=0;
if(_6a||d.getHours()!=0||d.getMinutes()!=0){
pos=(d.getDate()-1)*this.renderData.daySize;
}else{
var d2=this._waDojoxAddIssue(d,"day",-1);
pos=this.renderData.daySize+((d2.getDate()-1)*this.renderData.daySize);
}
pos+=(d.getHours()*60+d.getMinutes())*this.renderData.daySize/1440;
return pos;
},_layoutVerticalItems:function(_6b,_6c,_6d,_6e,_6f){
if(this.verticalRenderer==null){
return;
}
var _70=_6b.cells[_6c];
var _71=[];
for(var i=0;i<_6f.length;i++){
var _72=_6f[i];
var _73=this.computeRangeOverlap(_6b,_72.startTime,_72.endTime,_6d,_6e);
var top=this._dateToYCoordinate(_6b,_73[0],true);
var _74=this._dateToYCoordinate(_6b,_73[1],false);
if(_74>top){
var _75=_7.mixin({start:top,end:_74,range:_73,item:_72},_72);
_71.push(_75);
}
}
var _76=this.computeOverlapping(_71,this._overlapLayoutPass2).numLanes;
var _77=this.percentOverlap/100;
for(i=0;i<_71.length;i++){
_72=_71[i];
var _78=_72.lane;
var _79=_72.extent;
var w;
var _7a;
if(_77==0){
w=_76==1?_6b.colW:((_6b.colW-(_76-1)*this.horizontalGap)/_76);
_7a=_78*(w+this.horizontalGap);
w=_79==1?w:w*_79+(_79-1)*this.horizontalGap;
w=100*w/_6b.colW;
_7a=100*_7a/_6b.colW;
}else{
w=_76==1?100:(100/(_76-(_76-1)*_77));
_7a=_78*(w-_77*w);
w=_79==1?w:w*(_79-(_79-1)*_77);
}
var ir=this._createRenderer(_72,"vertical",this.verticalRenderer,"dojoxCalendarVertical");
_d.set(ir.container,{"top":_72.start+"px","left":_7a+"%","width":w+"%","height":(_72.end-_72.start+1)+"px"});
var _7b=this.isItemBeingEdited(_72);
var _7c=this.isItemSelected(_72);
var _7d=this.isItemHovered(_72);
var _7e=this.isItemFocused(_72);
var _7f=ir.renderer;
_7f.set("hovered",_7d);
_7f.set("selected",_7c);
_7f.set("edited",_7b);
_7f.set("focused",this.showFocus?_7e:false);
_7f.set("storeState",this.getItemStoreState(_72));
_7f.set("moveEnabled",this.isItemMoveEnabled(_72._item,"vertical"));
_7f.set("resizeEnabled",this.isItemResizeEnabled(_72._item,"vertical"));
this.applyRendererZIndex(_72,ir,_7d,_7c,_7b,_7e);
if(_7f.updateRendering){
_7f.updateRendering(w,_72.end-_72.start+1);
}
_f.place(ir.container,_70);
_d.set(ir.container,"display","block");
}
},_getCellAt:function(_80,_81,rtl){
if((rtl==undefined||rtl==true)&&!this.isLeftToRight()){
_81=this.renderData.columnCount-1-_81;
}
return this.gridTable.childNodes[0].childNodes[_80].childNodes[_81];
},invalidateLayout:function(){
_11("td",this.gridTable).forEach(function(td){
_c.remove(td,"dojoxCalendarHiddenEvents");
});
this.inherited(arguments);
},_layoutBgItems:function(_82,col,_83,_84,_85){
var _86={};
for(var i=0;i<_85.length;i++){
var _87=_85[i];
var _88=this.computeRangeOverlap(_82,_87.startTime,_87.endTime,_83,_84);
var _89=_88[0].getDate()-1;
var end;
if(this.isStartOfDay(_88[1])){
end=this._waDojoxAddIssue(_88[1],"day",-1);
end=end.getDate()-1;
}else{
end=_88[1].getDate()-1;
}
for(var d=_89;d<=end;d++){
_86[d]=true;
}
}
for(var row in _86){
if(_86[row]){
var _8a=this._getCellAt(row,col,false);
_c.add(_8a,"dojoxCalendarHiddenEvents");
}
}
},_sortItemsFunction:function(a,b){
var res=this.dateModule.compare(a.startTime,b.startTime);
if(res==0){
res=-1*this.dateModule.compare(a.endTime,b.endTime);
}
return this.isLeftToRight()?res:-res;
},getTime:function(e,x,y,_8b){
if(e!=null){
var _8c=_e.position(this.itemContainer,true);
if(e.touches){
_8b=_8b==undefined?0:_8b;
x=e.touches[_8b].pageX-_8c.x;
y=e.touches[_8b].pageY-_8c.y;
}else{
x=e.pageX-_8c.x;
y=e.pageY-_8c.y;
}
}
var r=_e.getContentBox(this.itemContainer);
if(!this.isLeftToRight()){
x=r.w-x;
}
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
var col=Math.floor(x/(r.w/this.renderData.columnCount));
var row=Math.floor(y/(r.h/this.renderData.maxDayCount));
var _8d=null;
if(col<this.renderData.dates.length&&row<this.renderData.dates[col].length){
_8d=this.newDate(this.renderData.dates[col][row]);
}
return _8d;
},_onGridMouseUp:function(e){
this.inherited(arguments);
if(this._gridMouseDown){
this._gridMouseDown=false;
this._onGridClick({date:this.getTime(e),triggerEvent:e});
}
},_onGridTouchStart:function(e){
this.inherited(arguments);
var g=this._gridProps;
g.moved=false;
g.start=e.touches[0].screenY;
g.scrollTop=this.scrollContainer.scrollTop;
},_onGridTouchMove:function(e){
this.inherited(arguments);
if(e.touches.length>1&&!this._isEditing){
_6.stop(e);
return;
}
if(this._gridProps&&!this._isEditing){
var _8e={x:e.touches[0].screenX,y:e.touches[0].screenY};
var p=this._edProps;
if(!p||p&&(Math.abs(_8e.x-p.start.x)>25||Math.abs(_8e.y-p.start.y)>25)){
this._gridProps.moved=true;
var d=e.touches[0].screenY-this._gridProps.start;
var _8f=this._gridProps.scrollTop-d;
var max=this.itemContainer.offsetHeight-this.scrollContainer.offsetHeight;
if(_8f<0){
this._gridProps.start=e.touches[0].screenY;
this._setScrollImpl(0);
this._gridProps.scrollTop=0;
}else{
if(_8f>max){
this._gridProps.start=e.touches[0].screenY;
this._setScrollImpl(max);
this._gridProps.scrollTop=max;
}else{
this._setScrollImpl(_8f);
}
}
}
}
},_onGridTouchEnd:function(e){
this.inherited(arguments);
var g=this._gridProps;
if(g){
if(!this._isEditing){
if(!g.moved){
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
this._pendingDoubleTap={grid:true,timer:setTimeout(_7.hitch(this,function(){
delete this._pendingDoubleTap;
}),this.doubleTapDelay)};
}
}
}
}
this._gridProps=null;
}
},_onColumnHeaderClick:function(e){
this._dispatchCalendarEvt(e,"onColumnHeaderClick");
},onColumnHeaderClick:function(e){
},_onScrollTimer_tick:function(){
this._setScrollImpl(this.scrollContainer.scrollTop+this._scrollProps.scrollStep);
},snapUnit:"day",snapSteps:1,minDurationUnit:"day",minDurationSteps:1,liveLayout:false,stayInView:true,allowStartEndSwap:true,allowResizeLessThan24H:false});
});
