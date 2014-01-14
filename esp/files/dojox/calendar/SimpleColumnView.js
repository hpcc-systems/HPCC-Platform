//>>built
require({cache:{"url:dojox/calendar/templates/SimpleColumnView.html":"<div data-dojo-attach-events=\"keydown:_onKeyDown\">\t\n\t<div data-dojo-attach-point=\"header\" class=\"dojoxCalendarHeader\">\n\t\t<div class=\"dojoxCalendarYearColumnHeader\" data-dojo-attach-point=\"yearColumnHeader\">\n\t\t\t<table><tr><td><span data-dojo-attach-point=\"yearColumnHeaderContent\"></span></td></tr></table>\t\t\n\t\t</div>\n\t\t<div data-dojo-attach-point=\"columnHeader\" class=\"dojoxCalendarColumnHeader\">\n\t\t\t<table data-dojo-attach-point=\"columnHeaderTable\" class=\"dojoxCalendarColumnHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t\t</div>\n\t</div>\t\n\t<div data-dojo-attach-point=\"vScrollBar\" class=\"dojoxCalendarVScrollBar\">\n\t\t<div data-dojo-attach-point=\"vScrollBarContent\" style=\"visibility:hidden;position:relative; width:1px; height:1px;\" ></div>\n\t</div>\t\n\t<div data-dojo-attach-point=\"scrollContainer\" class=\"dojoxCalendarScrollContainer\">\n\t\t<div data-dojo-attach-point=\"sheetContainer\" style=\"position:relative;left:0;right:0;margin:0;padding:0\">\n\t\t\t<div data-dojo-attach-point=\"rowHeader\" class=\"dojoxCalendarRowHeader\">\n\t\t\t\t<table data-dojo-attach-point=\"rowHeaderTable\" class=\"dojoxCalendarRowHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t\t\t</div>\n\t\t\t<div data-dojo-attach-point=\"grid\" class=\"dojoxCalendarGrid\">\n\t\t\t\t<table data-dojo-attach-point=\"gridTable\" class=\"dojoxCalendarGridTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t\t\t</div>\n\t\t\t<div data-dojo-attach-point=\"itemContainer\" class=\"dojoxCalendarContainer\" data-dojo-attach-event=\"mousedown:_onGridMouseDown,mouseup:_onGridMouseUp,ondblclick:_onGridDoubleClick,touchstart:_onGridTouchStart,touchmove:_onGridTouchMove,touchend:_onGridTouchEnd\">\n\t\t\t\t<table data-dojo-attach-point=\"itemContainerTable\" class=\"dojoxCalendarContainerTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t\t\t</div>\n\t\t</div> \n\t</div>\n</div>\n\n"}});
define("dojox/calendar/SimpleColumnView",["./ViewBase","dijit/_TemplatedMixin","./_VerticalScrollBarBase","dojo/text!./templates/SimpleColumnView.html","dojo/_base/declare","dojo/_base/event","dojo/_base/lang","dojo/_base/array","dojo/_base/sniff","dojo/_base/fx","dojo/_base/html","dojo/on","dojo/dom","dojo/dom-class","dojo/dom-style","dojo/dom-geometry","dojo/dom-construct","dojo/mouse","dojo/query","dojox/html/metrics"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,fx,_a,on,_b,_c,_d,_e,_f,_10,_11,_12){
return _5("dojox.calendar.SimpleColumnView",[_1,_2],{baseClass:"dojoxCalendarSimpleColumnView",templateString:_4,viewKind:"columns",_setTabIndexAttr:"domNode",renderData:null,startDate:null,columnCount:7,minHours:8,maxHours:18,hourSize:100,timeSlotDuration:15,rowHeaderGridSlotDuration:60,rowHeaderLabelSlotDuration:60,rowHeaderLabelOffset:2,rowHeaderFirstLabelOffset:2,verticalRenderer:null,percentOverlap:70,horizontalGap:4,_columnHeaderHandlers:null,constructor:function(){
this.invalidatingProperties=["columnCount","startDate","minHours","maxHours","hourSize","verticalRenderer","rowHeaderTimePattern","columnHeaderDatePattern","timeSlotDuration","rowHeaderGridSlotDuration","rowHeaderLabelSlotDuration","rowHeaderLabelOffset","rowHeaderFirstLabelOffset","percentOverlap","horizontalGap","scrollBarRTLPosition","itemToRendererKindFunc","layoutPriorityFunction","formatItemTimeFunc","textDir","items"];
this._columnHeaderHandlers=[];
},destroy:function(_13){
this._cleanupColumnHeader();
if(this.scrollBar){
this.scrollBar.destroy(_13);
}
this.inherited(arguments);
},_scrollBar_onScroll:function(_14){
this._setScrollPosition(_14);
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
},_setVerticalRendererAttr:function(_15){
this._destroyRenderersByKind("vertical");
this._set("verticalRenderer",_15);
},_createRenderData:function(){
var _16={};
_16.minHours=this.get("minHours");
_16.maxHours=this.get("maxHours");
_16.hourSize=this.get("hourSize");
_16.hourCount=_16.maxHours-_16.minHours;
_16.slotDuration=this.get("timeSlotDuration");
_16.rowHeaderGridSlotDuration=this.get("rowHeaderGridSlotDuration");
_16.slotSize=Math.ceil(_16.hourSize/(60/_16.slotDuration));
_16.hourSize=_16.slotSize*(60/_16.slotDuration);
_16.sheetHeight=_16.hourSize*_16.hourCount;
_16.scrollbarWidth=_12.getScrollbar().w+1;
_16.dateLocaleModule=this.dateLocaleModule;
_16.dateClassObj=this.dateClassObj;
_16.dateModule=this.dateModule;
_16.dates=[];
_16.columnCount=this.get("columnCount");
var d=this.get("startDate");
if(d==null){
d=new _16.dateClassObj();
}
d=this.floorToDay(d,false,_16);
this.startDate=d;
for(var col=0;col<_16.columnCount;col++){
_16.dates.push(d);
d=_16.dateModule.add(d,"day",1);
d=this.floorToDay(d,false,_16);
}
_16.startTime=new _16.dateClassObj(_16.dates[0]);
_16.startTime.setHours(_16.minHours);
_16.endTime=new _16.dateClassObj(_16.dates[_16.columnCount-1]);
_16.endTime.setHours(_16.maxHours);
if(this.displayedItemsInvalidated&&!this._isEditing){
this._computeVisibleItems(_16);
}else{
if(this.renderData){
_16.items=this.renderData.items;
}
}
return _16;
},_validateProperties:function(){
this.inherited(arguments);
var v=this.minHours;
if(v<0||v>23||isNaN(v)){
this.minHours=0;
}
v=this.maxHours;
if(v<1||v>24||isNaN(v)){
this.minHours=24;
}
if(this.minHours>this.maxHours){
var t=this.maxHours;
this.maxHours=this.minHours;
this.minHours=t;
}
if(this.maxHours-this.minHours<1){
this.minHours=0;
this.maxHours=24;
}
if(this.columnCount<1||isNaN(this.columnCount)){
this.columnCount=1;
}
v=this.percentOverlap;
if(v<0||v>100||isNaN(v)){
this.percentOverlap=70;
}
if(this.hourSize<5||isNaN(this.hourSize)){
this.hourSize=10;
}
v=this.timeSlotDuration;
if(v<1||v>60||isNaN(v)){
this.timeSlotDuration=15;
}
},_setStartDateAttr:function(_17){
this.displayedItemsInvalidated=true;
this._set("startDate",_17);
},_setColumnCountAttr:function(_18){
this.displayedItemsInvalidated=true;
this._set("columnCount",_18);
},__fixEvt:function(e){
e.sheet="primary";
e.source=this;
return e;
},_formatRowHeaderLabel:function(d){
return this.renderData.dateLocaleModule.format(d,{selector:"time",timePattern:this.rowHeaderTimePattern});
},_formatColumnHeaderLabel:function(d){
return this.renderData.dateLocaleModule.format(d,{selector:"date",datePattern:this.columnHeaderDatePattern,formatLength:"medium"});
},startTimeOfDay:null,scrollBarRTLPosition:"left",_getStartTimeOfDay:function(){
var v=(this.get("maxHours")-this.get("minHours"))*this._getScrollPosition()/this.renderData.sheetHeight;
return {hours:this.renderData.minHours+Math.floor(v),minutes:(v-Math.floor(v))*60};
},_getEndTimeOfDay:function(){
var v=(this.get("maxHours")-this.get("minHours"))*(this._getScrollPosition()+this.scrollContainer.offsetHeight)/this.renderData.sheetHeight;
return {hours:this.renderData.minHours+Math.floor(v),minutes:(v-Math.floor(v))*60};
},startTimeOfDay:0,_setStartTimeOfDayAttr:function(_19){
if(this.renderData){
this._setStartTimeOfDay(_19.hours,_19.minutes,_19.duration,_19.easing);
}else{
this._startTimeOfDayInvalidated=true;
}
this._set("startTimeOfDay",_19);
},_getStartTimeOfDayAttr:function(){
if(this.renderData){
return this._getStartTimeOfDay();
}else{
return this._get("startTimeOfDay");
}
},_setStartTimeOfDay:function(_1a,_1b,_1c,_1d){
var rd=this.renderData;
_1a=_1a||rd.minHours;
_1b=_1b||0;
_1c=_1c||0;
if(_1b<0){
_1b=0;
}else{
if(_1b>59){
_1b=59;
}
}
if(_1a<0){
_1a=0;
}else{
if(_1a>24){
_1a=24;
}
}
var _1e=_1a*60+_1b;
var _1f=rd.minHours*60;
var _20=rd.maxHours*60;
if(_1e<_1f){
_1e=_1f;
}else{
if(_1e>_20){
_1e=_20;
}
}
var pos=(_1e-_1f)*rd.sheetHeight/(_20-_1f);
pos=Math.min(rd.sheetHeight-this.scrollContainer.offsetHeight,pos);
this._scrollToPosition(pos,_1c,_1d);
},_scrollToPosition:function(_21,_22,_23){
if(_22){
if(this._scrollAnimation){
this._scrollAnimation.stop();
}
var _24=this._getScrollPosition();
var _25=Math.abs(((_21-_24)*_22)/this.renderData.sheetHeight);
this._scrollAnimation=new fx.Animation({curve:[_24,_21],duration:_25,easing:_23,onAnimate:_7.hitch(this,function(_26){
this._setScrollImpl(_26);
})});
this._scrollAnimation.play();
}else{
this._setScrollImpl(_21);
}
},_setScrollImpl:function(v){
this._setScrollPosition(v);
if(this.scrollBar){
this.scrollBar.set("value",v);
}
},ensureVisibility:function(_27,end,_28,_29,_2a){
_29=_29==undefined?this.renderData.slotDuration:_29;
if(this.scrollable&&this.autoScroll){
var s=_27.getHours()*60+_27.getMinutes()-_29;
var e=end.getHours()*60+end.getMinutes()+_29;
var vs=this._getStartTimeOfDay();
var ve=this._getEndTimeOfDay();
var _2b=vs.hours*60+vs.minutes;
var _2c=ve.hours*60+ve.minutes;
var _2d=false;
var _2e=null;
switch(_28){
case "start":
_2d=s>=_2b&&s<=_2c;
_2e=s;
break;
case "end":
_2d=e>=_2b&&e<=_2c;
_2e=e-(_2c-_2b);
break;
case "both":
_2d=s>=_2b&&e<=_2c;
_2e=s;
break;
}
if(!_2d){
this._setStartTimeOfDay(Math.floor(_2e/60),_2e%60,_2a);
}
}
},scrollView:function(dir){
var t=this._getStartTimeOfDay();
t=t.hours*60+t.minutes+(dir*this.timeSlotDuration);
this._setStartTimeOfDay(Math.floor(t/60),t%60);
},_mouseWheelScrollHander:function(e){
this.scrollView(e.wheelDelta>0?-1:1);
},refreshRendering:function(){
if(!this._initialized){
return;
}
this._validateProperties();
var _2f=this.renderData;
var rd=this._createRenderData();
this.renderData=rd;
this._createRendering(rd,_2f);
this._layoutRenderers(rd);
},_createRendering:function(_30,_31){
_d.set(this.sheetContainer,"height",_30.sheetHeight+"px");
this._configureScrollBar(_30);
this._buildColumnHeader(_30,_31);
this._buildRowHeader(_30,_31);
this._buildGrid(_30,_31);
this._buildItemContainer(_30,_31);
this._commitProperties(_30);
},_commitProperties:function(_32){
if(this._startTimeOfDayInvalidated){
this._startTimeOfDayInvalidated=false;
var v=this.startTimeOfDay;
if(v!=null){
this._setStartTimeOfDay(v.hours,v.minutes==undefined?0:v.minutes);
}
}
},_configureScrollBar:function(_33){
if(_9("ie")&&this.scrollBar){
_d.set(this.scrollBar.domNode,"width",(_33.scrollbarWidth+1)+"px");
}
var _34=this.isLeftToRight()?true:this.scrollBarRTLPosition=="right";
var _35=_34?"right":"left";
var _36=_34?"left":"right";
if(this.scrollBar){
this.scrollBar.set("maximum",_33.sheetHeight);
_d.set(this.scrollBar.domNode,_35,0);
_d.set(this.scrollBar.domNode,_34?"left":"right","auto");
}
_d.set(this.scrollContainer,_35,_33.scrollbarWidth+"px");
_d.set(this.scrollContainer,_36,"0");
_d.set(this.header,_35,_33.scrollbarWidth+"px");
_d.set(this.header,_36,"0");
if(this.buttonContainer&&this.owner!=null&&this.owner.currentView==this){
_d.set(this.buttonContainer,_35,_33.scrollbarWidth+"px");
_d.set(this.buttonContainer,_36,"0");
}
},_columnHeaderClick:function(e){
_6.stop(e);
var _37=_11("td",this.columnHeaderTable).indexOf(e.currentTarget);
this._onColumnHeaderClick({index:_37,date:this.renderData.dates[_37],triggerEvent:e});
},_buildColumnHeader:function(_38,_39){
var _3a=this.columnHeaderTable;
if(!_3a){
return;
}
var _3b=_38.columnCount-(_39?_39.columnCount:0);
if(_9("ie")==8){
if(this._colTableSave==null){
this._colTableSave=_7.clone(_3a);
}else{
if(_3b<0){
this._cleanupColumnHeader();
this.columnHeader.removeChild(_3a);
_f.destroy(_3a);
_3a=_7.clone(this._colTableSave);
this.columnHeaderTable=_3a;
this.columnHeader.appendChild(_3a);
_3b=_38.columnCount;
}
}
}
var _3c=_11("tbody",_3a);
var trs=_11("tr",_3a);
var _3d,tr,td;
if(_3c.length==1){
_3d=_3c[0];
}else{
_3d=_a.create("tbody",null,_3a);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_f.create("tr",null,_3d);
}
if(_3b>0){
for(var i=0;i<_3b;i++){
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
_3b=-_3b;
for(var i=0;i<_3b;i++){
td=tr.lastChild;
tr.removeChild(td);
_f.destroy(td);
var _3e=this._columnHeaderHandlers.pop();
while(_3e.length>0){
_3e.pop().remove();
}
}
}
_11("td",_3a).forEach(function(td,i){
td.className="";
if(i==0){
_c.add(td,"first-child");
}else{
if(i==this.renderData.columnCount-1){
_c.add(td,"last-child");
}
}
var d=_38.dates[i];
this._setText(td,this._formatColumnHeaderLabel(d));
this.styleColumnHeaderCell(td,d,_38);
},this);
if(this.yearColumnHeaderContent){
var d=_38.dates[0];
this._setText(this.yearColumnHeaderContent,_38.dateLocaleModule.format(d,{selector:"date",datePattern:"yyyy"}));
}
},_cleanupColumnHeader:function(){
while(this._columnHeaderHandlers.length>0){
var _3f=this._columnHeaderHandlers.pop();
while(_3f.length>0){
_3f.pop().remove();
}
}
},styleColumnHeaderCell:function(_40,_41,_42){
_c.add(_40,this._cssDays[_41.getDay()]);
if(this.isToday(_41)){
_c.add(_40,"dojoxCalendarToday");
}else{
if(this.isWeekEnd(_41)){
_c.add(_40,"dojoxCalendarWeekend");
}
}
},_addMinutesClasses:function(_43,_44){
switch(_44){
case 0:
_c.add(_43,"hour");
break;
case 30:
_c.add(_43,"halfhour");
break;
case 15:
case 45:
_c.add(_43,"quarterhour");
break;
}
},_buildRowHeader:function(_45,_46){
var _47=this.rowHeaderTable;
if(!_47){
return;
}
if(this._rowHeaderLabelContainer==null){
this._rowHeaderLabelContainer=_f.create("div",{"class":"dojoxCalendarRowHeaderLabelContainer"},this.rowHeader);
}
_d.set(_47,"height",_45.sheetHeight+"px");
var _48=_11("tbody",_47);
var _49,tr,td;
if(_48.length==1){
_49=_48[0];
}else{
_49=_f.create("tbody",null,_47);
}
var _4a=Math.floor(60/_45.rowHeaderGridSlotDuration)*_45.hourCount;
var _4b=_4a-(_46?Math.floor(60/_46.rowHeaderGridSlotDuration)*_46.hourCount:0);
if(_4b>0){
for(var i=0;i<_4b;i++){
tr=_f.create("tr",null,_49);
td=_f.create("td",null,tr);
}
}else{
_4b=-_4b;
for(var i=0;i<_4b;i++){
_49.removeChild(_49.lastChild);
}
}
var rd=this.renderData;
var _4c=Math.ceil(_45.hourSize/(60/_45.rowHeaderGridSlotDuration));
var d=new Date(2000,0,1,0,0,0);
_11("tr",_47).forEach(function(tr,i){
var td=_11("td",tr)[0];
td.className="";
_d.set(tr,"height",(_9("ie")==7)?_4c-2*(60/_45.rowHeaderGridSlotDuration):_4c+"px");
this.styleRowHeaderCell(td,d.getHours(),d.getMinutes(),rd);
var m=(i*this.renderData.rowHeaderGridSlotDuration)%60;
this._addMinutesClasses(td,m);
},this);
var lc=this._rowHeaderLabelContainer;
_4b=(Math.floor(60/this.rowHeaderLabelSlotDuration)*_45.hourCount)-lc.childNodes.length;
var _4d;
if(_4b>0){
for(var i=0;i<_4b;i++){
_4d=_f.create("span",null,lc);
_c.add(_4d,"dojoxCalendarRowHeaderLabel");
}
}else{
_4b=-_4b;
for(var i=0;i<_4b;i++){
lc.removeChild(lc.lastChild);
}
}
_4c=Math.ceil(_45.hourSize/(60/this.rowHeaderLabelSlotDuration));
_11(">span",lc).forEach(function(_4e,i){
d.setHours(0);
d.setMinutes(_45.minHours*60+(i*this.rowHeaderLabelSlotDuration));
this._configureRowHeaderLabel(_4e,d,i,_4c*i,rd);
},this);
},_configureRowHeaderLabel:function(_4f,d,_50,pos,_51){
this._setText(_4f,this._formatRowHeaderLabel(d));
_d.set(_4f,"top",(pos+(_50==0?this.rowHeaderFirstLabelOffset:this.rowHeaderLabelOffset))+"px");
var m=(_50*this.rowHeaderLabelSlotDuration)%60;
_c.remove(_4f,["hour","halfhour","quarterhour"]);
this._addMinutesClasses(_4f,m);
},styleRowHeaderCell:function(_52,h,m,_53){
},_buildGrid:function(_54,_55){
var _56=this.gridTable;
if(!_56){
return;
}
_d.set(_56,"height",_54.sheetHeight+"px");
var _57=Math.floor(60/_54.slotDuration)*_54.hourCount;
var _58=_57-(_55?Math.floor(60/_55.slotDuration)*_55.hourCount:0);
var _59=_58>0;
var _5a=_54.columnCount-(_55?_55.columnCount:0);
if(_9("ie")==8){
if(this._gridTableSave==null){
this._gridTableSave=_7.clone(_56);
}else{
if(_5a<0){
this.grid.removeChild(_56);
_f.destroy(_56);
_56=_7.clone(this._gridTableSave);
this.gridTable=_56;
this.grid.appendChild(_56);
_5a=_54.columnCount;
_58=_57;
_59=true;
}
}
}
var _5b=_11("tbody",_56);
var _5c;
if(_5b.length==1){
_5c=_5b[0];
}else{
_5c=_f.create("tbody",null,_56);
}
if(_59){
for(var i=0;i<_58;i++){
_f.create("tr",null,_5c);
}
}else{
_58=-_58;
for(var i=0;i<_58;i++){
_5c.removeChild(_5c.lastChild);
}
}
var _5d=Math.floor(60/_54.slotDuration)*_54.hourCount-_58;
var _5e=_59||_5a>0;
_5a=_5e?_5a:-_5a;
_11("tr",_56).forEach(function(tr,i){
if(_5e){
var len=i>=_5d?_54.columnCount:_5a;
for(var i=0;i<len;i++){
_f.create("td",null,tr);
}
}else{
for(var i=0;i<_5a;i++){
tr.removeChild(tr.lastChild);
}
}
});
_11("tr",_56).forEach(function(tr,i){
_d.set(tr,"height",_54.slotSize+"px");
if(i==0){
_c.add(tr,"first-child");
}else{
if(i==_57-1){
_c.add(tr,"last-child");
}
}
var m=(i*this.renderData.slotDuration)%60;
var h=this.minHours+Math.floor((i*this.renderData.slotDuration)/60);
_11("td",tr).forEach(function(td,col){
td.className="";
if(col==0){
_c.add(td,"first-child");
}else{
if(col==this.renderData.columnCount-1){
_c.add(td,"last-child");
}
}
var d=_54.dates[col];
this.styleGridCell(td,d,h,m,_54);
this._addMinutesClasses(td,m);
},this);
},this);
},styleGridCellFunc:null,defaultStyleGridCell:function(_5f,_60,_61,_62,_63){
_c.add(_5f,[this._cssDays[_60.getDay()],"H"+_61,"M"+_62]);
if(this.isToday(_60)){
return _c.add(_5f,"dojoxCalendarToday");
}else{
if(this.isWeekEnd(_60)){
return _c.add(_5f,"dojoxCalendarWeekend");
}
}
},styleGridCell:function(_64,_65,_66,_67,_68){
if(this.styleGridCellFunc){
this.styleGridCellFunc(_64,_65,_66,_67,_68);
}else{
this.defaultStyleGridCell(_64,_65,_66,_67,_68);
}
},_buildItemContainer:function(_69,_6a){
var _6b=this.itemContainerTable;
if(!_6b){
return;
}
var _6c=[];
_d.set(_6b,"height",_69.sheetHeight+"px");
var _6d=_69.columnCount-(_6a?_6a.columnCount:0);
if(_9("ie")==8){
if(this._itemTableSave==null){
this._itemTableSave=_7.clone(_6b);
}else{
if(_6d<0){
this.itemContainer.removeChild(_6b);
this._recycleItemRenderers(true);
_f.destroy(_6b);
_6b=_7.clone(this._itemTableSave);
this.itemContainerTable=_6b;
this.itemContainer.appendChild(_6b);
_6d=_69.columnCount;
}
}
}
var _6e=_11("tbody",_6b);
var trs=_11("tr",_6b);
var _6f,tr,td;
if(_6e.length==1){
_6f=_6e[0];
}else{
_6f=_f.create("tbody",null,_6b);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_f.create("tr",null,_6f);
}
if(_6d>0){
for(var i=0;i<_6d;i++){
td=_f.create("td",null,tr);
_f.create("div",{"className":"dojoxCalendarContainerColumn"},td);
}
}else{
_6d=-_6d;
for(var i=0;i<_6d;i++){
tr.removeChild(tr.lastChild);
}
}
_11("td>div",_6b).forEach(function(div,i){
_d.set(div,{"height":_69.sheetHeight+"px"});
_6c.push(div);
},this);
_69.cells=_6c;
},_overlapLayoutPass2:function(_70){
var i,j,_71,_72;
_71=_70[_70.length-1];
for(j=0;j<_71.length;j++){
_71[j].extent=1;
}
for(i=0;i<_70.length-1;i++){
_71=_70[i];
for(var j=0;j<_71.length;j++){
_72=_71[j];
if(_72.extent==-1){
_72.extent=1;
var _73=0;
var _74=false;
for(var k=i+1;k<_70.length&&!_74;k++){
var _75=_70[k];
for(var l=0;l<_75.length&&!_74;l++){
var _76=_75[l];
if(_72.start<_76.end&&_76.start<_72.end){
_74=true;
}
}
if(!_74){
_73++;
}
}
_72.extent+=_73;
}
}
}
},_defaultItemToRendererKindFunc:function(_77){
return "vertical";
},_layoutInterval:function(_78,_79,_7a,end,_7b){
var _7c=[];
_78.colW=this.itemContainer.offsetWidth/_78.columnCount;
for(var i=0;i<_7b.length;i++){
var _7d=_7b[i];
if(this._itemToRendererKind(_7d)=="vertical"){
_7c.push(_7d);
}
}
if(_7c.length>0){
this._layoutVerticalItems(_78,_79,_7a,end,_7c);
}
},_layoutVerticalItems:function(_7e,_7f,_80,_81,_82){
if(this.verticalRenderer==null){
return;
}
var _83=_7e.cells[_7f];
var _84=[];
for(var i=0;i<_82.length;i++){
var _85=_82[i];
var _86=this.computeRangeOverlap(_7e,_85.startTime,_85.endTime,_80,_81);
var top=this.computeProjectionOnDate(_7e,_80,_86[0],_7e.sheetHeight);
var _87=this.computeProjectionOnDate(_7e,_80,_86[1],_7e.sheetHeight);
if(_87>top){
var _88=_7.mixin({start:top,end:_87,range:_86,item:_85},_85);
_84.push(_88);
}
}
var _89=this.computeOverlapping(_84,this._overlapLayoutPass2).numLanes;
var _8a=this.percentOverlap/100;
for(i=0;i<_84.length;i++){
_85=_84[i];
var _8b=_85.lane;
var _8c=_85.extent;
var w;
var _8d;
if(_8a==0){
w=_89==1?_7e.colW:((_7e.colW-(_89-1)*this.horizontalGap)/_89);
_8d=_8b*(w+this.horizontalGap);
w=_8c==1?w:w*_8c+(_8c-1)*this.horizontalGap;
w=100*w/_7e.colW;
_8d=100*_8d/_7e.colW;
}else{
w=_89==1?100:(100/(_89-(_89-1)*_8a));
_8d=_8b*(w-_8a*w);
w=_8c==1?w:w*(_8c-(_8c-1)*_8a);
}
var ir=this._createRenderer(_85,"vertical",this.verticalRenderer,"dojoxCalendarVertical");
_d.set(ir.container,{"top":_85.start+"px","left":_8d+"%","width":w+"%","height":(_85.end-_85.start+1)+"px"});
var _8e=this.isItemBeingEdited(_85);
var _8f=this.isItemSelected(_85);
var _90=this.isItemHovered(_85);
var _91=this.isItemFocused(_85);
var _92=ir.renderer;
_92.set("hovered",_90);
_92.set("selected",_8f);
_92.set("edited",_8e);
_92.set("focused",this.showFocus?_91:false);
_92.set("storeState",this.getItemStoreState(_85));
_92.set("moveEnabled",this.isItemMoveEnabled(_85._item,"vertical"));
_92.set("resizeEnabled",this.isItemResizeEnabled(_85._item,"vertical"));
this.applyRendererZIndex(_85,ir,_90,_8f,_8e,_91);
if(_92.updateRendering){
_92.updateRendering(w,_85.end-_85.start+1);
}
_f.place(ir.container,_83);
_d.set(ir.container,"display","block");
}
},_sortItemsFunction:function(a,b){
var res=this.dateModule.compare(a.startTime,b.startTime);
if(res==0){
res=-1*this.dateModule.compare(a.endTime,b.endTime);
}
return this.isLeftToRight()?res:-res;
},getTime:function(e,x,y,_93){
if(e!=null){
var _94=_e.position(this.itemContainer,true);
if(e.touches){
_93=_93==undefined?0:_93;
x=e.touches[_93].pageX-_94.x;
y=e.touches[_93].pageY-_94.y;
}else{
x=e.pageX-_94.x;
y=e.pageY-_94.y;
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
var col=Math.floor(x/(_e.getMarginBox(this.itemContainer).w/this.renderData.columnCount));
var t=this.getTimeOfDay(y,this.renderData);
var _95=null;
if(col<this.renderData.dates.length){
_95=this.newDate(this.renderData.dates[col]);
_95=this.floorToDay(_95,true);
_95.setHours(t.hours);
_95.setMinutes(t.minutes);
}
return _95;
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
g.scrollTop=this._getScrollPosition();
},_onGridTouchMove:function(e){
this.inherited(arguments);
if(e.touches.length>1&&!this._isEditing){
_6.stop(e);
return;
}
if(this._gridProps&&!this._isEditing){
var _96={x:e.touches[0].screenX,y:e.touches[0].screenY};
var p=this._edProps;
if(!p||p&&(Math.abs(_96.x-p.start.x)>25||Math.abs(_96.y-p.start.y)>25)){
this._gridProps.moved=true;
var d=e.touches[0].screenY-this._gridProps.start;
var _97=this._gridProps.scrollTop-d;
var max=this.itemContainer.offsetHeight-this.scrollContainer.offsetHeight;
if(_97<0){
this._gridProps.start=e.touches[0].screenY;
this._setScrollImpl(0);
this._gridProps.scrollTop=0;
}else{
if(_97>max){
this._gridProps.start=e.touches[0].screenY;
this._setScrollImpl(max);
this._gridProps.scrollTop=max;
}else{
this._setScrollImpl(_97);
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
},getTimeOfDay:function(pos,rd){
var _98=rd.minHours*60;
var _99=rd.maxHours*60;
var _9a=_98+(pos*(_99-_98)/rd.sheetHeight);
return {hours:Math.floor(_9a/60),minutes:Math.floor(_9a%60)};
},_isItemInView:function(_9b){
var res=this.inherited(arguments);
if(res){
var rd=this.renderData;
var len=rd.dateModule.difference(_9b.startTime,_9b.endTime,"millisecond");
var _9c=(24-rd.maxHours+rd.minHours)*3600000;
if(len>_9c){
return true;
}
var _9d=_9b.startTime.getHours()*60+_9b.startTime.getMinutes();
var _9e=_9b.endTime.getHours()*60+_9b.endTime.getMinutes();
var sV=rd.minHours*60;
var eV=rd.maxHours*60;
if(_9d>0&&_9d<sV||_9d>eV&&_9d<=1440){
return false;
}
if(_9e>0&&_9e<sV||_9e>eV&&_9e<=1440){
return false;
}
}
return res;
},_ensureItemInView:function(_9f){
var _a0;
var _a1=_9f.startTime;
var _a2=_9f.endTime;
var rd=this.renderData;
var cal=rd.dateModule;
var len=Math.abs(cal.difference(_9f.startTime,_9f.endTime,"millisecond"));
var _a3=(24-rd.maxHours+rd.minHours)*3600000;
if(len>_a3){
return false;
}
var _a4=_a1.getHours()*60+_a1.getMinutes();
var _a5=_a2.getHours()*60+_a2.getMinutes();
var sV=rd.minHours*60;
var eV=rd.maxHours*60;
if(_a4>0&&_a4<sV){
this.floorToDay(_9f.startTime,true,rd);
_9f.startTime.setHours(rd.minHours);
_9f.endTime=cal.add(_9f.startTime,"millisecond",len);
_a0=true;
}else{
if(_a4>eV&&_a4<=1440){
this.floorToDay(_9f.startTime,true,rd);
_9f.startTime=cal.add(_9f.startTime,"day",1);
_9f.startTime.setHours(rd.minHours);
_9f.endTime=cal.add(_9f.startTime,"millisecond",len);
_a0=true;
}
}
if(_a5>0&&_a5<sV){
this.floorToDay(_9f.endTime,true,rd);
_9f.endTime=cal.add(_9f.endTime,"day",-1);
_9f.endTime.setHours(rd.maxHours);
_9f.startTime=cal.add(_9f.endTime,"millisecond",-len);
_a0=true;
}else{
if(_a5>eV&&_a5<=1440){
this.floorToDay(_9f.endTime,true,rd);
_9f.endTime.setHours(rd.maxHours);
_9f.startTime=cal.add(_9f.endTime,"millisecond",-len);
_a0=true;
}
}
_a0=_a0||this.inherited(arguments);
return _a0;
},_onScrollTimer_tick:function(){
this._scrollToPosition(this._getScrollPosition()+this._scrollProps.scrollStep);
},snapUnit:"minute",snapSteps:15,minDurationUnit:"minute",minDurationSteps:15,liveLayout:false,stayInView:true,allowStartEndSwap:true,allowResizeLessThan24H:true});
});
