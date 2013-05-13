//>>built
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
if(this.displayedItemsInvalidated){
this.displayedItemsInvalidated=false;
this._computeVisibleItems(_16);
if(this._isEditing){
this._endItemEditing(null,false);
}
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
},_setStartTimeOfDayAttr:function(_19){
this._setStartTimeOfDay(_19.hours,_19.minutes,_19.duration,_19.easing);
},_getStartTimeOfDayAttr:function(){
return this._getStartTimeOfDay();
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
},_configureScrollBar:function(_32){
if(_9("ie")&&this.scrollBar){
_d.set(this.scrollBar.domNode,"width",(_32.scrollbarWidth+1)+"px");
}
var _33=this.isLeftToRight()?true:this.scrollBarRTLPosition=="right";
var _34=_33?"right":"left";
var _35=_33?"left":"right";
if(this.scrollBar){
this.scrollBar.set("maximum",_32.sheetHeight);
_d.set(this.scrollBar.domNode,_34,0);
_d.set(this.scrollBar.domNode,_33?"left":"right","auto");
}
_d.set(this.scrollContainer,_34,_32.scrollbarWidth+"px");
_d.set(this.scrollContainer,_35,"0");
_d.set(this.header,_34,_32.scrollbarWidth+"px");
_d.set(this.header,_35,"0");
if(this.buttonContainer&&this.owner!=null&&this.owner.currentView==this){
_d.set(this.buttonContainer,_34,_32.scrollbarWidth+"px");
_d.set(this.buttonContainer,_35,"0");
}
},_columnHeaderClick:function(e){
_6.stop(e);
var _36=_11("td",this.columnHeaderTable).indexOf(e.currentTarget);
this._onColumnHeaderClick({index:_36,date:this.renderData.dates[_36],triggerEvent:e});
},_buildColumnHeader:function(_37,_38){
var _39=this.columnHeaderTable;
if(!_39){
return;
}
var _3a=_37.columnCount-(_38?_38.columnCount:0);
if(_9("ie")==8){
if(this._colTableSave==null){
this._colTableSave=_7.clone(_39);
}else{
if(_3a<0){
this._cleanupColumnHeader();
this.columnHeader.removeChild(_39);
_f.destroy(_39);
_39=_7.clone(this._colTableSave);
this.columnHeaderTable=_39;
this.columnHeader.appendChild(_39);
_3a=_37.columnCount;
}
}
}
var _3b=_11("tbody",_39);
var trs=_11("tr",_39);
var _3c,tr,td;
if(_3b.length==1){
_3c=_3b[0];
}else{
_3c=_a.create("tbody",null,_39);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_f.create("tr",null,_3c);
}
if(_3a>0){
for(var i=0;i<_3a;i++){
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
_3a=-_3a;
for(var i=0;i<_3a;i++){
td=tr.lastChild;
tr.removeChild(td);
_f.destroy(td);
var _3d=this._columnHeaderHandlers.pop();
while(_3d.length>0){
_3d.pop().remove();
}
}
}
_11("td",_39).forEach(function(td,i){
td.className="";
if(i==0){
_c.add(td,"first-child");
}else{
if(i==this.renderData.columnCount-1){
_c.add(td,"last-child");
}
}
var d=_37.dates[i];
this._setText(td,this._formatColumnHeaderLabel(d));
this.styleColumnHeaderCell(td,d,_37);
},this);
if(this.yearColumnHeaderContent){
var d=_37.dates[0];
this._setText(this.yearColumnHeaderContent,_37.dateLocaleModule.format(d,{selector:"date",datePattern:"yyyy"}));
}
},_cleanupColumnHeader:function(){
while(this._columnHeaderHandlers.length>0){
var _3e=this._columnHeaderHandlers.pop();
while(_3e.length>0){
_3e.pop().remove();
}
}
},styleColumnHeaderCell:function(_3f,_40,_41){
_c.add(_3f,this._cssDays[_40.getDay()]);
if(this.isToday(_40)){
_c.add(_3f,"dojoxCalendarToday");
}else{
if(this.isWeekEnd(_40)){
_c.add(_3f,"dojoxCalendarWeekend");
}
}
},_addMinutesClasses:function(_42,_43){
switch(_43){
case 0:
_c.add(_42,"hour");
break;
case 30:
_c.add(_42,"halfhour");
break;
case 15:
case 45:
_c.add(_42,"quarterhour");
break;
}
},_buildRowHeader:function(_44,_45){
var _46=this.rowHeaderTable;
if(!_46){
return;
}
if(this._rowHeaderLabelContainer==null){
this._rowHeaderLabelContainer=_f.create("div",{"class":"dojoxCalendarRowHeaderLabelContainer"},this.rowHeader);
}
_d.set(_46,"height",_44.sheetHeight+"px");
var _47=_11("tbody",_46);
var _48,tr,td;
if(_47.length==1){
_48=_47[0];
}else{
_48=_f.create("tbody",null,_46);
}
var _49=Math.floor(60/_44.rowHeaderGridSlotDuration)*_44.hourCount;
var _4a=_49-(_45?Math.floor(60/_45.rowHeaderGridSlotDuration)*_45.hourCount:0);
if(_4a>0){
for(var i=0;i<_4a;i++){
tr=_f.create("tr",null,_48);
td=_f.create("td",null,tr);
}
}else{
_4a=-_4a;
for(var i=0;i<_4a;i++){
_48.removeChild(_48.lastChild);
}
}
var rd=this.renderData;
var _4b=Math.ceil(_44.hourSize/(60/_44.rowHeaderGridSlotDuration));
var d=new Date(2000,0,1,0,0,0);
_11("tr",_46).forEach(function(tr,i){
var td=_11("td",tr)[0];
td.className="";
_d.set(tr,"height",(_9("ie")==7)?_4b-2*(60/_44.rowHeaderGridSlotDuration):_4b+"px");
this.styleRowHeaderCell(td,d.getHours(),d.getMinutes(),rd);
var m=(i*this.renderData.rowHeaderGridSlotDuration)%60;
this._addMinutesClasses(td,m);
},this);
var lc=this._rowHeaderLabelContainer;
_4a=(Math.floor(60/this.rowHeaderLabelSlotDuration)*_44.hourCount)-lc.childNodes.length;
var _4c;
if(_4a>0){
for(var i=0;i<_4a;i++){
_4c=_f.create("span",null,lc);
_c.add(_4c,"dojoxCalendarRowHeaderLabel");
}
}else{
_4a=-_4a;
for(var i=0;i<_4a;i++){
lc.removeChild(lc.lastChild);
}
}
_4b=Math.ceil(_44.hourSize/(60/this.rowHeaderLabelSlotDuration));
_11(">span",lc).forEach(function(_4d,i){
d.setHours(0);
d.setMinutes(_44.minHours*60+(i*this.rowHeaderLabelSlotDuration));
this._configureRowHeaderLabel(_4d,d,i,_4b*i,rd);
},this);
},_configureRowHeaderLabel:function(_4e,d,_4f,pos,_50){
this._setText(_4e,this._formatRowHeaderLabel(d));
_d.set(_4e,"top",(pos+(_4f==0?this.rowHeaderFirstLabelOffset:this.rowHeaderLabelOffset))+"px");
var m=(_4f*this.rowHeaderLabelSlotDuration)%60;
_c.remove(_4e,["hour","halfhour","quarterhour"]);
this._addMinutesClasses(_4e,m);
},styleRowHeaderCell:function(_51,h,m,_52){
},_buildGrid:function(_53,_54){
var _55=this.gridTable;
if(!_55){
return;
}
_d.set(_55,"height",_53.sheetHeight+"px");
var _56=Math.floor(60/_53.slotDuration)*_53.hourCount;
var _57=_56-(_54?Math.floor(60/_54.slotDuration)*_54.hourCount:0);
var _58=_57>0;
var _59=_53.columnCount-(_54?_54.columnCount:0);
if(_9("ie")==8){
if(this._gridTableSave==null){
this._gridTableSave=_7.clone(_55);
}else{
if(_59<0){
this.grid.removeChild(_55);
_f.destroy(_55);
_55=_7.clone(this._gridTableSave);
this.gridTable=_55;
this.grid.appendChild(_55);
_59=_53.columnCount;
_57=_56;
_58=true;
}
}
}
var _5a=_11("tbody",_55);
var _5b;
if(_5a.length==1){
_5b=_5a[0];
}else{
_5b=_f.create("tbody",null,_55);
}
if(_58){
for(var i=0;i<_57;i++){
_f.create("tr",null,_5b);
}
}else{
_57=-_57;
for(var i=0;i<_57;i++){
_5b.removeChild(_5b.lastChild);
}
}
var _5c=Math.floor(60/_53.slotDuration)*_53.hourCount-_57;
var _5d=_58||_59>0;
_59=_5d?_59:-_59;
_11("tr",_55).forEach(function(tr,i){
if(_5d){
var len=i>=_5c?_53.columnCount:_59;
for(var i=0;i<len;i++){
_f.create("td",null,tr);
}
}else{
for(var i=0;i<_59;i++){
tr.removeChild(tr.lastChild);
}
}
});
_11("tr",_55).forEach(function(tr,i){
_d.set(tr,"height",_53.slotSize+"px");
if(i==0){
_c.add(tr,"first-child");
}else{
if(i==_56-1){
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
var d=_53.dates[col];
this.styleGridCell(td,d,h,m,_53);
this._addMinutesClasses(td,m);
},this);
},this);
},styleGridCellFunc:null,defaultStyleGridCell:function(_5e,_5f,_60,_61,_62){
_c.add(_5e,[this._cssDays[_5f.getDay()],"H"+_60,"M"+_61]);
if(this.isToday(_5f)){
return _c.add(_5e,"dojoxCalendarToday");
}else{
if(this.isWeekEnd(_5f)){
return _c.add(_5e,"dojoxCalendarWeekend");
}
}
},styleGridCell:function(_63,_64,_65,_66,_67){
if(this.styleGridCellFunc){
this.styleGridCellFunc(_63,_64,_65,_66,_67);
}else{
this.defaultStyleGridCell(_63,_64,_65,_66,_67);
}
},_buildItemContainer:function(_68,_69){
var _6a=this.itemContainerTable;
if(!_6a){
return;
}
var _6b=[];
_d.set(_6a,"height",_68.sheetHeight+"px");
var _6c=_68.columnCount-(_69?_69.columnCount:0);
if(_9("ie")==8){
if(this._itemTableSave==null){
this._itemTableSave=_7.clone(_6a);
}else{
if(_6c<0){
this.itemContainer.removeChild(_6a);
this._recycleItemRenderers(true);
_f.destroy(_6a);
_6a=_7.clone(this._itemTableSave);
this.itemContainerTable=_6a;
this.itemContainer.appendChild(_6a);
_6c=_68.columnCount;
}
}
}
var _6d=_11("tbody",_6a);
var trs=_11("tr",_6a);
var _6e,tr,td;
if(_6d.length==1){
_6e=_6d[0];
}else{
_6e=_f.create("tbody",null,_6a);
}
if(trs.length==1){
tr=trs[0];
}else{
tr=_f.create("tr",null,_6e);
}
if(_6c>0){
for(var i=0;i<_6c;i++){
td=_f.create("td",null,tr);
_f.create("div",{"className":"dojoxCalendarContainerColumn"},td);
}
}else{
_6c=-_6c;
for(var i=0;i<_6c;i++){
tr.removeChild(tr.lastChild);
}
}
_11("td>div",_6a).forEach(function(div,i){
_d.set(div,{"height":_68.sheetHeight+"px"});
_6b.push(div);
},this);
_68.cells=_6b;
},_overlapLayoutPass2:function(_6f){
var i,j,_70,_71;
_70=_6f[_6f.length-1];
for(j=0;j<_70.length;j++){
_70[j].extent=1;
}
for(i=0;i<_6f.length-1;i++){
_70=_6f[i];
for(var j=0;j<_70.length;j++){
_71=_70[j];
if(_71.extent==-1){
_71.extent=1;
var _72=0;
var _73=false;
for(var k=i+1;k<_6f.length&&!_73;k++){
var _74=_6f[k];
for(var l=0;l<_74.length&&!_73;l++){
var _75=_74[l];
if(_71.start<_75.end&&_75.start<_71.end){
_73=true;
}
}
if(!_73){
_72++;
}
}
_71.extent+=_72;
}
}
}
},_defaultItemToRendererKindFunc:function(_76){
return "vertical";
},_layoutInterval:function(_77,_78,_79,end,_7a){
var _7b=[];
_77.colW=this.itemContainer.offsetWidth/_77.columnCount;
for(var i=0;i<_7a.length;i++){
var _7c=_7a[i];
if(this._itemToRendererKind(_7c)=="vertical"){
_7b.push(_7c);
}
}
if(_7b.length>0){
this._layoutVerticalItems(_77,_78,_79,end,_7b);
}
},_layoutVerticalItems:function(_7d,_7e,_7f,_80,_81){
if(this.verticalRenderer==null){
return;
}
var _82=_7d.cells[_7e];
var _83=[];
for(var i=0;i<_81.length;i++){
var _84=_81[i];
var _85=this.computeRangeOverlap(_7d,_84.startTime,_84.endTime,_7f,_80);
var top=this.computeProjectionOnDate(_7d,_7f,_85[0],_7d.sheetHeight);
var _86=this.computeProjectionOnDate(_7d,_7f,_85[1],_7d.sheetHeight);
if(_86>top){
var _87=_7.mixin({start:top,end:_86,range:_85,item:_84},_84);
_83.push(_87);
}
}
var _88=this.computeOverlapping(_83,this._overlapLayoutPass2).numLanes;
var _89=this.percentOverlap/100;
for(i=0;i<_83.length;i++){
_84=_83[i];
var _8a=_84.lane;
var _8b=_84.extent;
var w;
var _8c;
if(_89==0){
w=_88==1?_7d.colW:((_7d.colW-(_88-1)*this.horizontalGap)/_88);
_8c=_8a*(w+this.horizontalGap);
w=_8b==1?w:w*_8b+(_8b-1)*this.horizontalGap;
w=100*w/_7d.colW;
_8c=100*_8c/_7d.colW;
}else{
w=_88==1?100:(100/(_88-(_88-1)*_89));
_8c=_8a*(w-_89*w);
w=_8b==1?w:w*(_8b-(_8b-1)*_89);
}
var ir=this._createRenderer(_84,"vertical",this.verticalRenderer,"dojoxCalendarVertical");
_d.set(ir.container,{"top":_84.start+"px","left":_8c+"%","width":w+"%","height":(_84.end-_84.start+1)+"px"});
var _8d=this.isItemBeingEdited(_84);
var _8e=this.isItemSelected(_84);
var _8f=this.isItemHovered(_84);
var _90=this.isItemFocused(_84);
var _91=ir.renderer;
_91.set("hovered",_8f);
_91.set("selected",_8e);
_91.set("edited",_8d);
_91.set("focused",this.showFocus?_90:false);
_91.set("storeState",this.getItemStoreState(_84));
_91.set("moveEnabled",this.isItemMoveEnabled(_84._item,"vertical"));
_91.set("resizeEnabled",this.isItemResizeEnabled(_84._item,"vertical"));
this.applyRendererZIndex(_84,ir,_8f,_8e,_8d,_90);
if(_91.updateRendering){
_91.updateRendering(w,_84.end-_84.start+1);
}
_f.place(ir.container,_82);
_d.set(ir.container,"display","block");
}
},_sortItemsFunction:function(a,b){
var res=this.dateModule.compare(a.startTime,b.startTime);
if(res==0){
res=-1*this.dateModule.compare(a.endTime,b.endTime);
}
return this.isLeftToRight()?res:-res;
},getTime:function(e,x,y,_92){
if(e!=null){
var _93=_e.position(this.itemContainer,true);
if(e.touches){
_92=_92==undefined?0:_92;
x=e.touches[_92].pageX-_93.x;
y=e.touches[_92].pageY-_93.y;
}else{
x=e.pageX-_93.x;
y=e.pageY-_93.y;
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
var _94=null;
if(col<this.renderData.dates.length){
_94=this.newDate(this.renderData.dates[col]);
_94=this.floorToDay(_94,true);
_94.setHours(t.hours);
_94.setMinutes(t.minutes);
}
return _94;
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
var _95={x:e.touches[0].screenX,y:e.touches[0].screenY};
var p=this._edProps;
if(!p||p&&(Math.abs(_95.x-p.start.x)>25||Math.abs(_95.y-p.start.y)>25)){
this._gridProps.moved=true;
var d=e.touches[0].screenY-this._gridProps.start;
var _96=this._gridProps.scrollTop-d;
var max=this.itemContainer.offsetHeight-this.scrollContainer.offsetHeight;
if(_96<0){
this._gridProps.start=e.touches[0].screenY;
this._setScrollImpl(0);
this._gridProps.scrollTop=0;
}else{
if(_96>max){
this._gridProps.start=e.touches[0].screenY;
this._setScrollImpl(max);
this._gridProps.scrollTop=max;
}else{
this._setScrollImpl(_96);
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
var _97=rd.minHours*60;
var _98=rd.maxHours*60;
var _99=_97+(pos*(_98-_97)/rd.sheetHeight);
return {hours:Math.floor(_99/60),minutes:Math.floor(_99%60)};
},_isItemInView:function(_9a){
var res=this.inherited(arguments);
if(res){
var rd=this.renderData;
var len=rd.dateModule.difference(_9a.startTime,_9a.endTime,"millisecond");
var _9b=(24-rd.maxHours+rd.minHours)*3600000;
if(len>_9b){
return true;
}
var _9c=_9a.startTime.getHours()*60+_9a.startTime.getMinutes();
var _9d=_9a.endTime.getHours()*60+_9a.endTime.getMinutes();
var sV=rd.minHours*60;
var eV=rd.maxHours*60;
if(_9c>0&&_9c<sV||_9c>eV&&_9c<=1440){
return false;
}
if(_9d>0&&_9d<sV||_9d>eV&&_9d<=1440){
return false;
}
}
return res;
},_ensureItemInView:function(_9e){
var _9f;
var _a0=_9e.startTime;
var _a1=_9e.endTime;
var rd=this.renderData;
var cal=rd.dateModule;
var len=Math.abs(cal.difference(_9e.startTime,_9e.endTime,"millisecond"));
var _a2=(24-rd.maxHours+rd.minHours)*3600000;
if(len>_a2){
return false;
}
var _a3=_a0.getHours()*60+_a0.getMinutes();
var _a4=_a1.getHours()*60+_a1.getMinutes();
var sV=rd.minHours*60;
var eV=rd.maxHours*60;
if(_a3>0&&_a3<sV){
this.floorToDay(_9e.startTime,true,rd);
_9e.startTime.setHours(rd.minHours);
_9e.endTime=cal.add(_9e.startTime,"millisecond",len);
_9f=true;
}else{
if(_a3>eV&&_a3<=1440){
this.floorToDay(_9e.startTime,true,rd);
_9e.startTime=cal.add(_9e.startTime,"day",1);
_9e.startTime.setHours(rd.minHours);
_9e.endTime=cal.add(_9e.startTime,"millisecond",len);
_9f=true;
}
}
if(_a4>0&&_a4<sV){
this.floorToDay(_9e.endTime,true,rd);
_9e.endTime=cal.add(_9e.endTime,"day",-1);
_9e.endTime.setHours(rd.maxHours);
_9e.startTime=cal.add(_9e.endTime,"millisecond",-len);
_9f=true;
}else{
if(_a4>eV&&_a4<=1440){
this.floorToDay(_9e.endTime,true,rd);
_9e.endTime.setHours(rd.maxHours);
_9e.startTime=cal.add(_9e.endTime,"millisecond",-len);
_9f=true;
}
}
_9f=_9f||this.inherited(arguments);
return _9f;
},_onScrollTimer_tick:function(){
this._scrollToPosition(this._getScrollPosition()+this._scrollProps.scrollStep);
},snapUnit:"minute",snapSteps:15,minDurationUnit:"minute",minDurationSteps:15,liveLayout:false,stayInView:true,allowStartEndSwap:true,allowResizeLessThan24H:true});
});
require({cache:{"url:dojox/calendar/templates/SimpleColumnView.html":"<div data-dojo-attach-events=\"keydown:_onKeyDown\">\t\n\t<div data-dojo-attach-point=\"header\" class=\"dojoxCalendarHeader\">\n\t\t<div class=\"dojoxCalendarYearColumnHeader\" data-dojo-attach-point=\"yearColumnHeader\">\n\t\t\t<table><tr><td><span data-dojo-attach-point=\"yearColumnHeaderContent\"></span></td></tr></table>\t\t\n\t\t</div>\n\t\t<div data-dojo-attach-point=\"columnHeader\" class=\"dojoxCalendarColumnHeader\">\n\t\t\t<table data-dojo-attach-point=\"columnHeaderTable\" class=\"dojoxCalendarColumnHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t\t</div>\n\t</div>\t\n\t<div data-dojo-attach-point=\"vScrollBar\" class=\"dojoxCalendarVScrollBar\">\n\t\t<div data-dojo-attach-point=\"vScrollBarContent\" style=\"visibility:hidden;position:relative; width:1px; height:1px;\" ></div>\n\t</div>\t\n\t<div data-dojo-attach-point=\"scrollContainer\" class=\"dojoxCalendarScrollContainer\">\n\t\t<div data-dojo-attach-point=\"sheetContainer\" style=\"position:relative;left:0;right:0;margin:0;padding:0\">\n\t\t\t<div data-dojo-attach-point=\"rowHeader\" class=\"dojoxCalendarRowHeader\">\n\t\t\t\t<table data-dojo-attach-point=\"rowHeaderTable\" class=\"dojoxCalendarRowHeaderTable\" cellpadding=\"0\" cellspacing=\"0\"></table>\n\t\t\t</div>\n\t\t\t<div data-dojo-attach-point=\"grid\" class=\"dojoxCalendarGrid\">\n\t\t\t\t<table data-dojo-attach-point=\"gridTable\" class=\"dojoxCalendarGridTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t\t\t</div>\n\t\t\t<div data-dojo-attach-point=\"itemContainer\" class=\"dojoxCalendarContainer\" data-dojo-attach-event=\"mousedown:_onGridMouseDown,mouseup:_onGridMouseUp,ondblclick:_onGridDoubleClick,touchstart:_onGridTouchStart,touchmove:_onGridTouchMove,touchend:_onGridTouchEnd\">\n\t\t\t\t<table data-dojo-attach-point=\"itemContainerTable\" class=\"dojoxCalendarContainerTable\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%\"></table>\n\t\t\t</div>\n\t\t</div> \n\t</div>\n</div>\n\n"}});
