//>>built
define("dojox/calendar/CalendarBase",["dojo/_base/declare","dojo/_base/sniff","dojo/_base/event","dojo/_base/lang","dojo/_base/array","dojo/cldr/supplemental","dojo/dom","dojo/dom-class","dojo/dom-style","dojo/dom-construct","dojo/dom-geometry","dojo/date","dojo/date/locale","dojo/_base/fx","dojo/fx","dojo/on","dijit/_WidgetBase","dijit/_TemplatedMixin","dijit/_WidgetsInTemplateMixin","./StoreMixin","dojox/widget/_Invalidating","dojox/widget/Selection","dojox/calendar/time","dojo/i18n!./nls/buttons"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e,fx,on,_f,_10,_11,_12,_13,_14,_15,_16){
return _1("dojox.calendar.CalendarBase",[_f,_10,_11,_12,_13,_14],{baseClass:"dojoxCalendar",datePackage:_c,startDate:null,endDate:null,date:null,dateInterval:"week",dateIntervalSteps:1,viewContainer:null,firstDayOfWeek:-1,formatItemTimeFunc:null,editable:true,moveEnabled:true,resizeEnabled:true,columnView:null,matrixView:null,columnViewProps:null,matrixViewProps:null,createOnGridClick:false,createItemFunc:null,currentView:null,_currentViewIndex:-1,views:null,_calendar:"gregorian",constructor:function(_17){
this.views=[];
this.invalidatingProperties=["store","items","startDate","endDate","views","date","dateInterval","dateIntervalSteps","firstDayOfWeek"];
_17=_17||{};
this._calendar=_17.datePackage?_17.datePackage.substr(_17.datePackage.lastIndexOf(".")+1):this._calendar;
this.dateModule=_17.datePackage?_4.getObject(_17.datePackage,false):_c;
this.dateClassObj=this.dateModule.Date||Date;
this.dateLocaleModule=_17.datePackage?_4.getObject(_17.datePackage+".locale",false):_d;
this.invalidateRendering();
},buildRendering:function(){
this.inherited(arguments);
if(this.views==null||this.views.length==0){
this.set("views",this._createDefaultViews());
}
},_applyAttributes:function(){
this._applyAttr=true;
this.inherited(arguments);
delete this._applyAttr;
},_setStartDateAttr:function(_18){
this._set("startDate",_18);
this._timeRangeInvalidated=true;
},_setEndDateAttr:function(_19){
this._set("endDate",_19);
this._timeRangeInvalidated=true;
},_setDateAttr:function(_1a){
this._set("date",_1a);
this._timeRangeInvalidated=true;
},_setDateIntervalAttr:function(_1b){
this._set("dateInterval",_1b);
this._timeRangeInvalidated=true;
},_setDateIntervalStepsAttr:function(_1c){
this._set("dateIntervalSteps",_1c);
this._timeRangeInvalidated=true;
},_setFirstDayOfWeekAttr:function(_1d){
this._set("firstDayOfWeek",_1d);
if(this.get("date")!=null&&this.get("dateInterval")=="week"){
this._timeRangeInvalidated=true;
}
},_setTextDirAttr:function(_1e){
_5.forEach(this.views,function(_1f){
_1f.set("textDir",_1e);
});
},refreshRendering:function(){
this.inherited(arguments);
this._validateProperties();
},_refreshItemsRendering:function(){
if(this.currentView){
this.currentView._refreshItemsRendering();
}
},resize:function(_20){
if(_20){
_b.setMarginBox(this.domNode,_20);
}
if(this.currentView){
this.currentView.resize();
}
},_validateProperties:function(){
var cal=this.dateModule;
var _21=this.get("startDate");
var _22=this.get("endDate");
var _23=this.get("date");
if(this.firstDayOfWeek<-1||this.firstDayOfWeek>6){
this._set("firstDayOfWeek",0);
}
if(_23==null&&(_21!=null||_22!=null)){
if(_21==null){
_21=new this.dateClassObj();
this._set("startDate",_21);
this._timeRangeInvalidated=true;
}
if(_22==null){
_22=new this.dateClassObj();
this._set("endDate",_22);
this._timeRangeInvalidated=true;
}
if(cal.compare(_21,_22)>=0){
_22=cal.add(_21,"day",1);
this._set("endDate",_22);
this._timeRangeInvalidated=true;
}
}else{
if(this.date==null){
this._set("date",new this.dateClassObj());
this._timeRangeInvalidated=true;
}
var _24=this.get("dateInterval");
if(_24!="day"&&_24!="week"&&_24!="month"){
this._set("dateInterval","day");
this._timeRangeInvalidated=true;
}
var dis=this.get("dateIntervalSteps");
if(_4.isString(dis)){
dis=parseInt(dis);
this._set("dateIntervalSteps",dis);
}
if(dis<=0){
this.set("dateIntervalSteps",1);
this._timeRangeInvalidated=true;
}
}
if(this._timeRangeInvalidated){
this._timeRangeInvalidated=false;
var _25=this.computeTimeInterval();
if(this._timeInterval==null||cal.compare(this._timeInterval[0],_25[0])!=0||cal.compare(this._timeInterval[1],_25[1])!=0){
this.onTimeIntervalChange({oldStartTime:this._timeInterval==null?null:this._timeInterval[0],oldEndTime:this._timeInterval==null?null:this._timeInterval[1],startTime:_25[0],endTime:_25[1]});
}
this._timeInterval=_25;
var _26=this.dateModule.difference(this._timeInterval[0],this._timeInterval[1],"day");
var _27=this._computeCurrentView(_25[0],_25[1],_26);
var _28=_5.indexOf(this.views,_27);
if(_27==null||_28==-1){
return;
}
if(this.animateRange&&(!_2("ie")||_2("ie")>8)){
if(this.currentView){
var ltr=this.isLeftToRight();
var _29=this._animRangeInDir=="left"||this._animRangeInDir==null;
var _2a=this._animRangeOutDir=="left"||this._animRangeOutDir==null;
this._animateRange(this.currentView.domNode,_2a&&ltr,false,0,_2a?-100:100,_4.hitch(this,function(){
this.animateRangeTimer=setTimeout(_4.hitch(this,function(){
this._applyViewChange(_27,_28,_25,_26);
this._animateRange(this.currentView.domNode,_29&&ltr,true,_29?-100:100,0);
this._animRangeInDir=null;
this._animRangeOutDir=null;
}),100);
}));
}else{
this._applyViewChange(_27,_28,_25,_26);
}
}else{
this._applyViewChange(_27,_28,_25,_26);
}
}
},_applyViewChange:function(_2b,_2c,_2d,_2e){
this._configureView(_2b,_2c,_2d,_2e);
if(_2c!=this._currentViewIndex){
if(this.currentView==null){
_2b.set("items",this.items);
this.set("currentView",_2b);
}else{
if(this.items==null||this.items.length==0){
this.set("currentView",_2b);
if(this.animateRange&&(!_2("ie")||_2("ie")>8)){
_9.set(this.currentView.domNode,"opacity",0);
}
_2b.set("items",this.items);
}else{
this.currentView=_2b;
_2b.set("items",this.items);
this.set("currentView",_2b);
if(this.animateRange&&(!_2("ie")||_2("ie")>8)){
_9.set(this.currentView.domNode,"opacity",0);
}
}
}
}
},_timeInterval:null,computeTimeInterval:function(){
var cal=this.dateModule;
var d=this.get("date");
if(d==null){
return [this.floorToDay(this.get("startDate")),cal.add(this.get("endDate"),"day",1)];
}
var s=this.floorToDay(d);
var di=this.get("dateInterval");
var dis=this.get("dateIntervalSteps");
var e;
switch(di){
case "day":
e=cal.add(s,"day",dis);
break;
case "week":
s=this.floorToWeek(s);
e=cal.add(s,"week",dis);
break;
case "month":
s.setDate(1);
e=cal.add(s,"month",dis);
break;
}
return [s,e];
},onTimeIntervalChange:function(e){
},views:null,_setViewsAttr:function(_2f){
if(!this._applyAttr){
for(var i=0;i<this.views.length;i++){
this._onViewRemoved(this.views[i]);
}
}
if(_2f!=null){
for(var i=0;i<_2f.length;i++){
this._onViewAdded(_2f[i]);
}
}
this._set("views",_2f==null?[]:_2f.concat());
},_getViewsAttr:function(){
return this.views.concat();
},_createDefaultViews:function(){
},addView:function(_30,_31){
if(_31<=0||_31>this.views.length){
_31=this.views.length;
}
this.views.splice(_31,_30);
this._onViewAdded(_30);
},removeView:function(_32){
if(index<0||index>=this.views.length){
return;
}
this._onViewRemoved(this.views[index]);
this.views.splice(index,1);
},_onViewAdded:function(_33){
_33.owner=this;
_33.buttonContainer=this.buttonContainer;
_33._calendar=this._calendar;
_33.datePackage=this.datePackage;
_33.dateModule=this.dateModule;
_33.dateClassObj=this.dateClassObj;
_33.dateLocaleModule=this.dateLocaleModule;
_9.set(_33.domNode,"display","none");
_8.add(_33.domNode,"view");
_a.place(_33.domNode,this.viewContainer);
this.onViewAdded(_33);
},onViewAdded:function(_34){
},_onViewRemoved:function(_35){
_35.owner=null;
_35.buttonContainer=null;
_8.remove(_35.domNode,"view");
this.viewContainer.removeChild(_35.domNode);
this.onViewRemoved(_35);
},onViewRemoved:function(_36){
},_setCurrentViewAttr:function(_37){
var _38=_5.indexOf(this.views,_37);
if(_38!=-1){
var _39=this.get("currentView");
this._currentViewIndex=_38;
this._set("currentView",_37);
this._showView(_39,_37);
this.onCurrentViewChange({oldView:_39,newView:_37});
}
},_getCurrentViewAttr:function(){
return this.views[this._currentViewIndex];
},onCurrentViewChange:function(e){
},_configureView:function(_3a,_3b,_3c,_3d){
var cal=this.dateModule;
if(_3a.viewKind=="columns"){
_3a.set("startDate",_3c[0]);
_3a.set("columnCount",_3d);
}else{
if(_3a.viewKind=="matrix"){
if(_3d>7){
var s=this.floorToWeek(_3c[0]);
var e=this.floorToWeek(_3c[1]);
if(cal.compare(e,_3c[1])!=0){
e=this.dateModule.add(e,"week",1);
}
_3d=this.dateModule.difference(s,e,"day");
_3a.set("startDate",s);
_3a.set("columnCount",7);
_3a.set("rowCount",Math.ceil(_3d/7));
_3a.set("refStartTime",_3c[0]);
_3a.set("refEndTime",_3c[1]);
}else{
_3a.set("startDate",_3c[0]);
_3a.set("columnCount",_3d);
_3a.set("rowCount",1);
_3a.set("refStartTime",null);
_3a.set("refEndTime",null);
}
}
}
},_computeCurrentView:function(_3e,_3f,_40){
return _40<=7?this.columnView:this.matrixView;
},matrixViewRowHeaderClick:function(e){
var _41=this.matrixView.getExpandedRowIndex();
if(_41==e.index){
this.matrixView.collapseRow();
}else{
if(_41==-1){
this.matrixView.expandRow(e.index);
}else{
var h=this.matrixView.on("expandAnimationEnd",_4.hitch(this,function(){
h.remove();
this.matrixView.expandRow(e.index);
}));
this.matrixView.collapseRow();
}
}
},columnViewColumnHeaderClick:function(e){
var cal=this.dateModule;
if(cal.compare(e.date,this._timeInterval[0])==0&&this.dateInterval=="day"&&this.dateIntervalSteps==1){
this.set("dateInterval","week");
}else{
this.set("date",e.date);
this.set("dateInterval","day");
this.set("dateIntervalSteps",1);
}
},viewChangeDuration:0,_showView:function(_42,_43){
if(_42!=null){
_9.set(_42.domNode,"display","none");
}
if(_43!=null){
_9.set(_43.domNode,"display","block");
_43.resize();
if(!_2("ie")||_2("ie")>7){
_9.set(_43.domNode,"opacity","1");
}
}
},_setItemsAttr:function(_44){
this._set("items",_44);
if(this.currentView){
this.currentView.set("items",_44);
if(!this._isEditing){
this.currentView.invalidateRendering();
}
}
},floorToDay:function(_45,_46){
return _15.floorToDay(_45,_46,this.dateClassObj);
},floorToWeek:function(d){
return _15.floorToWeek(d,this.dateClassObj,this.dateModule,this.firstDayOfWeek,this.locale);
},newDate:function(obj){
return _15.newDate(obj,this.dateClassObj);
},isToday:function(_47){
return _15.isToday(_47,this.dateClassObj);
},isStartOfDay:function(d){
return _15.isStartOfDay(d,this.dateClassObj,this.dateModule);
},floorDate:function(_48,_49,_4a,_4b){
return _15.floor(_48,_49,_4a,_4b,this.classFuncObj);
},animateRange:true,animationRangeDuration:400,_animateRange:function(_4c,_4d,_4e,_4f,xTo,_50){
if(this.animateRangeTimer){
clearTimeout(this.animateRangeTimer);
delete this.animateRangeTimer;
}
var _51=_4e?_e.fadeIn:_e.fadeOut;
_9.set(_4c,{left:_4f+"px",right:(-_4f)+"px"});
fx.combine([_e.animateProperty({node:_4c,properties:{left:xTo,right:-xTo},duration:this.animationRangeDuration/2,onEnd:_50}),_51({node:_4c,duration:this.animationRangeDuration/2})]).play();
},_animRangeOutDir:null,_animRangeOutDir:null,nextRange:function(){
this._animRangeOutDir="left";
this._animRangeInDir="right";
this._navigate(1);
},previousRange:function(){
this._animRangeOutDir="right";
this._animRangeInDir="left";
this._navigate(-1);
},_navigate:function(dir){
var d=this.get("date");
var cal=this.dateModule;
if(d==null){
var s=this.get("startDate");
var e=this.get("endDate");
var dur=cal.difference(s,e,"day");
if(dir==1){
e=cal.add(e,"day",1);
this.set("startDate",e);
this.set("endDate",cal.add(e,"day",dur));
}else{
s=cal.add(s,"day",-1);
this.set("startDate",cal.add(s,"day",-dur));
this.set("endDate",s);
}
}else{
var di=this.get("dateInterval");
var dis=this.get("dateIntervalSteps");
this.set("date",cal.add(d,di,dir*dis));
}
},goToday:function(){
this.set("date",this.floorToDay(new this.dateClassObj(),true));
this.set("dateInterval","day");
this.set("dateIntervalSteps",1);
},postCreate:function(){
this.inherited(arguments);
this.configureButtons();
},configureButtons:function(){
var rtl=!this.isLeftToRight();
if(this.previousButton){
this.previousButton.set("label",_16[rtl?"nextButton":"previousButton"]);
this.own(on(this.previousButton,"click",_4.hitch(this,this.previousRange)));
}
if(this.nextButton){
this.nextButton.set("label",_16[rtl?"previousButton":"nextButton"]);
this.own(on(this.nextButton,"click",_4.hitch(this,this.nextRange)));
}
if(rtl&&this.previousButton&&this.nextButton){
var t=this.previousButton;
this.previousButton=this.nextButton;
this.nextButton=t;
}
if(this.todayButton){
this.todayButton.set("label",_16.todayButton);
this.own(on(this.todayButton,"click",_4.hitch(this,this.todayButtonClick)));
}
if(this.dayButton){
this.dayButton.set("label",_16.dayButton);
this.own(on(this.dayButton,"click",_4.hitch(this,this.dayButtonClick)));
}
if(this.weekButton){
this.weekButton.set("label",_16.weekButton);
this.own(on(this.weekButton,"click",_4.hitch(this,this.weekButtonClick)));
}
if(this.fourDaysButton){
this.fourDaysButton.set("label",_16.fourDaysButton);
this.own(on(this.fourDaysButton,"click",_4.hitch(this,this.fourDaysButtonClick)));
}
if(this.monthButton){
this.monthButton.set("label",_16.monthButton);
this.own(on(this.monthButton,"click",_4.hitch(this,this.monthButtonClick)));
}
},todayButtonClick:function(e){
this.goToday();
},dayButtonClick:function(e){
if(this.get("date")==null){
this.set("date",this.floorToDay(new this.dateClassObj(),true));
}
this.set("dateInterval","day");
this.set("dateIntervalSteps",1);
},weekButtonClick:function(e){
this.set("dateInterval","week");
this.set("dateIntervalSteps",1);
},fourDaysButtonClick:function(e){
this.set("dateInterval","day");
this.set("dateIntervalSteps",4);
},monthButtonClick:function(e){
this.set("dateInterval","month");
this.set("dateIntervalSteps",1);
},updateRenderers:function(obj,_52){
if(this.currentView){
this.currentView.updateRenderers(obj,_52);
}
},getIdentity:function(_53){
return _53?_53.id:null;
},_setHoveredItem:function(_54,_55){
if(this.hoveredItem&&_54&&this.hoveredItem.id!=_54.id||_54==null||this.hoveredItem==null){
var old=this.hoveredItem;
this.hoveredItem=_54;
this.updateRenderers([old,this.hoveredItem],true);
if(_54&&_55){
this.currentView._updateEditingCapabilities(_54._item?_54._item:_54,_55);
}
}
},hoveredItem:null,isItemHovered:function(_56){
return this.hoveredItem!=null&&this.hoveredItem.id==_56.id;
},isItemEditable:function(_57,_58){
return this.editable;
},isItemMoveEnabled:function(_59,_5a){
return this.isItemEditable(_59,_5a)&&this.moveEnabled;
},isItemResizeEnabled:function(_5b,_5c){
return this.isItemEditable(_5b,_5c)&&this.resizeEnabled;
},onGridClick:function(e){
},onGridDoubleClick:function(e){
},onItemClick:function(e){
},onItemDoubleClick:function(e){
},onItemContextMenu:function(e){
},onItemEditBegin:function(e){
},onItemEditEnd:function(e){
},onItemEditBeginGesture:function(e){
},onItemEditMoveGesture:function(e){
},onItemEditResizeGesture:function(e){
},onItemEditEndGesture:function(e){
},onItemRollOver:function(e){
},onItemRollOut:function(e){
},onColumnHeaderClick:function(e){
},onRowHeaderClick:function(e){
},onExpandRendererClick:function(e){
},_onRendererCreated:function(e){
this.onRendererCreated(e);
},onRendererCreated:function(e){
},_onRendererRecycled:function(e){
this.onRendererRecycled(e);
},onRendererRecycled:function(e){
},_onRendererReused:function(e){
this.onRendererReused(e);
},onRendererReused:function(e){
},_onRendererDestroyed:function(e){
this.onRendererDestroyed(e);
},onRendererDestroyed:function(e){
},_onRenderersLayoutDone:function(_5d){
this.onRenderersLayoutDone(_5d);
},onRenderersLayoutDone:function(_5e){
}});
});
