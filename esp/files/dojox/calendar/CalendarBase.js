//>>built
define("dojox/calendar/CalendarBase",["dojo/_base/declare","dojo/_base/sniff","dojo/_base/event","dojo/_base/lang","dojo/_base/array","dojo/cldr/supplemental","dojo/dom","dojo/dom-class","dojo/dom-style","dojo/dom-construct","dojo/date","dojo/date/locale","dojo/_base/fx","dojo/fx","dojo/on","dijit/_WidgetBase","dijit/_TemplatedMixin","dijit/_WidgetsInTemplateMixin","./StoreMixin","dojox/widget/_Invalidating","dojox/widget/Selection","dojox/calendar/time","dojo/i18n!./nls/buttons"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,fx,on,_e,_f,_10,_11,_12,_13,_14,_15){
return _1("dojox.calendar.CalendarBase",[_e,_f,_10,_11,_12,_13],{baseClass:"dojoxCalendar",datePackage:_b,startDate:null,endDate:null,date:null,dateInterval:"week",dateIntervalSteps:1,viewContainer:null,firstDayOfWeek:-1,formatItemTimeFunc:null,editable:true,moveEnabled:true,resizeEnabled:true,columnView:null,matrixView:null,columnViewProps:null,matrixViewProps:null,createOnGridClick:false,createItemFunc:null,currentView:null,_currentViewIndex:-1,views:null,_calendar:"gregorian",constructor:function(_16){
this.views=[];
this.invalidatingProperties=["store","items","startDate","endDate","views","date","dateInterval","dateIntervalSteps","firstDayOfWeek"];
_16=_16||{};
this._calendar=_16.datePackage?_16.datePackage.substr(_16.datePackage.lastIndexOf(".")+1):this._calendar;
this.dateModule=_16.datePackage?_4.getObject(_16.datePackage,false):_b;
this.dateClassObj=this.dateModule.Date||Date;
this.dateLocaleModule=_16.datePackage?_4.getObject(_16.datePackage+".locale",false):_c;
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
},_setStartDateAttr:function(_17){
this._set("startDate",_17);
this._timeRangeInvalidated=true;
},_setEndDateAttr:function(_18){
this._set("endDate",_18);
this._timeRangeInvalidated=true;
},_setDateAttr:function(_19){
this._set("date",_19);
this._timeRangeInvalidated=true;
},_setDateIntervalAttr:function(_1a){
this._set("dateInterval",_1a);
this._timeRangeInvalidated=true;
},_setDateIntervalStepsAttr:function(_1b){
this._set("dateIntervalSteps",_1b);
this._timeRangeInvalidated=true;
},_setFirstDayOfWeekAttr:function(_1c){
this._set("firstDayOfWeek",_1c);
if(this.get("date")!=null&&this.get("dateInterval")=="week"){
this._timeRangeInvalidated=true;
}
},_setTextDirAttr:function(_1d){
_5.forEach(this.views,function(_1e){
_1e.set("textDir",_1d);
});
},refreshRendering:function(){
this.inherited(arguments);
this._validateProperties();
},_refreshItemsRendering:function(){
if(this.currentView){
this.currentView._refreshItemsRendering();
}
},resize:function(){
if(this.currentView){
this.currentView.resize();
}
},_validateProperties:function(){
var cal=this.dateModule;
var _1f=this.get("startDate");
var _20=this.get("endDate");
var _21=this.get("date");
if(this.firstDayOfWeek<-1||this.firstDayOfWeek>6){
this._set("firstDayOfWeek",0);
}
if(_21==null&&(_1f!=null||_20!=null)){
if(_1f==null){
_1f=new this.dateClassObj();
this._set("startDate",_1f);
this._timeRangeInvalidated=true;
}
if(_20==null){
_20=new this.dateClassObj();
this._set("endDate",_20);
this._timeRangeInvalidated=true;
}
if(cal.compare(_1f,_20)>=0){
_20=cal.add(_1f,"day",1);
this._set("endDate",_20);
this._timeRangeInvalidated=true;
}
}else{
if(this.date==null){
this._set("date",new this.dateClassObj());
this._timeRangeInvalidated=true;
}
var _22=this.get("dateInterval");
if(_22!="day"&&_22!="week"&&_22!="month"){
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
var _23=this.computeTimeInterval();
if(this._timeInterval==null||cal.compare(this._timeInterval[0],_23[0]!=0)||cal.compare(this._timeInterval[1],_23[1]!=0)){
this.onTimeIntervalChange({oldStartTime:this._timeInterval==null?null:this._timeInterval[0],oldEndTime:this._timeInterval==null?null:this._timeInterval[1],startTime:_23[0],endTime:_23[1]});
}
this._timeInterval=_23;
var _24=this.dateModule.difference(this._timeInterval[0],this._timeInterval[1],"day");
var _25=this._computeCurrentView(_23[0],_23[1],_24);
var _26=_5.indexOf(this.views,_25);
if(_25==null||_26==-1){
return;
}
if(this.animateRange&&(!_2("ie")||_2("ie")>8)){
if(this.currentView){
var ltr=this.isLeftToRight();
var _27=this._animRangeInDir=="left"||this._animRangeInDir==null;
var _28=this._animRangeOutDir=="left"||this._animRangeOutDir==null;
this._animateRange(this.currentView.domNode,_28&&ltr,false,0,_28?-100:100,_4.hitch(this,function(){
this.animateRangeTimer=setTimeout(_4.hitch(this,function(){
this._applyViewChange(_25,_26,_23,_24);
this._animateRange(this.currentView.domNode,_27&&ltr,true,_27?-100:100,0);
this._animRangeInDir=null;
this._animRangeOutDir=null;
}),100);
}));
}else{
this._applyViewChange(_25,_26,_23,_24);
}
}else{
this._applyViewChange(_25,_26,_23,_24);
}
}
},_applyViewChange:function(_29,_2a,_2b,_2c){
this._configureView(_29,_2a,_2b,_2c);
if(_2a!=this._currentViewIndex){
if(this.currentView==null){
_29.set("items",this.items);
this.set("currentView",_29);
}else{
if(this.items==null||this.items.length==0){
this.set("currentView",_29);
if(this.animateRange&&(!_2("ie")||_2("ie")>8)){
_9.set(this.currentView.domNode,"opacity",0);
}
_29.set("items",this.items);
}else{
this.currentView=_29;
_29.set("items",this.items);
this.set("currentView",_29);
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
},views:null,_setViewsAttr:function(_2d){
if(!this._applyAttr){
for(var i=0;i<this.views.length;i++){
this._onViewRemoved(this.views[i]);
}
}
if(_2d!=null){
for(var i=0;i<_2d.length;i++){
this._onViewAdded(_2d[i]);
}
}
this._set("views",_2d==null?[]:_2d.concat());
},_getViewsAttr:function(){
return this.views.concat();
},_createDefaultViews:function(){
},addView:function(_2e,_2f){
if(_2f<=0||_2f>this.views.length){
_2f=this.views.length;
}
this.views.splice(_2f,_2e);
this._onViewAdded(_2e);
},removeView:function(_30){
if(index<0||index>=this.views.length){
return;
}
this._onViewRemoved(this.views[index]);
this.views.splice(index,1);
},_onViewAdded:function(_31){
_31.owner=this;
_31.buttonContainer=this.buttonContainer;
_31._calendar=this._calendar;
_31.datePackage=this.datePackage;
_31.dateModule=this.dateModule;
_31.dateClassObj=this.dateClassObj;
_31.dateLocaleModule=this.dateLocaleModule;
_9.set(_31.domNode,"display","none");
_8.add(_31.domNode,"view");
_a.place(_31.domNode,this.viewContainer);
this.onViewAdded(_31);
},onViewAdded:function(_32){
},_onViewRemoved:function(_33){
_33.owner=null;
_33.buttonContainer=null;
_8.remove(_33.domNode,"view");
this.viewContainer.removeChild(_33.domNode);
this.onViewRemoved(_33);
},onViewRemoved:function(_34){
},_setCurrentViewAttr:function(_35){
var _36=_5.indexOf(this.views,_35);
if(_36!=-1){
var _37=this.get("currentView");
this._currentViewIndex=_36;
this._set("currentView",_35);
this._showView(_37,_35);
this.onCurrentViewChange({oldView:_37,newView:_35});
}
},_getCurrentViewAttr:function(){
return this.views[this._currentViewIndex];
},onCurrentViewChange:function(e){
},_configureView:function(_38,_39,_3a,_3b){
var cal=this.dateModule;
if(_38.viewKind=="columns"){
_38.set("startDate",_3a[0]);
_38.set("columnCount",_3b);
}else{
if(_38.viewKind=="matrix"){
if(_3b>7){
var s=this.floorToWeek(_3a[0]);
var e=this.floorToWeek(_3a[1]);
if(cal.compare(e,_3a[1])!=0){
e=this.dateModule.add(e,"week",1);
}
_3b=this.dateModule.difference(s,e,"day");
_38.set("startDate",s);
_38.set("columnCount",7);
_38.set("rowCount",Math.ceil(_3b/7));
_38.set("refStartTime",_3a[0]);
_38.set("refEndTime",_3a[1]);
}else{
_38.set("startDate",_3a[0]);
_38.set("columnCount",_3b);
_38.set("rowCount",1);
_38.set("refStartTime",null);
_38.set("refEndTime",null);
}
}
}
},_computeCurrentView:function(_3c,_3d,_3e){
return _3e<=7?this.columnView:this.matrixView;
},matrixViewRowHeaderClick:function(e){
var _3f=this.matrixView.getExpandedRowIndex();
if(_3f==e.index){
this.matrixView.collapseRow();
}else{
if(_3f==-1){
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
},viewChangeDuration:0,_showView:function(_40,_41){
if(_40!=null){
_9.set(_40.domNode,"display","none");
}
if(_41!=null){
_9.set(_41.domNode,"display","block");
_41.resize();
if(!_2("ie")||_2("ie")>7){
_9.set(_41.domNode,"opacity","1");
}
}
},_setItemsAttr:function(_42){
this._set("items",_42);
if(this.currentView){
this.currentView.set("items",_42);
if(!this._isEditing){
this.currentView.invalidateRendering();
}
}
},floorToDay:function(_43,_44){
return _14.floorToDay(_43,_44,this.dateClassObj);
},floorToWeek:function(d){
return _14.floorToWeek(d,this.dateClassObj,this.dateModule,this.firstDayOfWeek,this.locale);
},newDate:function(obj){
return _14.newDate(obj,this.dateClassObj);
},isToday:function(_45){
return _14.isToday(_45,this.dateClassObj);
},isStartOfDay:function(d){
return _14.isStartOfDay(d,this.dateClassObj,this.dateModule);
},floorDate:function(_46,_47,_48,_49){
return _14.floor(_46,_47,_48,_49,this.classFuncObj);
},animateRange:true,animationRangeDuration:400,_animateRange:function(_4a,_4b,_4c,_4d,xTo,_4e){
if(this.animateRangeTimer){
clearTimeout(this.animateRangeTimer);
delete this.animateRangeTimer;
}
var _4f=_4c?_d.fadeIn:_d.fadeOut;
_9.set(_4a,{left:_4d+"px",right:(-_4d)+"px"});
fx.combine([_d.animateProperty({node:_4a,properties:{left:xTo,right:-xTo},duration:this.animationRangeDuration/2,onEnd:_4e}),_4f({node:_4a,duration:this.animationRangeDuration/2})]).play();
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
this.previousButton.set("label",_15[rtl?"nextButton":"previousButton"]);
this.own(on(this.previousButton,"click",_4.hitch(this,rtl?this.nextRange:this.previousRange)));
}
if(this.nextButton){
this.nextButton.set("label",_15[rtl?"previousButton":"nextButton"]);
this.own(on(this.nextButton,"click",_4.hitch(this,rtl?this.previousRange:this.nextRange)));
}
if(rtl&&this.previousButton&&this.nextButton){
var t=this.previousButton;
this.previousButton=this.nextButton;
this.nextButton=t;
}
if(this.todayButton){
this.todayButton.set("label",_15.todayButton);
this.own(on(this.todayButton,"click",_4.hitch(this,this.todayButtonClick)));
}
if(this.dayButton){
this.dayButton.set("label",_15.dayButton);
this.own(on(this.dayButton,"click",_4.hitch(this,this.dayButtonClick)));
}
if(this.weekButton){
this.weekButton.set("label",_15.weekButton);
this.own(on(this.weekButton,"click",_4.hitch(this,this.weekButtonClick)));
}
if(this.fourDaysButton){
this.fourDaysButton.set("label",_15.fourDaysButton);
this.own(on(this.fourDaysButton,"click",_4.hitch(this,this.fourDaysButtonClick)));
}
if(this.monthButton){
this.monthButton.set("label",_15.monthButton);
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
},updateRenderers:function(obj,_50){
if(this.currentView){
this.currentView.updateRenderers(obj,_50);
}
},getIdentity:function(_51){
return _51?_51.id:null;
},_setHoveredItem:function(_52,_53){
if(this.hoveredItem&&_52&&this.hoveredItem.id!=_52.id||_52==null||this.hoveredItem==null){
var old=this.hoveredItem;
this.hoveredItem=_52;
this.updateRenderers([old,this.hoveredItem],true);
if(_52&&_53){
this.currentView._updateEditingCapabilities(_52._item?_52._item:_52,_53);
}
}
},hoveredItem:null,isItemHovered:function(_54){
return this.hoveredItem!=null&&this.hoveredItem.id==_54.id;
},isItemEditable:function(_55,_56){
return this.editable;
},isItemMoveEnabled:function(_57,_58){
return this.isItemEditable(_57,_58)&&this.moveEnabled;
},isItemResizeEnabled:function(_59,_5a){
return this.isItemEditable(_59,_5a)&&this.resizeEnabled;
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
},_onRenderersLayoutDone:function(_5b){
this.onRenderersLayoutDone(_5b);
},onRenderersLayoutDone:function(_5c){
}});
});
