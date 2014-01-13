//>>built
define("dojox/calendar/ViewBase",["dojo/_base/declare","dojo/_base/lang","dojo/_base/array","dojo/_base/window","dojo/_base/event","dojo/_base/html","dojo/sniff","dojo/query","dojo/dom","dojo/dom-style","dojo/dom-class","dojo/dom-construct","dojo/dom-geometry","dojo/on","dojo/date","dojo/date/locale","dojo/when","dijit/_WidgetBase","dojox/widget/_Invalidating","dojox/widget/Selection","dojox/calendar/time","./StoreMixin"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,on,_e,_f,_10,_11,_12,_13,_14,_15){
return _1("dojox.calendar.ViewBase",[_11,_15,_12,_13],{datePackage:_e,_calendar:"gregorian",viewKind:null,_layoutStep:1,_layoutUnit:"day",resizeCursor:"n-resize",formatItemTimeFunc:null,_cssDays:["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],_getFormatItemTimeFuncAttr:function(){
if(this.owner!=null){
return this.owner.get("formatItemTimeFunc");
}
return this.formatItemTimeFunc;
},_viewHandles:null,doubleTapDelay:300,constructor:function(_16){
_16=_16||{};
this._calendar=_16.datePackage?_16.datePackage.substr(_16.datePackage.lastIndexOf(".")+1):this._calendar;
this.dateModule=_16.datePackage?_2.getObject(_16.datePackage,false):_e;
this.dateClassObj=this.dateModule.Date||Date;
this.dateLocaleModule=_16.datePackage?_2.getObject(_16.datePackage+".locale",false):_f;
this.rendererPool=[];
this.rendererList=[];
this.itemToRenderer={};
this._viewHandles=[];
},destroy:function(_17){
while(this.rendererList.length>0){
this._destroyRenderer(this.rendererList.pop());
}
for(var _18 in this._rendererPool){
var _19=this._rendererPool[_18];
if(_19){
while(_19.length>0){
this._destroyRenderer(_19.pop());
}
}
}
while(this._viewHandles.length>0){
this._viewHandles.pop().remove();
}
this.inherited(arguments);
},resize:function(_1a){
if(_1a){
_d.setMarginBox(this.domNode,_1a);
}
},_getTopOwner:function(){
var p=this;
while(p.owner!=undefined){
p=p.owner;
}
return p;
},_createRenderData:function(){
},_validateProperties:function(){
},_setText:function(_1b,_1c,_1d){
if(_1c!=null){
if(!_1d&&_1b.hasChildNodes()){
_1b.childNodes[0].childNodes[0].nodeValue=_1c;
}else{
while(_1b.hasChildNodes()){
_1b.removeChild(_1b.lastChild);
}
var _1e=_4.doc.createElement("span");
if(_7("dojo-bidi")){
this.applyTextDir(_1e,_1c);
}
if(_1d){
_1e.innerHTML=_1c;
}else{
_1e.appendChild(_4.doc.createTextNode(_1c));
}
_1b.appendChild(_1e);
}
}
},isAscendantHasClass:function(_1f,_20,_21){
while(_1f!=_20&&_1f!=document){
if(_b.contains(_1f,_21)){
return true;
}
_1f=_1f.parentNode;
}
return false;
},isWeekEnd:function(_22){
return _f.isWeekend(_22);
},getWeekNumberLabel:function(_23){
if(_23.toGregorian){
_23=_23.toGregorian();
}
return _f.format(_23,{selector:"date",datePattern:"w"});
},floorToDay:function(_24,_25){
return _14.floorToDay(_24,_25,this.dateClassObj);
},floorToMonth:function(_26,_27){
return _14.floorToMonth(_26,_27,this.dateClassObj);
},floorDate:function(_28,_29,_2a,_2b){
return _14.floor(_28,_29,_2a,_2b,this.dateClassObj);
},isToday:function(_2c){
return _14.isToday(_2c,this.dateClassObj);
},isStartOfDay:function(d){
return _14.isStartOfDay(d,this.dateClassObj,this.dateModule);
},isOverlapping:function(_2d,_2e,_2f,_30,_31,_32){
if(_2e==null||_30==null||_2f==null||_31==null){
return false;
}
var cal=_2d.dateModule;
if(_32){
if(cal.compare(_2e,_31)==1||cal.compare(_30,_2f)==1){
return false;
}
}else{
if(cal.compare(_2e,_31)!=-1||cal.compare(_30,_2f)!=-1){
return false;
}
}
return true;
},computeRangeOverlap:function(_33,_34,_35,_36,_37,_38){
var cal=_33.dateModule;
if(_34==null||_36==null||_35==null||_37==null){
return null;
}
var _39=cal.compare(_34,_37);
var _3a=cal.compare(_36,_35);
if(_38){
if(_39==0||_39==1||_3a==0||_3a==1){
return null;
}
}else{
if(_39==1||_3a==1){
return null;
}
}
return [this.newDate(cal.compare(_34,_36)>0?_34:_36,_33),this.newDate(cal.compare(_35,_37)>0?_37:_35,_33)];
},isSameDay:function(_3b,_3c){
if(_3b==null||_3c==null){
return false;
}
return _3b.getFullYear()==_3c.getFullYear()&&_3b.getMonth()==_3c.getMonth()&&_3b.getDate()==_3c.getDate();
},computeProjectionOnDate:function(_3d,_3e,_3f,max){
var cal=_3d.dateModule;
if(max<=0||cal.compare(_3f,_3e)==-1){
return 0;
}
var _40=this.floorToDay(_3e,false,_3d);
if(_3f.getDate()!=_40.getDate()){
if(_3f.getMonth()==_40.getMonth()){
if(_3f.getDate()<_40.getDate()){
return 0;
}else{
if(_3f.getDate()>_40.getDate()){
return max;
}
}
}else{
if(_3f.getFullYear()==_40.getFullYear()){
if(_3f.getMonth()<_40.getMonth()){
return 0;
}else{
if(_3f.getMonth()>_40.getMonth()){
return max;
}
}
}else{
if(_3f.getFullYear()<_40.getFullYear()){
return 0;
}else{
if(_3f.getFullYear()>_40.getFullYear()){
return max;
}
}
}
}
}
var res;
if(this.isSameDay(_3e,_3f)){
var d=_2.clone(_3e);
var _41=0;
if(_3d.minHours!=null&&_3d.minHours!=0){
d.setHours(_3d.minHours);
_41=d.getHours()*3600+d.getMinutes()*60+d.getSeconds();
}
d=_2.clone(_3e);
var _42;
if(_3d.maxHours==null||_3d.maxHours==24){
_42=86400;
}else{
d.setHours(_3d.maxHours);
_42=d.getHours()*3600+d.getMinutes()*60+d.getSeconds();
}
var _43=_3f.getHours()*3600+_3f.getMinutes()*60+_3f.getSeconds()-_41;
if(_43<0){
return 0;
}
if(_43>_42){
return max;
}
res=(max*_43)/(_42-_41);
}else{
if(_3f.getDate()<_3e.getDate()&&_3f.getMonth()==_3e.getMonth()){
return 0;
}
var d2=this.floorToDay(_3f);
var dp1=_3d.dateModule.add(_3e,"day",1);
dp1=this.floorToDay(dp1,false,_3d);
if(cal.compare(d2,_3e)==1&&cal.compare(d2,dp1)==0||cal.compare(d2,dp1)==1){
res=max;
}else{
res=0;
}
}
return res;
},getTime:function(e,x,y,_44){
return null;
},newDate:function(obj){
return _14.newDate(obj,this.dateClassObj);
},_isItemInView:function(_45){
var rd=this.renderData;
var cal=rd.dateModule;
if(cal.compare(_45.startTime,rd.startTime)==-1){
return false;
}
return cal.compare(_45.endTime,rd.endTime)!=1;
},_ensureItemInView:function(_46){
var rd=this.renderData;
var cal=rd.dateModule;
var _47=Math.abs(cal.difference(_46.startTime,_46.endTime,"millisecond"));
var _48=false;
if(cal.compare(_46.startTime,rd.startTime)==-1){
_46.startTime=rd.startTime;
_46.endTime=cal.add(_46.startTime,"millisecond",_47);
_48=true;
}else{
if(cal.compare(_46.endTime,rd.endTime)==1){
_46.endTime=rd.endTime;
_46.startTime=cal.add(_46.endTime,"millisecond",-_47);
_48=true;
}
}
return _48;
},scrollable:true,autoScroll:true,_autoScroll:function(gx,gy,_49){
return false;
},scrollMethod:"auto",_setScrollMethodAttr:function(_4a){
if(this.scrollMethod!=_4a){
this.scrollMethod=_4a;
if(this._domScroll!==undefined){
if(this._domScroll){
_a.set(this.sheetContainer,this._cssPrefix+"transform","translateY(0px)");
}else{
this.scrollContainer.scrollTop=0;
}
}
delete this._domScroll;
var pos=this._getScrollPosition();
delete this._scrollPos;
this._setScrollPosition(pos);
}
},_startAutoScroll:function(_4b){
var sp=this._scrollProps;
if(!sp){
sp=this._scrollProps={};
}
sp.scrollStep=_4b;
if(!sp.isScrolling){
sp.isScrolling=true;
sp.scrollTimer=setInterval(_2.hitch(this,this._onScrollTimer_tick),10);
}
},_stopAutoScroll:function(){
var sp=this._scrollProps;
if(sp&&sp.isScrolling){
clearInterval(sp.scrollTimer);
sp.scrollTimer=null;
}
this._scrollProps=null;
},_onScrollTimer_tick:function(pos){
},_scrollPos:0,getCSSPrefix:function(){
if(_7("ie")){
return "-ms-";
}
if(_7("webkit")){
return "-webkit-";
}
if(_7("mozilla")){
return "-moz-";
}
if(_7("opera")){
return "-o-";
}
return "";
},_setScrollPosition:function(pos){
if(this._scrollPos==pos){
return;
}
if(this._domScroll===undefined){
var sm=this.get("scrollMethod");
if(sm==="auto"){
this._domScroll=!_7("ios")&&!_7("android")&&!_7("webkit");
}else{
this._domScroll=sm==="dom";
}
}
var _4c=_d.getMarginBox(this.scrollContainer);
var _4d=_d.getMarginBox(this.sheetContainer);
var max=_4d.h-_4c.h;
if(pos<0){
pos=0;
}else{
if(pos>max){
pos=max;
}
}
this._scrollPos=pos;
if(this._domScroll){
this.scrollContainer.scrollTop=pos;
}else{
if(!this._cssPrefix){
this._cssPrefix=this.getCSSPrefix();
}
_a.set(this.sheetContainer,this._cssPrefix+"transform","translateY(-"+pos+"px)");
}
},_getScrollPosition:function(){
return this._scrollPos;
},scrollView:function(dir){
},ensureVisibility:function(_4e,end,_4f,_50,_51){
},_getStoreAttr:function(){
if(this.owner){
return this.owner.get("store");
}
return this.store;
},_setItemsAttr:function(_52){
this._set("items",_52);
this.displayedItemsInvalidated=true;
},_refreshItemsRendering:function(){
var rd=this.renderData;
this._computeVisibleItems(rd);
this._layoutRenderers(rd);
},invalidateLayout:function(){
this._layoutRenderers(this.renderData);
},computeOverlapping:function(_53,_54){
if(_53.length==0){
return {numLanes:0,addedPassRes:[1]};
}
var _55=[];
for(var i=0;i<_53.length;i++){
var _56=_53[i];
this._layoutPass1(_56,_55);
}
var _57=null;
if(_54){
_57=_2.hitch(this,_54)(_55);
}
return {numLanes:_55.length,addedPassRes:_57};
},_layoutPass1:function(_58,_59){
var _5a=true;
for(var i=0;i<_59.length;i++){
var _5b=_59[i];
_5a=false;
for(var j=0;j<_5b.length&&!_5a;j++){
if(_5b[j].start<_58.end&&_58.start<_5b[j].end){
_5a=true;
_5b[j].extent=1;
}
}
if(!_5a){
_58.lane=i;
_58.extent=-1;
_5b.push(_58);
return;
}
}
_59.push([_58]);
_58.lane=_59.length-1;
_58.extent=-1;
},_layoutInterval:function(_5c,_5d,_5e,end,_5f){
},layoutPriorityFunction:null,_sortItemsFunction:function(a,b){
var res=this.dateModule.compare(a.startTime,b.startTime);
if(res==0){
res=-1*this.dateModule.compare(a.endTime,b.endTime);
}
return res;
},_layoutRenderers:function(_60){
if(!_60.items){
return;
}
this._recycleItemRenderers();
var cal=_60.dateModule;
var _61=this.newDate(_60.startTime);
var _62=_2.clone(_61);
var _63;
var _64=_60.items.concat();
var _65=[],_66;
var _67=0;
while(cal.compare(_61,_60.endTime)==-1&&_64.length>0){
_63=cal.add(_61,this._layoutUnit,this._layoutStep);
_63=this.floorToDay(_63,true,_60);
var _68=_2.clone(_63);
if(_60.minHours){
_62.setHours(_60.minHours);
}
if(_60.maxHours&&_60.maxHours!=24){
_68=cal.add(_63,"day",-1);
_68=this.floorToDay(_68,true,_60);
_68.setHours(_60.maxHours);
}
_66=_3.filter(_64,function(_69){
var r=this.isOverlapping(_60,_69.startTime,_69.endTime,_62,_68);
if(r){
if(cal.compare(_69.endTime,_68)==1){
_65.push(_69);
}
}else{
_65.push(_69);
}
return r;
},this);
_64=_65;
_65=[];
if(_66.length>0){
_66.sort(_2.hitch(this,this.layoutPriorityFunction?this.layoutPriorityFunction:this._sortItemsFunction));
this._layoutInterval(_60,_67,_62,_68,_66);
}
_61=_63;
_62=_2.clone(_61);
_67++;
}
this._onRenderersLayoutDone(this);
},_recycleItemRenderers:function(_6a){
while(this.rendererList.length>0){
this._recycleRenderer(this.rendererList.pop(),_6a);
}
this.itemToRenderer={};
},rendererPool:null,rendererList:null,itemToRenderer:null,getRenderers:function(_6b){
if(_6b==null||_6b.id==null){
return null;
}
var _6c=this.itemToRenderer[_6b.id];
return _6c==null?null:_6c.concat();
},_rendererHandles:{},itemToRendererKindFunc:null,_itemToRendererKind:function(_6d){
if(this.itemToRendererKindFunc){
return this.itemToRendererKindFunc(_6d);
}
return this._defaultItemToRendererKindFunc(_6d);
},_defaultItemToRendererKindFunc:function(_6e){
return null;
},_createRenderer:function(_6f,_70,_71,_72){
if(_6f!=null&&_70!=null&&_71!=null){
var res=null,_73=null;
var _74=this.rendererPool[_70];
if(_74!=null){
res=_74.shift();
}
if(res==null){
_73=new _71;
res={renderer:_73,container:_73.domNode,kind:_70};
this._onRendererCreated({renderer:res,source:this,item:_6f});
}else{
_73=res.renderer;
this._onRendererReused({renderer:_73,source:this,item:_6f});
}
_73.owner=this;
_73.set("rendererKind",_70);
_73.set("item",_6f);
var _75=this.itemToRenderer[_6f.id];
if(_75==null){
this.itemToRenderer[_6f.id]=_75=[];
}
_75.push(res);
this.rendererList.push(res);
return res;
}
return null;
},_onRendererCreated:function(e){
if(e.source==this){
this.onRendererCreated(e);
}
if(this.owner!=null){
this.owner._onRendererCreated(e);
}
},onRendererCreated:function(e){
},_onRendererRecycled:function(e){
if(e.source==this){
this.onRendererRecycled(e);
}
if(this.owner!=null){
this.owner._onRendererRecycled(e);
}
},onRendererRecycled:function(e){
},_onRendererReused:function(e){
if(e.source==this){
this.onRendererReused(e);
}
if(this.owner!=null){
this.owner._onRendererReused(e);
}
},onRendererReused:function(e){
},_onRendererDestroyed:function(e){
if(e.source==this){
this.onRendererDestroyed(e);
}
if(this.owner!=null){
this.owner._onRendererDestroyed(e);
}
},onRendererDestroyed:function(e){
},_onRenderersLayoutDone:function(_76){
this.onRenderersLayoutDone(_76);
if(this.owner!=null){
this.owner._onRenderersLayoutDone(_76);
}
},onRenderersLayoutDone:function(_77){
},_recycleRenderer:function(_78,_79){
this._onRendererRecycled({renderer:_78,source:this});
var _7a=this.rendererPool[_78.kind];
if(_7a==null){
this.rendererPool[_78.kind]=[_78];
}else{
_7a.push(_78);
}
if(_79){
_78.container.parentNode.removeChild(_78.container);
}
_a.set(_78.container,"display","none");
_78.renderer.owner=null;
_78.renderer.set("item",null);
},_destroyRenderer:function(_7b){
this._onRendererDestroyed({renderer:_7b,source:this});
var ir=_7b.renderer;
if(ir["destroy"]){
ir.destroy();
}
_6.destroy(_7b.container);
},_destroyRenderersByKind:function(_7c){
var _7d=[];
for(var i=0;i<this.rendererList.length;i++){
var ir=this.rendererList[i];
if(ir.kind==_7c){
this._destroyRenderer(ir);
}else{
_7d.push(ir);
}
}
this.rendererList=_7d;
var _7e=this.rendererPool[_7c];
if(_7e){
while(_7e.length>0){
this._destroyRenderer(_7e.pop());
}
}
},_updateEditingCapabilities:function(_7f,_80){
var _81=this.isItemMoveEnabled(_7f,_80.rendererKind);
var _82=this.isItemResizeEnabled(_7f,_80.rendererKind);
var _83=false;
if(_81!=_80.get("moveEnabled")){
_80.set("moveEnabled",_81);
_83=true;
}
if(_82!=_80.get("resizeEnabled")){
_80.set("resizeEnabled",_82);
_83=true;
}
if(_83){
_80.updateRendering();
}
},updateRenderers:function(obj,_84){
if(obj==null){
return;
}
var _85=_2.isArray(obj)?obj:[obj];
for(var i=0;i<_85.length;i++){
var _86=_85[i];
if(_86==null||_86.id==null){
continue;
}
var _87=this.itemToRenderer[_86.id];
if(_87==null){
continue;
}
var _88=this.isItemSelected(_86);
var _89=this.isItemHovered(_86);
var _8a=this.isItemBeingEdited(_86);
var _8b=this.showFocus?this.isItemFocused(_86):false;
for(var j=0;j<_87.length;j++){
var _8c=_87[j].renderer;
_8c.set("hovered",_89);
_8c.set("selected",_88);
_8c.set("edited",_8a);
_8c.set("focused",_8b);
_8c.set("storeState",this.getItemStoreState(_86));
this.applyRendererZIndex(_86,_87[j],_89,_88,_8a,_8b);
if(!_84){
_8c.set("item",_86);
if(_8c.updateRendering){
_8c.updateRendering();
}
}
}
}
},applyRendererZIndex:function(_8d,_8e,_8f,_90,_91,_92){
_a.set(_8e.container,{"zIndex":_91||_90?20:_8d.lane==undefined?0:_8d.lane});
},getIdentity:function(_93){
return this.owner?this.owner.getIdentity(_93):_93.id;
},_setHoveredItem:function(_94,_95){
if(this.owner){
this.owner._setHoveredItem(_94,_95);
return;
}
if(this.hoveredItem&&_94&&this.hoveredItem.id!=_94.id||_94==null||this.hoveredItem==null){
var old=this.hoveredItem;
this.hoveredItem=_94;
this.updateRenderers([old,this.hoveredItem],true);
if(_94&&_95){
this._updateEditingCapabilities(_94._item?_94._item:_94,_95);
}
}
},hoveredItem:null,isItemHovered:function(_96){
if(this._isEditing&&this._edProps){
return _96.id==this._edProps.editedItem.id;
}
return this.owner?this.owner.isItemHovered(_96):this.hoveredItem!=null&&this.hoveredItem.id==_96.id;
},isItemFocused:function(_97){
return this._isItemFocused?this._isItemFocused(_97):false;
},_setSelectionModeAttr:function(_98){
if(this.owner){
this.owner.set("selectionMode",_98);
}else{
this.inherited(arguments);
}
},_getSelectionModeAttr:function(_99){
if(this.owner){
return this.owner.get("selectionMode");
}
return this.inherited(arguments);
},_setSelectedItemAttr:function(_9a){
if(this.owner){
this.owner.set("selectedItem",_9a);
}else{
this.inherited(arguments);
}
},_getSelectedItemAttr:function(_9b){
if(this.owner){
return this.owner.get("selectedItem");
}
return this.selectedItem;
},_setSelectedItemsAttr:function(_9c){
if(this.owner){
this.owner.set("selectedItems",_9c);
}else{
this.inherited(arguments);
}
},_getSelectedItemsAttr:function(){
if(this.owner){
return this.owner.get("selectedItems");
}
return this.inherited(arguments);
},isItemSelected:function(_9d){
if(this.owner){
return this.owner.isItemSelected(_9d);
}
return this.inherited(arguments);
},selectFromEvent:function(e,_9e,_9f,_a0){
if(this.owner){
this.owner.selectFromEvent(e,_9e,_9f,_a0);
}else{
this.inherited(arguments);
}
},setItemSelected:function(_a1,_a2){
if(this.owner){
this.owner.setItemSelected(_a1,_a2);
}else{
this.inherited(arguments);
}
},createItemFunc:null,_getCreateItemFuncAttr:function(){
if(this.owner){
return this.owner.get("createItemFunc");
}
return this.createItemFunc;
},createOnGridClick:false,_getCreateOnGridClickAttr:function(){
if(this.owner){
return this.owner.get("createOnGridClick");
}
return this.createOnGridClick;
},_gridMouseDown:false,_tempIdCount:0,_tempItemsMap:null,_onGridMouseDown:function(e){
this._gridMouseDown=true;
this.showFocus=false;
if(this._isEditing){
this._endItemEditing("mouse",false);
}
this._doEndItemEditing(this.owner,"mouse");
this.set("focusedItem",null);
this.selectFromEvent(e,null,null,true);
if(this._setTabIndexAttr){
this[this._setTabIndexAttr].focus();
}
if(this._onRendererHandleMouseDown){
var f=this.get("createItemFunc");
if(!f){
return;
}
var _a3=this._createdEvent=f(this,this.getTime(e),e);
var _a4=this.get("store");
if(!_a3||_a4==null){
return;
}
if(_a4.getIdentity(_a3)==undefined){
var id="_tempId_"+(this._tempIdCount++);
_a3[_a4.idProperty]=id;
if(this._tempItemsMap==null){
this._tempItemsMap={};
}
this._tempItemsMap[id]=true;
}
var _a5=this.itemToRenderItem(_a3,_a4);
_a5._item=_a3;
this._setItemStoreState(_a3,"unstored");
var _a6=this._getTopOwner();
var _a7=_a6.get("items");
_a6.set("items",_a7?_a7.concat([_a5]):[_a5]);
this._refreshItemsRendering();
var _a8=this.getRenderers(_a3);
if(_a8&&_a8.length>0){
var _a9=_a8[0];
if(_a9){
this._onRendererHandleMouseDown(e,_a9.renderer,"resizeEnd");
this._startItemEditing(_a5,"mouse");
}
}
}
},_onGridMouseMove:function(e){
},_onGridMouseUp:function(e){
},_onGridTouchStart:function(e){
var p=this._edProps;
this._gridProps={event:e,fromItem:this.isAscendantHasClass(e.target,this.eventContainer,"dojoxCalendarEvent")};
if(this._isEditing){
if(this._gridProps){
this._gridProps.editingOnStart=true;
}
_2.mixin(p,this._getTouchesOnRenderers(e,p.editedItem));
if(p.touchesLen==0){
if(p&&p.endEditingTimer){
clearTimeout(p.endEditingTimer);
p.endEditingTimer=null;
}
this._endItemEditing("touch",false);
}
}
this._doEndItemEditing(this.owner,"touch");
_5.stop(e);
},_doEndItemEditing:function(obj,_aa){
if(obj&&obj._isEditing){
var p=obj._edProps;
if(p&&p.endEditingTimer){
clearTimeout(p.endEditingTimer);
p.endEditingTimer=null;
}
obj._endItemEditing(_aa,false);
}
},_onGridTouchEnd:function(e){
},_onGridTouchMove:function(e){
},__fixEvt:function(e){
return e;
},_dispatchCalendarEvt:function(e,_ab){
e=this.__fixEvt(e);
this[_ab](e);
if(this.owner){
this.owner[_ab](e);
}
return e;
},_onGridClick:function(e){
if(!e.triggerEvent){
e={date:this.getTime(e),triggerEvent:e};
}
this._dispatchCalendarEvt(e,"onGridClick");
},onGridClick:function(e){
},_onGridDoubleClick:function(e){
if(!e.triggerEvent){
e={date:this.getTime(e),triggerEvent:e};
}
this._dispatchCalendarEvt(e,"onGridDoubleClick");
},onGridDoubleClick:function(e){
},_onItemClick:function(e){
this._dispatchCalendarEvt(e,"onItemClick");
},onItemClick:function(e){
},_onItemDoubleClick:function(e){
this._dispatchCalendarEvt(e,"onItemDoubleClick");
},onItemDoubleClick:function(e){
},_onItemContextMenu:function(e){
this._dispatchCalendarEvt(e,"onItemContextMenu");
},onItemContextMenu:function(e){
},_getStartEndRenderers:function(_ac){
var _ad=this.itemToRenderer[_ac.id];
if(_ad==null){
return null;
}
if(_ad.length==1){
var _ae=_ad[0].renderer;
return [_ae,_ae];
}
var rd=this.renderData;
var _af=false;
var _b0=false;
var res=[];
for(var i=0;i<_ad.length;i++){
var ir=_ad[i].renderer;
if(!_af){
_af=rd.dateModule.compare(ir.item.range[0],ir.item.startTime)==0;
res[0]=ir;
}
if(!_b0){
_b0=rd.dateModule.compare(ir.item.range[1],ir.item.endTime)==0;
res[1]=ir;
}
if(_af&&_b0){
break;
}
}
return res;
},editable:true,moveEnabled:true,resizeEnabled:true,isItemEditable:function(_b1,_b2){
return this.getItemStoreState(_b1)!="storing"&&this.editable&&(this.owner?this.owner.isItemEditable(_b1,_b2):true);
},isItemMoveEnabled:function(_b3,_b4){
return this.isItemEditable(_b3,_b4)&&this.moveEnabled&&(this.owner?this.owner.isItemMoveEnabled(_b3,_b4):true);
},isItemResizeEnabled:function(_b5,_b6){
return this.isItemEditable(_b5,_b6)&&this.resizeEnabled&&(this.owner?this.owner.isItemResizeEnabled(_b5,_b6):true);
},_isEditing:false,isItemBeingEdited:function(_b7){
return this._isEditing&&this._edProps&&this._edProps.editedItem&&this._edProps.editedItem.id==_b7.id;
},_setEditingProperties:function(_b8){
this._edProps=_b8;
},_startItemEditing:function(_b9,_ba){
this._isEditing=true;
this._getTopOwner()._isEditing=true;
var p=this._edProps;
p.editedItem=_b9;
p.storeItem=_b9._item;
p.eventSource=_ba;
p.secItem=this._secondarySheet?this._findRenderItem(_b9.id,this._secondarySheet.renderData.items):null;
p.ownerItem=this.owner?this._findRenderItem(_b9.id,this.items):null;
if(!p.liveLayout){
p.editSaveStartTime=_b9.startTime;
p.editSaveEndTime=_b9.endTime;
p.editItemToRenderer=this.itemToRenderer;
p.editItems=this.renderData.items;
p.editRendererList=this.rendererList;
this.renderData.items=[p.editedItem];
var id=p.editedItem.id;
this.itemToRenderer={};
this.rendererList=[];
var _bb=p.editItemToRenderer[id];
p.editRendererIndices=[];
_3.forEach(_bb,_2.hitch(this,function(ir,i){
if(this.itemToRenderer[id]==null){
this.itemToRenderer[id]=[ir];
}else{
this.itemToRenderer[id].push(ir);
}
this.rendererList.push(ir);
}));
p.editRendererList=_3.filter(p.editRendererList,function(ir){
return ir!=null&&ir.renderer.item.id!=id;
});
delete p.editItemToRenderer[id];
}
this._layoutRenderers(this.renderData);
this._onItemEditBegin({item:_b9,storeItem:p.storeItem,eventSource:_ba});
},_onItemEditBegin:function(e){
this._editStartTimeSave=this.newDate(e.item.startTime);
this._editEndTimeSave=this.newDate(e.item.endTime);
this._dispatchCalendarEvt(e,"onItemEditBegin");
},onItemEditBegin:function(e){
},_endItemEditing:function(_bc,_bd){
this._isEditing=false;
this._getTopOwner()._isEditing=false;
var p=this._edProps;
_3.forEach(p.handles,function(_be){
_be.remove();
});
if(!p.liveLayout){
this.renderData.items=p.editItems;
this.rendererList=p.editRendererList.concat(this.rendererList);
_2.mixin(this.itemToRenderer,p.editItemToRenderer);
}
this._onItemEditEnd(_2.mixin(this._createItemEditEvent(),{item:p.editedItem,storeItem:p.storeItem,eventSource:_bc,completed:!_bd}));
this._layoutRenderers(this.renderData);
this._edProps=null;
},_onItemEditEnd:function(e){
this._dispatchCalendarEvt(e,"onItemEditEnd");
if(!e.isDefaultPrevented()){
var _bf=this.get("store");
var _c0=this.renderItemToItem(e.item,_bf);
var s=this._getItemStoreStateObj(e.item);
if(s!=null&&s.state=="unstored"){
if(e.completed){
_c0=_2.mixin(s.item,_c0);
this._setItemStoreState(_c0,"storing");
var _c1=_bf.getIdentity(_c0);
var _c2=null;
if(this._tempItemsMap&&this._tempItemsMap[_c1]){
_c2={temporaryId:_c1};
delete this._tempItemsMap[_c1];
delete _c0[_bf.idProperty];
}
_10(_bf.add(_c0,_c2),_2.hitch(this,function(res){
var id;
if(_2.isObject(res)){
id=_bf.getIdentity(res);
}else{
id=res;
}
if(id!=_c1){
this._removeRenderItem(_c1);
}
}));
}else{
this.removeRenderItem(s.id);
}
}else{
if(e.completed){
this._setItemStoreState(_c0,"storing");
_bf.put(_c0);
}else{
e.item.startTime=this._editStartTimeSave;
e.item.endTime=this._editEndTimeSave;
}
}
}
},_removeRenderItem:function(id){
var _c3=this._getTopOwner();
var _c4=_c3.get("items");
var l=_c4.length;
var _c5=false;
for(var i=l-1;i>=0;i--){
if(_c4[i].id==id){
_c4.splice(i,1);
_c5=true;
break;
}
}
this._cleanItemStoreState(id);
if(_c5){
_c3.set("items",_c4);
this.invalidateLayout();
}
},onItemEditEnd:function(e){
},_createItemEditEvent:function(){
var e={cancelable:true,bubbles:false,__defaultPrevent:false};
e.preventDefault=function(){
this.__defaultPrevented=true;
};
e.isDefaultPrevented=function(){
return this.__defaultPrevented;
};
return e;
},_startItemEditingGesture:function(_c6,_c7,_c8,e){
var p=this._edProps;
if(!p||p.editedItem==null){
return;
}
this._editingGesture=true;
var _c9=p.editedItem;
p.editKind=_c7;
this._onItemEditBeginGesture(this.__fixEvt(_2.mixin(this._createItemEditEvent(),{item:_c9,storeItem:p.storeItem,startTime:_c9.startTime,endTime:_c9.endTime,editKind:_c7,rendererKind:p.rendererKind,triggerEvent:e,dates:_c6,eventSource:_c8})));
p.itemBeginDispatched=true;
},_onItemEditBeginGesture:function(e){
var p=this._edProps;
var _ca=p.editedItem;
var _cb=e.dates;
p.editingTimeFrom=[];
p.editingTimeFrom[0]=_cb[0];
p.editingItemRefTime=[];
p.editingItemRefTime[0]=this.newDate(p.editKind=="resizeEnd"?_ca.endTime:_ca.startTime);
if(p.editKind=="resizeBoth"){
p.editingTimeFrom[1]=_cb[1];
p.editingItemRefTime[1]=this.newDate(_ca.endTime);
}
var cal=this.renderData.dateModule;
p.inViewOnce=this._isItemInView(_ca);
if(p.rendererKind=="label"||this.roundToDay){
p._itemEditBeginSave=this.newDate(_ca.startTime);
p._itemEditEndSave=this.newDate(_ca.endTime);
}
p._initDuration=cal.difference(_ca.startTime,_ca.endTime,_ca.allDay?"day":"millisecond");
this._dispatchCalendarEvt(e,"onItemEditBeginGesture");
if(!e.isDefaultPrevented()){
if(e.eventSource=="mouse"){
var _cc=e.editKind=="move"?"move":this.resizeCursor;
p.editLayer=_c.create("div",{style:"position: absolute; left:0; right:0; bottom:0; top:0; z-index:30; tabIndex:-1; background-image:url('"+this._blankGif+"'); cursor: "+_cc,onresizestart:function(e){
return false;
},onselectstart:function(e){
return false;
}},this.domNode);
p.editLayer.focus();
}
}
},onItemEditBeginGesture:function(e){
},_waDojoxAddIssue:function(d,_cd,_ce){
var cal=this.renderData.dateModule;
if(this._calendar!="gregorian"&&_ce<0){
var gd=d.toGregorian();
gd=_e.add(gd,_cd,_ce);
return new this.renderData.dateClassObj(gd);
}else{
return cal.add(d,_cd,_ce);
}
},_computeItemEditingTimes:function(_cf,_d0,_d1,_d2,_d3){
var cal=this.renderData.dateModule;
var p=this._edProps;
var _d4=cal.difference(p.editingTimeFrom[0],_d2[0],"millisecond");
_d2[0]=this._waDojoxAddIssue(p.editingItemRefTime[0],"millisecond",_d4);
if(_d0=="resizeBoth"){
_d4=cal.difference(p.editingTimeFrom[1],_d2[1],"millisecond");
_d2[1]=this._waDojoxAddIssue(p.editingItemRefTime[1],"millisecond",_d4);
}
return _d2;
},_moveOrResizeItemGesture:function(_d5,_d6,e){
if(!this._isEditing||_d5[0]==null){
return;
}
var p=this._edProps;
var _d7=p.editedItem;
var rd=this.renderData;
var cal=rd.dateModule;
var _d8=p.editKind;
var _d9=[_d5[0]];
if(_d8=="resizeBoth"){
_d9[1]=_d5[1];
}
_d9=this._computeItemEditingTimes(_d7,p.editKind,p.rendererKind,_d9,_d6);
var _da=_d9[0];
var _db=false;
var _dc=_2.clone(_d7.startTime);
var _dd=_2.clone(_d7.endTime);
var _de=p.eventSource=="keyboard"?false:this.allowStartEndSwap;
if(_d8=="move"){
if(cal.compare(_d7.startTime,_da)!=0){
var _df=cal.difference(_d7.startTime,_d7.endTime,"millisecond");
_d7.startTime=this.newDate(_da);
_d7.endTime=cal.add(_d7.startTime,"millisecond",_df);
_db=true;
}
}else{
if(_d8=="resizeStart"){
if(cal.compare(_d7.startTime,_da)!=0){
if(cal.compare(_d7.endTime,_da)!=-1){
_d7.startTime=this.newDate(_da);
}else{
if(_de){
_d7.startTime=this.newDate(_d7.endTime);
_d7.endTime=this.newDate(_da);
p.editKind=_d8="resizeEnd";
if(_d6=="touch"){
p.resizeEndTouchIndex=p.resizeStartTouchIndex;
p.resizeStartTouchIndex=-1;
}
}else{
_d7.startTime=this.newDate(_d7.endTime);
_d7.startTime.setHours(_da.getHours());
_d7.startTime.setMinutes(_da.getMinutes());
_d7.startTime.setSeconds(_da.getSeconds());
}
}
_db=true;
}
}else{
if(_d8=="resizeEnd"){
if(cal.compare(_d7.endTime,_da)!=0){
if(cal.compare(_d7.startTime,_da)!=1){
_d7.endTime=this.newDate(_da);
}else{
if(_de){
_d7.endTime=this.newDate(_d7.startTime);
_d7.startTime=this.newDate(_da);
p.editKind=_d8="resizeStart";
if(_d6=="touch"){
p.resizeStartTouchIndex=p.resizeEndTouchIndex;
p.resizeEndTouchIndex=-1;
}
}else{
_d7.endTime=this.newDate(_d7.startTime);
_d7.endTime.setHours(_da.getHours());
_d7.endTime.setMinutes(_da.getMinutes());
_d7.endTime.setSeconds(_da.getSeconds());
}
}
_db=true;
}
}else{
if(_d8=="resizeBoth"){
_db=true;
var _e0=this.newDate(_da);
var end=this.newDate(_d9[1]);
if(cal.compare(_e0,end)!=-1){
if(_de){
var t=_e0;
_e0=end;
end=t;
}else{
_db=false;
}
}
if(_db){
_d7.startTime=_e0;
_d7.endTime=end;
}
}else{
return false;
}
}
}
}
if(!_db){
return false;
}
var evt=_2.mixin(this._createItemEditEvent(),{item:_d7,storeItem:p.storeItem,startTime:_d7.startTime,endTime:_d7.endTime,editKind:_d8,rendererKind:p.rendererKind,triggerEvent:e,eventSource:_d6});
if(_d8=="move"){
this._onItemEditMoveGesture(evt);
}else{
this._onItemEditResizeGesture(evt);
}
if(cal.compare(_d7.startTime,_d7.endTime)==1){
var tmp=_d7.startTime;
_d7.startTime=_d7.endTime;
_d7.endTime=tmp;
}
_db=cal.compare(_dc,_d7.startTime)!=0||cal.compare(_dd,_d7.endTime)!=0;
if(!_db){
return false;
}
this._layoutRenderers(this.renderData);
if(p.liveLayout&&p.secItem!=null){
p.secItem.startTime=_d7.startTime;
p.secItem.endTime=_d7.endTime;
this._secondarySheet._layoutRenderers(this._secondarySheet.renderData);
}else{
if(p.ownerItem!=null&&this.owner.liveLayout){
p.ownerItem.startTime=_d7.startTime;
p.ownerItem.endTime=_d7.endTime;
this.owner._layoutRenderers(this.owner.renderData);
}
}
return true;
},_findRenderItem:function(id,_e1){
_e1=_e1||this.renderData.items;
for(var i=0;i<_e1.length;i++){
if(_e1[i].id==id){
return _e1[i];
}
}
return null;
},_onItemEditMoveGesture:function(e){
this._dispatchCalendarEvt(e,"onItemEditMoveGesture");
if(!e.isDefaultPrevented()){
var p=e.source._edProps;
var rd=this.renderData;
var cal=rd.dateModule;
var _e2,_e3;
if(p.rendererKind=="label"||(this.roundToDay&&!e.item.allDay)){
_e2=this.floorToDay(e.item.startTime,false,rd);
_e2.setHours(p._itemEditBeginSave.getHours());
_e2.setMinutes(p._itemEditBeginSave.getMinutes());
_e3=cal.add(_e2,"millisecond",p._initDuration);
}else{
if(e.item.allDay){
_e2=this.floorToDay(e.item.startTime,true);
_e3=cal.add(_e2,"day",p._initDuration);
}else{
_e2=this.floorDate(e.item.startTime,this.snapUnit,this.snapSteps);
_e3=cal.add(_e2,"millisecond",p._initDuration);
}
}
e.item.startTime=_e2;
e.item.endTime=_e3;
if(!p.inViewOnce){
p.inViewOnce=this._isItemInView(e.item);
}
if(p.inViewOnce&&this.stayInView){
this._ensureItemInView(e.item);
}
}
},_DAY_IN_MILLISECONDS:24*60*60*1000,onItemEditMoveGesture:function(e){
},_onItemEditResizeGesture:function(e){
this._dispatchCalendarEvt(e,"onItemEditResizeGesture");
if(!e.isDefaultPrevented()){
var p=e.source._edProps;
var rd=this.renderData;
var cal=rd.dateModule;
var _e4=e.item.startTime;
var _e5=e.item.endTime;
if(e.editKind=="resizeStart"){
if(e.item.allDay){
_e4=this.floorToDay(e.item.startTime,false,this.renderData);
}else{
if(this.roundToDay){
_e4=this.floorToDay(e.item.startTime,false,rd);
_e4.setHours(p._itemEditBeginSave.getHours());
_e4.setMinutes(p._itemEditBeginSave.getMinutes());
}else{
_e4=this.floorDate(e.item.startTime,this.snapUnit,this.snapSteps);
}
}
}else{
if(e.editKind=="resizeEnd"){
if(e.item.allDay){
if(!this.isStartOfDay(e.item.endTime)){
_e5=this.floorToDay(e.item.endTime,false,this.renderData);
_e5=cal.add(_e5,"day",1);
}
}else{
if(this.roundToDay){
_e5=this.floorToDay(e.item.endTime,false,rd);
_e5.setHours(p._itemEditEndSave.getHours());
_e5.setMinutes(p._itemEditEndSave.getMinutes());
}else{
_e5=this.floorDate(e.item.endTime,this.snapUnit,this.snapSteps);
if(e.eventSource=="mouse"){
_e5=cal.add(_e5,this.snapUnit,this.snapSteps);
}
}
}
}else{
_e4=this.floorDate(e.item.startTime,this.snapUnit,this.snapSteps);
_e5=this.floorDate(e.item.endTime,this.snapUnit,this.snapSteps);
_e5=cal.add(_e5,this.snapUnit,this.snapSteps);
}
}
e.item.startTime=_e4;
e.item.endTime=_e5;
var _e6=e.item.allDay||p._initDuration>=this._DAY_IN_MILLISECONDS&&!this.allowResizeLessThan24H;
this.ensureMinimalDuration(this.renderData,e.item,_e6?"day":this.minDurationUnit,_e6?1:this.minDurationSteps,e.editKind);
if(!p.inViewOnce){
p.inViewOnce=this._isItemInView(e.item);
}
if(p.inViewOnce&&this.stayInView){
this._ensureItemInView(e.item);
}
}
},onItemEditResizeGesture:function(e){
},_endItemEditingGesture:function(_e7,e){
if(!this._isEditing){
return;
}
this._editingGesture=false;
var p=this._edProps;
var _e8=p.editedItem;
p.itemBeginDispatched=false;
this._onItemEditEndGesture(_2.mixin(this._createItemEditEvent(),{item:_e8,storeItem:p.storeItem,startTime:_e8.startTime,endTime:_e8.endTime,editKind:p.editKind,rendererKind:p.rendererKind,triggerEvent:e,eventSource:_e7}));
},_onItemEditEndGesture:function(e){
var p=this._edProps;
delete p._itemEditBeginSave;
delete p._itemEditEndSave;
this._dispatchCalendarEvt(e,"onItemEditEndGesture");
if(!e.isDefaultPrevented()){
if(p.editLayer){
if(_7("ie")){
p.editLayer.style.cursor="default";
}
setTimeout(_2.hitch(this,function(){
if(this.domNode){
this.domNode.focus();
p.editLayer.parentNode.removeChild(p.editLayer);
p.editLayer=null;
}
}),10);
}
}
},onItemEditEndGesture:function(e){
},ensureMinimalDuration:function(_e9,_ea,_eb,_ec,_ed){
var _ee;
var cal=_e9.dateModule;
if(_ed=="resizeStart"){
_ee=cal.add(_ea.endTime,_eb,-_ec);
if(cal.compare(_ea.startTime,_ee)==1){
_ea.startTime=_ee;
}
}else{
_ee=cal.add(_ea.startTime,_eb,_ec);
if(cal.compare(_ea.endTime,_ee)==-1){
_ea.endTime=_ee;
}
}
},doubleTapDelay:300,snapUnit:"minute",snapSteps:15,minDurationUnit:"hour",minDurationSteps:1,liveLayout:false,stayInView:true,allowStartEndSwap:true,allowResizeLessThan24H:false});
});
