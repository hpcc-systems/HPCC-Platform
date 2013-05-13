//>>built
define("dojox/calendar/ViewBase",["dojo/_base/declare","dojo/_base/lang","dojo/_base/array","dojo/_base/window","dojo/_base/event","dojo/_base/html","dojo/sniff","dojo/query","dojo/dom","dojo/dom-style","dojo/dom-construct","dojo/dom-geometry","dojo/on","dojo/date","dojo/date/locale","dijit/_WidgetBase","dojox/widget/_Invalidating","dojox/widget/Selection","dojox/calendar/time","./StoreMixin"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,on,_d,_e,_f,_10,_11,_12,_13){
return _1("dojox.calendar.ViewBase",[_f,_13,_10,_11],{datePackage:_d,_calendar:"gregorian",viewKind:null,_layoutStep:1,_layoutUnit:"day",resizeCursor:"n-resize",formatItemTimeFunc:null,_cssDays:["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],_getFormatItemTimeFuncAttr:function(){
if(this.owner!=null){
return this.owner.get("formatItemTimeFunc");
}
return this.formatItemTimeFunc;
},_viewHandles:null,doubleTapDelay:300,constructor:function(_14){
_14=_14||{};
this._calendar=_14.datePackage?_14.datePackage.substr(_14.datePackage.lastIndexOf(".")+1):this._calendar;
this.dateModule=_14.datePackage?_2.getObject(_14.datePackage,false):_d;
this.dateClassObj=this.dateModule.Date||Date;
this.dateLocaleModule=_14.datePackage?_2.getObject(_14.datePackage+".locale",false):_e;
this.rendererPool=[];
this.rendererList=[];
this.itemToRenderer={};
this._viewHandles=[];
},destroy:function(_15){
while(this.rendererList.length>0){
this._destroyRenderer(this.rendererList.pop());
}
for(var _16 in this._rendererPool){
var _17=this._rendererPool[_16];
if(_17){
while(_17.length>0){
this._destroyRenderer(_17.pop());
}
}
}
while(this._viewHandles.length>0){
this._viewHandles.pop().remove();
}
this.inherited(arguments);
},resize:function(){
},_getTopOwner:function(){
var p=this;
while(p.owner!=undefined){
p=p.owner;
}
return p;
},_createRenderData:function(){
},_validateProperties:function(){
},_setText:function(_18,_19,_1a){
if(_19!=null){
if(!_1a&&_18.hasChildNodes()){
_18.childNodes[0].childNodes[0].nodeValue=_19;
}else{
while(_18.hasChildNodes()){
_18.removeChild(_18.lastChild);
}
var _1b=_4.doc.createElement("span");
if(_7("dojo-bidi")){
this.applyTextDir(_1b,_19);
}
if(_1a){
_1b.innerHTML=_19;
}else{
_1b.appendChild(_4.doc.createTextNode(_19));
}
_18.appendChild(_1b);
}
}
},isAscendantHasClass:function(_1c,_1d,_1e){
while(_1c!=_1d&&_1c!=document){
if(dojo.hasClass(_1c,_1e)){
return true;
}
_1c=_1c.parentNode;
}
return false;
},isWeekEnd:function(_1f){
return _e.isWeekend(_1f);
},getWeekNumberLabel:function(_20){
if(_20.toGregorian){
_20=_20.toGregorian();
}
return _e.format(_20,{selector:"date",datePattern:"w"});
},floorToDay:function(_21,_22){
return _12.floorToDay(_21,_22,this.dateClassObj);
},floorToMonth:function(_23,_24){
return _12.floorToMonth(_23,_24,this.dateClassObj);
},floorDate:function(_25,_26,_27,_28){
return _12.floor(_25,_26,_27,_28,this.dateClassObj);
},isToday:function(_29){
return _12.isToday(_29,this.dateClassObj);
},isStartOfDay:function(d){
return _12.isStartOfDay(d,this.dateClassObj,this.dateModule);
},isOverlapping:function(_2a,_2b,_2c,_2d,_2e,_2f){
if(_2b==null||_2d==null||_2c==null||_2e==null){
return false;
}
var cal=_2a.dateModule;
if(_2f){
if(cal.compare(_2b,_2e)==1||cal.compare(_2d,_2c)==1){
return false;
}
}else{
if(cal.compare(_2b,_2e)!=-1||cal.compare(_2d,_2c)!=-1){
return false;
}
}
return true;
},computeRangeOverlap:function(_30,_31,_32,_33,_34,_35){
var cal=_30.dateModule;
if(_31==null||_33==null||_32==null||_34==null){
return null;
}
var _36=cal.compare(_31,_34);
var _37=cal.compare(_33,_32);
if(_35){
if(_36==0||_36==1||_37==0||_37==1){
return null;
}
}else{
if(_36==1||_37==1){
return null;
}
}
return [this.newDate(cal.compare(_31,_33)>0?_31:_33,_30),this.newDate(cal.compare(_32,_34)>0?_34:_32,_30)];
},isSameDay:function(_38,_39){
if(_38==null||_39==null){
return false;
}
return _38.getFullYear()==_39.getFullYear()&&_38.getMonth()==_39.getMonth()&&_38.getDate()==_39.getDate();
},computeProjectionOnDate:function(_3a,_3b,_3c,max){
var cal=_3a.dateModule;
if(max<=0||cal.compare(_3c,_3b)==-1){
return 0;
}
var _3d=this.floorToDay(_3b,false,_3a);
if(_3c.getDate()!=_3d.getDate()){
if(_3c.getMonth()==_3d.getMonth()){
if(_3c.getDate()<_3d.getDate()){
return 0;
}else{
if(_3c.getDate()>_3d.getDate()){
return max;
}
}
}else{
if(_3c.getFullYear()==_3d.getFullYear()){
if(_3c.getMonth()<_3d.getMonth()){
return 0;
}else{
if(_3c.getMonth()>_3d.getMonth()){
return max;
}
}
}else{
if(_3c.getFullYear()<_3d.getFullYear()){
return 0;
}else{
if(_3c.getFullYear()>_3d.getFullYear()){
return max;
}
}
}
}
}
var res;
if(this.isSameDay(_3b,_3c)){
var d=_2.clone(_3b);
var _3e=0;
if(_3a.minHours!=null&&_3a.minHours!=0){
d.setHours(_3a.minHours);
_3e=d.getHours()*3600+d.getMinutes()*60+d.getSeconds();
}
d=_2.clone(_3b);
var _3f;
if(_3a.maxHours==null||_3a.maxHours==24){
_3f=86400;
}else{
d.setHours(_3a.maxHours);
_3f=d.getHours()*3600+d.getMinutes()*60+d.getSeconds();
}
var _40=_3c.getHours()*3600+_3c.getMinutes()*60+_3c.getSeconds()-_3e;
if(_40<0){
return 0;
}
if(_40>_3f){
return max;
}
res=(max*_40)/(_3f-_3e);
}else{
if(_3c.getDate()<_3b.getDate()&&_3c.getMonth()==_3b.getMonth()){
return 0;
}
var d2=this.floorToDay(_3c);
var dp1=_3a.dateModule.add(_3b,"day",1);
dp1=this.floorToDay(dp1,false,_3a);
if(cal.compare(d2,_3b)==1&&cal.compare(d2,dp1)==0||cal.compare(d2,dp1)==1){
res=max;
}else{
res=0;
}
}
return res;
},getTime:function(e,x,y,_41){
return null;
},newDate:function(obj){
return _12.newDate(obj,this.dateClassObj);
},_isItemInView:function(_42){
var rd=this.renderData;
var cal=rd.dateModule;
if(cal.compare(_42.startTime,rd.startTime)==-1){
return false;
}
return cal.compare(_42.endTime,rd.endTime)!=1;
},_ensureItemInView:function(_43){
var rd=this.renderData;
var cal=rd.dateModule;
var _44=Math.abs(cal.difference(_43.startTime,_43.endTime,"millisecond"));
var _45=false;
if(cal.compare(_43.startTime,rd.startTime)==-1){
_43.startTime=rd.startTime;
_43.endTime=cal.add(_43.startTime,"millisecond",_44);
_45=true;
}else{
if(cal.compare(_43.endTime,rd.endTime)==1){
_43.endTime=rd.endTime;
_43.startTime=cal.add(_43.endTime,"millisecond",-_44);
_45=true;
}
}
return _45;
},scrollable:true,autoScroll:true,_autoScroll:function(gx,gy,_46){
return false;
},scrollMethod:"auto",_setScrollMethodAttr:function(_47){
if(this.scrollMethod!=_47){
this.scrollMethod=_47;
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
},_startAutoScroll:function(_48){
var sp=this._scrollProps;
if(!sp){
sp=this._scrollProps={};
}
sp.scrollStep=_48;
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
var _49=_c.getMarginBox(this.scrollContainer);
var _4a=_c.getMarginBox(this.sheetContainer);
var max=_4a.h-_49.h;
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
},ensureVisibility:function(_4b,end,_4c,_4d,_4e){
},_getStoreAttr:function(){
if(this.owner){
return this.owner.get("store");
}
return this.store;
},_setItemsAttr:function(_4f){
this._set("items",_4f);
this.displayedItemsInvalidated=true;
},_refreshItemsRendering:function(){
var rd=this.renderData;
this._computeVisibleItems(rd);
this._layoutRenderers(rd);
},invalidateLayout:function(){
this._layoutRenderers(this.renderData);
},resize:function(){
},computeOverlapping:function(_50,_51){
if(_50.length==0){
return {numLanes:0,addedPassRes:[1]};
}
var _52=[];
for(var i=0;i<_50.length;i++){
var _53=_50[i];
this._layoutPass1(_53,_52);
}
var _54=null;
if(_51){
_54=_2.hitch(this,_51)(_52);
}
return {numLanes:_52.length,addedPassRes:_54};
},_layoutPass1:function(_55,_56){
var _57=true;
for(var i=0;i<_56.length;i++){
var _58=_56[i];
_57=false;
for(var j=0;j<_58.length&&!_57;j++){
if(_58[j].start<_55.end&&_55.start<_58[j].end){
_57=true;
_58[j].extent=1;
}
}
if(!_57){
_55.lane=i;
_55.extent=-1;
_58.push(_55);
return;
}
}
_56.push([_55]);
_55.lane=_56.length-1;
_55.extent=-1;
},_layoutInterval:function(_59,_5a,_5b,end,_5c){
},layoutPriorityFunction:null,_sortItemsFunction:function(a,b){
var res=this.dateModule.compare(a.startTime,b.startTime);
if(res==0){
res=-1*this.dateModule.compare(a.endTime,b.endTime);
}
return res;
},_layoutRenderers:function(_5d){
if(!_5d.items){
return;
}
this._recycleItemRenderers();
var cal=_5d.dateModule;
var _5e=this.newDate(_5d.startTime);
var _5f=_2.clone(_5e);
var _60;
var _61=_5d.items.concat();
var _62=[],_63;
var _64=0;
while(cal.compare(_5e,_5d.endTime)==-1&&_61.length>0){
_60=cal.add(_5e,this._layoutUnit,this._layoutStep);
_60=this.floorToDay(_60,true,_5d);
var _65=_2.clone(_60);
if(_5d.minHours){
_5f.setHours(_5d.minHours);
}
if(_5d.maxHours&&_5d.maxHours!=24){
_65=cal.add(_60,"day",-1);
_65=this.floorToDay(_65,true,_5d);
_65.setHours(_5d.maxHours);
}
_63=_3.filter(_61,function(_66){
var r=this.isOverlapping(_5d,_66.startTime,_66.endTime,_5f,_65);
if(r){
if(cal.compare(_66.endTime,_65)==1){
_62.push(_66);
}
}else{
_62.push(_66);
}
return r;
},this);
_61=_62;
_62=[];
if(_63.length>0){
_63.sort(_2.hitch(this,this.layoutPriorityFunction?this.layoutPriorityFunction:this._sortItemsFunction));
this._layoutInterval(_5d,_64,_5f,_65,_63);
}
_5e=_60;
_5f=_2.clone(_5e);
_64++;
}
this._onRenderersLayoutDone(this);
},_recycleItemRenderers:function(_67){
while(this.rendererList.length>0){
this._recycleRenderer(this.rendererList.pop(),_67);
}
this.itemToRenderer={};
},rendererPool:null,rendererList:null,itemToRenderer:null,getRenderers:function(_68){
if(_68==null||_68.id==null){
return null;
}
var _69=this.itemToRenderer[_68.id];
return _69==null?null:_69.concat();
},_rendererHandles:{},itemToRendererKindFunc:null,_itemToRendererKind:function(_6a){
if(this.itemToRendererKindFunc){
return this.itemToRendererKindFunc(_6a);
}
return this._defaultItemToRendererKindFunc(_6a);
},_defaultItemToRendererKindFunc:function(_6b){
return null;
},_createRenderer:function(_6c,_6d,_6e,_6f){
if(_6c!=null&&_6d!=null&&_6e!=null){
var res=null,_70=null;
var _71=this.rendererPool[_6d];
if(_71!=null){
res=_71.shift();
}
if(res==null){
_70=new _6e;
var _72=_b.create("div");
_72.className="dojoxCalendarEventContainer "+_6f;
_72.appendChild(_70.domNode);
res={renderer:_70,container:_70.domNode,kind:_6d};
this._onRendererCreated({renderer:res,source:this,item:_6c});
}else{
_70=res.renderer;
this._onRendererReused({renderer:_70,source:this,item:_6c});
}
_70.owner=this;
_70.set("rendererKind",_6d);
_70.set("item",_6c);
var _73=this.itemToRenderer[_6c.id];
if(_73==null){
this.itemToRenderer[_6c.id]=_73=[];
}
_73.push(res);
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
},_onRenderersLayoutDone:function(_74){
this.onRenderersLayoutDone(_74);
if(this.owner!=null){
this.owner._onRenderersLayoutDone(_74);
}
},onRenderersLayoutDone:function(_75){
},_recycleRenderer:function(_76,_77){
this._onRendererRecycled({renderer:_76,source:this});
var _78=this.rendererPool[_76.kind];
if(_78==null){
this.rendererPool[_76.kind]=[_76];
}else{
_78.push(_76);
}
if(_77){
_76.container.parentNode.removeChild(_76.container);
}
_a.set(_76.container,"display","none");
_76.renderer.owner=null;
_76.renderer.set("item",null);
},_destroyRenderer:function(_79){
this._onRendererDestroyed({renderer:_79,source:this});
var ir=_79.renderer;
if(ir["destroy"]){
ir.destroy();
}
_6.destroy(_79.container);
},_destroyRenderersByKind:function(_7a){
var _7b=[];
for(var i=0;i<this.rendererList.length;i++){
var ir=this.rendererList[i];
if(ir.kind==_7a){
this._destroyRenderer(ir);
}else{
_7b.push(ir);
}
}
this.rendererList=_7b;
var _7c=this.rendererPool[_7a];
if(_7c){
while(_7c.length>0){
this._destroyRenderer(_7c.pop());
}
}
},_updateEditingCapabilities:function(_7d,_7e){
var _7f=this.isItemMoveEnabled(_7d,_7e.rendererKind);
var _80=this.isItemResizeEnabled(_7d,_7e.rendererKind);
var _81=false;
if(_7f!=_7e.get("moveEnabled")){
_7e.set("moveEnabled",_7f);
_81=true;
}
if(_80!=_7e.get("resizeEnabled")){
_7e.set("resizeEnabled",_80);
_81=true;
}
if(_81){
_7e.updateRendering();
}
},updateRenderers:function(obj,_82){
if(obj==null){
return;
}
var _83=_2.isArray(obj)?obj:[obj];
for(var i=0;i<_83.length;i++){
var _84=_83[i];
if(_84==null||_84.id==null){
continue;
}
var _85=this.itemToRenderer[_84.id];
if(_85==null){
continue;
}
var _86=this.isItemSelected(_84);
var _87=this.isItemHovered(_84);
var _88=this.isItemBeingEdited(_84);
var _89=this.showFocus?this.isItemFocused(_84):false;
for(var j=0;j<_85.length;j++){
var _8a=_85[j].renderer;
_8a.set("hovered",_87);
_8a.set("selected",_86);
_8a.set("edited",_88);
_8a.set("focused",_89);
_8a.set("storeState",this.getItemStoreState(_84));
this.applyRendererZIndex(_84,_85[j],_87,_86,_88,_89);
if(!_82){
_8a.set("item",_84);
if(_8a.updateRendering){
_8a.updateRendering();
}
}
}
}
},applyRendererZIndex:function(_8b,_8c,_8d,_8e,_8f,_90){
_a.set(_8c.container,{"zIndex":_8f||_8e?20:_8b.lane==undefined?0:_8b.lane});
},getIdentity:function(_91){
return this.owner?this.owner.getIdentity(_91):_91.id;
},_setHoveredItem:function(_92,_93){
if(this.owner){
this.owner._setHoveredItem(_92,_93);
return;
}
if(this.hoveredItem&&_92&&this.hoveredItem.id!=_92.id||_92==null||this.hoveredItem==null){
var old=this.hoveredItem;
this.hoveredItem=_92;
this.updateRenderers([old,this.hoveredItem],true);
if(_92&&_93){
this._updateEditingCapabilities(_92._item?_92._item:_92,_93);
}
}
},hoveredItem:null,isItemHovered:function(_94){
if(this._isEditing&&this._edProps){
return _94.id==this._edProps.editedItem.id;
}
return this.owner?this.owner.isItemHovered(_94):this.hoveredItem!=null&&this.hoveredItem.id==_94.id;
},isItemFocused:function(_95){
return this._isItemFocused?this._isItemFocused(_95):false;
},_setSelectionModeAttr:function(_96){
if(this.owner){
this.owner.set("selectionMode",_96);
}else{
this.inherited(arguments);
}
},_getSelectionModeAttr:function(_97){
if(this.owner){
return this.owner.get("selectionMode");
}
return this.inherited(arguments);
},_setSelectedItemAttr:function(_98){
if(this.owner){
this.owner.set("selectedItem",_98);
}else{
this.inherited(arguments);
}
},_getSelectedItemAttr:function(_99){
if(this.owner){
return this.owner.get("selectedItem");
}
return this.selectedItem;
},_setSelectedItemsAttr:function(_9a){
if(this.owner){
this.owner.set("selectedItems",_9a);
}else{
this.inherited(arguments);
}
},_getSelectedItemsAttr:function(){
if(this.owner){
return this.owner.get("selectedItems");
}
return this.inherited(arguments);
},isItemSelected:function(_9b){
if(this.owner){
return this.owner.isItemSelected(_9b);
}
return this.inherited(arguments);
},selectFromEvent:function(e,_9c,_9d,_9e){
if(this.owner){
this.owner.selectFromEvent(e,_9c,_9d,_9e);
}else{
this.inherited(arguments);
}
},setItemSelected:function(_9f,_a0){
if(this.owner){
this.owner.setItemSelected(_9f,_a0);
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
},_gridMouseDown:false,_onGridMouseDown:function(e){
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
var _a1=this._createdEvent=f(this,this.getTime(e),e);
var _a2=this.get("store");
if(!_a1||_a2==null){
return;
}
var _a3=this.itemToRenderItem(_a1,_a2);
_a3._item=_a1;
this._setItemStoreState(_a1,"unstored");
var _a4=this._getTopOwner();
var _a5=_a4.get("items");
_a4.set("items",_a5?_a5.concat([_a3]):[_a3]);
this._refreshItemsRendering();
var _a6=this.getRenderers(_a1);
if(_a6&&_a6.length>0){
var _a7=_a6[0];
if(_a7){
this._onRendererHandleMouseDown(e,_a7.renderer,"resizeEnd");
}
}
}
},_onGridMouseMove:function(e){
},_onGridMouseUp:function(e){
},_onGridTouchStart:function(e){
var p=this._edProps;
this._gridProps={event:e,fromItem:this.isAscendantHasClass(e.target,this.eventContainer,"dojoxCalendarEventContainer")};
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
},_doEndItemEditing:function(obj,_a8){
if(obj&&obj._isEditing){
var p=obj._edProps;
if(p&&p.endEditingTimer){
clearTimeout(p.endEditingTimer);
p.endEditingTimer=null;
}
obj._endItemEditing(_a8,false);
}
},_onGridTouchEnd:function(e){
},_onGridTouchMove:function(e){
},__fixEvt:function(e){
return e;
},_dispatchCalendarEvt:function(e,_a9){
e=this.__fixEvt(e);
this[_a9](e);
if(this.owner){
this.owner[_a9](e);
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
},_getStartEndRenderers:function(_aa){
var _ab=this.itemToRenderer[_aa.id];
if(_ab==null){
return null;
}
if(_ab.length==1){
var _ac=_ab[0].renderer;
return [_ac,_ac];
}
var rd=this.renderData;
var _ad=false;
var _ae=false;
var res=[];
for(var i=0;i<_ab.length;i++){
var ir=_ab[i].renderer;
if(!_ad){
_ad=rd.dateModule.compare(ir.item.range[0],ir.item.startTime)==0;
res[0]=ir;
}
if(!_ae){
_ae=rd.dateModule.compare(ir.item.range[1],ir.item.endTime)==0;
res[1]=ir;
}
if(_ad&&_ae){
break;
}
}
return res;
},editable:true,moveEnabled:true,resizeEnabled:true,isItemEditable:function(_af,_b0){
return this.getItemStoreState(_af)!="storing"&&this.editable&&(this.owner?this.owner.isItemEditable(_af,_b0):true);
},isItemMoveEnabled:function(_b1,_b2){
return this.isItemEditable(_b1,_b2)&&this.moveEnabled&&(this.owner?this.owner.isItemMoveEnabled(_b1,_b2):true);
},isItemResizeEnabled:function(_b3,_b4){
return this.isItemEditable(_b3,_b4)&&this.resizeEnabled&&(this.owner?this.owner.isItemResizeEnabled(_b3,_b4):true);
},_isEditing:false,isItemBeingEdited:function(_b5){
return this._isEditing&&this._edProps&&this._edProps.editedItem&&this._edProps.editedItem.id==_b5.id;
},_setEditingProperties:function(_b6){
this._edProps=_b6;
},_startItemEditing:function(_b7,_b8){
this._isEditing=true;
this._getTopOwner()._isEditing=true;
var p=this._edProps;
p.editedItem=_b7;
p.storeItem=this.renderItemToItem(_b7,this.get("store"));
p.eventSource=_b8;
p.secItem=this._secondarySheet?this._findRenderItem(_b7.id,this._secondarySheet.renderData.items):null;
p.ownerItem=this.owner?this._findRenderItem(_b7.id,this.items):null;
if(!p.liveLayout){
p.editSaveStartTime=_b7.startTime;
p.editSaveEndTime=_b7.endTime;
p.editItemToRenderer=this.itemToRenderer;
p.editItems=this.renderData.items;
p.editRendererList=this.rendererList;
this.renderData.items=[p.editedItem];
var id=p.editedItem.id;
this.itemToRenderer={};
this.rendererList=[];
var _b9=p.editItemToRenderer[id];
p.editRendererIndices=[];
_3.forEach(_b9,_2.hitch(this,function(ir,i){
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
this._onItemEditBegin({item:_b7,storeItem:p.storeItem,eventSource:_b8});
},_onItemEditBegin:function(e){
this._editStartTimeSave=this.newDate(e.item.startTime);
this._editEndTimeSave=this.newDate(e.item.endTime);
this._dispatchCalendarEvt(e,"onItemEditBegin");
},onItemEditBegin:function(e){
},_endItemEditing:function(_ba,_bb){
this._isEditing=false;
this._getTopOwner()._isEditing=false;
var p=this._edProps;
_3.forEach(p.handles,function(_bc){
_bc.remove();
});
if(!p.liveLayout){
this.renderData.items=p.editItems;
this.rendererList=p.editRendererList.concat(this.rendererList);
_2.mixin(this.itemToRenderer,p.editItemToRenderer);
}
this._onItemEditEnd(_2.mixin(this._createItemEditEvent(),{item:p.editedItem,storeItem:p.storeItem,eventSource:_ba,completed:!_bb}));
this._layoutRenderers(this.renderData);
this._edProps=null;
},_onItemEditEnd:function(e){
this._dispatchCalendarEvt(e,"onItemEditEnd");
if(!e.isDefaultPrevented()){
var _bd=this.get("store");
var _be=this.renderItemToItem(e.item,_bd);
var s=this._getItemStoreStateObj(e.item);
if(s!=null&&s.state=="unstored"){
if(e.completed){
_be=_2.mixin(s.item,_be);
this._setItemStoreState(_be,"storing");
_bd.add(_be);
}else{
var _bf=this._getTopOwner();
var _c0=_bf.get("items");
var l=_c0.length;
for(var i=l-1;i>=0;i--){
if(_c0[i].id==s.id){
_c0.splice(i,1);
break;
}
}
this._setItemStoreState(_be,null);
_bf.set("items",_c0);
}
}else{
if(e.completed){
this._setItemStoreState(_be,"storing");
_bd.put(_be);
}else{
e.item.startTime=this._editStartTimeSave;
e.item.endTime=this._editEndTimeSave;
}
}
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
},_startItemEditingGesture:function(_c1,_c2,_c3,e){
var p=this._edProps;
if(!p||p.editedItem==null){
return;
}
this._editingGesture=true;
var _c4=p.editedItem;
p.editKind=_c2;
this._onItemEditBeginGesture(this.__fixEvt(_2.mixin(this._createItemEditEvent(),{item:_c4,storeItem:p.storeItem,startTime:_c4.startTime,endTime:_c4.endTime,editKind:_c2,rendererKind:p.rendererKind,triggerEvent:e,dates:_c1,eventSource:_c3})));
p.itemBeginDispatched=true;
},_onItemEditBeginGesture:function(e){
var p=this._edProps;
var _c5=p.editedItem;
var _c6=e.dates;
p.editingTimeFrom=[];
p.editingTimeFrom[0]=_c6[0];
p.editingItemRefTime=[];
p.editingItemRefTime[0]=this.newDate(p.editKind=="resizeEnd"?_c5.endTime:_c5.startTime);
if(p.editKind=="resizeBoth"){
p.editingTimeFrom[1]=_c6[1];
p.editingItemRefTime[1]=this.newDate(_c5.endTime);
}
var cal=this.renderData.dateModule;
p.inViewOnce=this._isItemInView(_c5);
if(p.rendererKind=="label"||this.roundToDay){
p._itemEditBeginSave=this.newDate(_c5.startTime);
p._itemEditEndSave=this.newDate(_c5.endTime);
}
p._initDuration=cal.difference(_c5.startTime,_c5.endTime,_c5.allDay?"day":"millisecond");
this._dispatchCalendarEvt(e,"onItemEditBeginGesture");
if(!e.isDefaultPrevented()){
if(e.eventSource=="mouse"){
var _c7=e.editKind=="move"?"move":this.resizeCursor;
p.editLayer=_b.create("div",{style:"position: absolute; left:0; right:0; bottom:0; top:0; z-index:30; tabIndex:-1; background-image:url('"+this._blankGif+"'); cursor: "+_c7,onresizestart:function(e){
return false;
},onselectstart:function(e){
return false;
}},this.domNode);
p.editLayer.focus();
}
}
},onItemEditBeginGesture:function(e){
},_waDojoxAddIssue:function(d,_c8,_c9){
var cal=this.renderData.dateModule;
if(this._calendar!="gregorian"&&_c9<0){
var gd=d.toGregorian();
gd=_d.add(gd,_c8,_c9);
return new this.renderData.dateClassObj(gd);
}else{
return cal.add(d,_c8,_c9);
}
},_computeItemEditingTimes:function(_ca,_cb,_cc,_cd,_ce){
var cal=this.renderData.dateModule;
var p=this._edProps;
var _cf=cal.difference(p.editingTimeFrom[0],_cd[0],"millisecond");
_cd[0]=this._waDojoxAddIssue(p.editingItemRefTime[0],"millisecond",_cf);
if(_cb=="resizeBoth"){
_cf=cal.difference(p.editingTimeFrom[1],_cd[1],"millisecond");
_cd[1]=this._waDojoxAddIssue(p.editingItemRefTime[1],"millisecond",_cf);
}
return _cd;
},_moveOrResizeItemGesture:function(_d0,_d1,e){
if(!this._isEditing||_d0[0]==null){
return;
}
var p=this._edProps;
var _d2=p.editedItem;
var rd=this.renderData;
var cal=rd.dateModule;
var _d3=p.editKind;
var _d4=[_d0[0]];
if(_d3=="resizeBoth"){
_d4[1]=_d0[1];
}
_d4=this._computeItemEditingTimes(_d2,p.editKind,p.rendererKind,_d4,_d1);
var _d5=_d4[0];
var _d6=false;
var _d7=_2.clone(_d2.startTime);
var _d8=_2.clone(_d2.endTime);
var _d9=p.eventSource=="keyboard"?false:this.allowStartEndSwap;
if(_d3=="move"){
if(cal.compare(_d2.startTime,_d5)!=0){
var _da=cal.difference(_d2.startTime,_d2.endTime,"millisecond");
_d2.startTime=this.newDate(_d5);
_d2.endTime=cal.add(_d2.startTime,"millisecond",_da);
_d6=true;
}
}else{
if(_d3=="resizeStart"){
if(cal.compare(_d2.startTime,_d5)!=0){
if(cal.compare(_d2.endTime,_d5)!=-1){
_d2.startTime=this.newDate(_d5);
}else{
if(_d9){
_d2.startTime=this.newDate(_d2.endTime);
_d2.endTime=this.newDate(_d5);
p.editKind=_d3="resizeEnd";
if(_d1=="touch"){
p.resizeEndTouchIndex=p.resizeStartTouchIndex;
p.resizeStartTouchIndex=-1;
}
}else{
_d2.startTime=this.newDate(_d2.endTime);
_d2.startTime.setHours(_d5.getHours());
_d2.startTime.setMinutes(_d5.getMinutes());
_d2.startTime.setSeconds(_d5.getSeconds());
}
}
_d6=true;
}
}else{
if(_d3=="resizeEnd"){
if(cal.compare(_d2.endTime,_d5)!=0){
if(cal.compare(_d2.startTime,_d5)!=1){
_d2.endTime=this.newDate(_d5);
}else{
if(_d9){
_d2.endTime=this.newDate(_d2.startTime);
_d2.startTime=this.newDate(_d5);
p.editKind=_d3="resizeStart";
if(_d1=="touch"){
p.resizeStartTouchIndex=p.resizeEndTouchIndex;
p.resizeEndTouchIndex=-1;
}
}else{
_d2.endTime=this.newDate(_d2.startTime);
_d2.endTime.setHours(_d5.getHours());
_d2.endTime.setMinutes(_d5.getMinutes());
_d2.endTime.setSeconds(_d5.getSeconds());
}
}
_d6=true;
}
}else{
if(_d3=="resizeBoth"){
_d6=true;
var _db=this.newDate(_d5);
var end=this.newDate(_d4[1]);
if(cal.compare(_db,end)!=-1){
if(_d9){
var t=_db;
_db=end;
end=t;
}else{
_d6=false;
}
}
if(_d6){
_d2.startTime=_db;
_d2.endTime=end;
}
}else{
return false;
}
}
}
}
if(!_d6){
return false;
}
var evt=_2.mixin(this._createItemEditEvent(),{item:_d2,storeItem:p.storeItem,startTime:_d2.startTime,endTime:_d2.endTime,editKind:_d3,rendererKind:p.rendererKind,triggerEvent:e,eventSource:_d1});
if(_d3=="move"){
this._onItemEditMoveGesture(evt);
}else{
this._onItemEditResizeGesture(evt);
}
if(cal.compare(_d2.startTime,_d2.endTime)==1){
var tmp=_d2.startTime;
_d2.startTime=_d2.endTime;
_d2.endTime=tmp;
}
_d6=cal.compare(_d7,_d2.startTime)!=0||cal.compare(_d8,_d2.endTime)!=0;
if(!_d6){
return false;
}
this._layoutRenderers(this.renderData);
if(p.liveLayout&&p.secItem!=null){
p.secItem.startTime=_d2.startTime;
p.secItem.endTime=_d2.endTime;
this._secondarySheet._layoutRenderers(this._secondarySheet.renderData);
}else{
if(p.ownerItem!=null&&this.owner.liveLayout){
p.ownerItem.startTime=_d2.startTime;
p.ownerItem.endTime=_d2.endTime;
this.owner._layoutRenderers(this.owner.renderData);
}
}
return true;
},_findRenderItem:function(id,_dc){
_dc=_dc||this.renderData.items;
for(var i=0;i<_dc.length;i++){
if(_dc[i].id==id){
return _dc[i];
}
}
return null;
},_onItemEditMoveGesture:function(e){
this._dispatchCalendarEvt(e,"onItemEditMoveGesture");
if(!e.isDefaultPrevented()){
var p=e.source._edProps;
var rd=this.renderData;
var cal=rd.dateModule;
var _dd,_de;
if(p.rendererKind=="label"||(this.roundToDay&&!e.item.allDay)){
_dd=this.floorToDay(e.item.startTime,false,rd);
_dd.setHours(p._itemEditBeginSave.getHours());
_dd.setMinutes(p._itemEditBeginSave.getMinutes());
_de=cal.add(_dd,"millisecond",p._initDuration);
}else{
if(e.item.allDay){
_dd=this.floorToDay(e.item.startTime,true);
_de=cal.add(_dd,"day",p._initDuration);
}else{
_dd=this.floorDate(e.item.startTime,this.snapUnit,this.snapSteps);
_de=cal.add(_dd,"millisecond",p._initDuration);
}
}
e.item.startTime=_dd;
e.item.endTime=_de;
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
var _df=e.item.startTime;
var _e0=e.item.endTime;
if(e.editKind=="resizeStart"){
if(e.item.allDay){
_df=this.floorToDay(e.item.startTime,false,this.renderData);
}else{
if(this.roundToDay){
_df=this.floorToDay(e.item.startTime,false,rd);
_df.setHours(p._itemEditBeginSave.getHours());
_df.setMinutes(p._itemEditBeginSave.getMinutes());
}else{
_df=this.floorDate(e.item.startTime,this.snapUnit,this.snapSteps);
}
}
}else{
if(e.editKind=="resizeEnd"){
if(e.item.allDay){
if(!this.isStartOfDay(e.item.endTime)){
_e0=this.floorToDay(e.item.endTime,false,this.renderData);
_e0=cal.add(_e0,"day",1);
}
}else{
if(this.roundToDay){
_e0=this.floorToDay(e.item.endTime,false,rd);
_e0.setHours(p._itemEditEndSave.getHours());
_e0.setMinutes(p._itemEditEndSave.getMinutes());
}else{
_e0=this.floorDate(e.item.endTime,this.snapUnit,this.snapSteps);
if(e.eventSource=="mouse"){
_e0=cal.add(_e0,this.snapUnit,this.snapSteps);
}
}
}
}else{
_df=this.floorDate(e.item.startTime,this.snapUnit,this.snapSteps);
_e0=this.floorDate(e.item.endTime,this.snapUnit,this.snapSteps);
_e0=cal.add(_e0,this.snapUnit,this.snapSteps);
}
}
e.item.startTime=_df;
e.item.endTime=_e0;
var _e1=e.item.allDay||p._initDuration>=this._DAY_IN_MILLISECONDS&&!this.allowResizeLessThan24H;
this.ensureMinimalDuration(this.renderData,e.item,_e1?"day":this.minDurationUnit,_e1?1:this.minDurationSteps,e.editKind);
if(!p.inViewOnce){
p.inViewOnce=this._isItemInView(e.item);
}
if(p.inViewOnce&&this.stayInView){
this._ensureItemInView(e.item);
}
}
},onItemEditResizeGesture:function(e){
},_endItemEditingGesture:function(_e2,e){
if(!this._isEditing){
return;
}
this._editingGesture=false;
var p=this._edProps;
var _e3=p.editedItem;
p.itemBeginDispatched=false;
this._onItemEditEndGesture(_2.mixin(this._createItemEditEvent(),{item:_e3,storeItem:p.storeItem,startTime:_e3.startTime,endTime:_e3.endTime,editKind:p.editKind,rendererKind:p.rendererKind,triggerEvent:e,eventSource:_e2}));
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
},ensureMinimalDuration:function(_e4,_e5,_e6,_e7,_e8){
var _e9;
var cal=_e4.dateModule;
if(_e8=="resizeStart"){
_e9=cal.add(_e5.endTime,_e6,-_e7);
if(cal.compare(_e5.startTime,_e9)==1){
_e5.startTime=_e9;
}
}else{
_e9=cal.add(_e5.startTime,_e6,_e7);
if(cal.compare(_e5.endTime,_e9)==-1){
_e5.endTime=_e9;
}
}
},doubleTapDelay:300,snapUnit:"minute",snapSteps:15,minDurationUnit:"hour",minDurationSteps:1,liveLayout:false,stayInView:true,allowStartEndSwap:true,allowResizeLessThan24H:false});
});
