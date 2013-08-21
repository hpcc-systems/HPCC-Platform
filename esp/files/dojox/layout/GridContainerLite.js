//>>built
require({cache:{"url:dojox/layout/resources/GridContainer.html":"<div id=\"${id}\" class=\"gridContainer\" dojoAttachPoint=\"containerNode\" tabIndex=\"0\" dojoAttachEvent=\"onkeypress:_selectFocus\">\n\t<div dojoAttachPoint=\"gridContainerDiv\">\n\t\t<table class=\"gridContainerTable\" dojoAttachPoint=\"gridContainerTable\" cellspacing=\"0\" cellpadding=\"0\">\n\t\t\t<tbody>\n\t\t\t\t<tr dojoAttachPoint=\"gridNode\" >\n\t\t\t\t\t\n\t\t\t\t</tr>\n\t\t\t</tbody>\n\t\t</table>\n\t</div>\n</div>"}});
define("dojox/layout/GridContainerLite",["dojo/_base/kernel","dojo/text!./resources/GridContainer.html","dojo/_base/declare","dojo/query","dojo/_base/sniff","dojo/dom-class","dojo/dom-style","dojo/dom-geometry","dojo/dom-construct","dojo/dom-attr","dojo/_base/array","dojo/_base/lang","dojo/_base/event","dojo/keys","dojo/topic","dijit/registry","dijit/focus","dijit/_base/focus","dijit/_WidgetBase","dijit/_TemplatedMixin","dijit/layout/_LayoutWidget","dojo/_base/NodeList","dojox/mdnd/AreaManager","dojox/mdnd/DropIndicator","dojox/mdnd/dropMode/OverDropMode","dojox/mdnd/AutoScroll"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e,_f,_10,_11,_12,_13,_14,_15,_16){
var gcl=_3("dojox.layout.GridContainerLite",[_15,_14],{autoRefresh:true,templateString:_2,dragHandleClass:"dojoxDragHandle",nbZones:1,doLayout:true,isAutoOrganized:true,acceptTypes:[],colWidths:"",constructor:function(_17,_18){
this.acceptTypes=(_17||{}).acceptTypes||["text"];
this._disabled=true;
},postCreate:function(){
this.inherited(arguments);
this._grid=[];
this._createCells();
this.subscribe("/dojox/mdnd/drop","resizeChildAfterDrop");
this.subscribe("/dojox/mdnd/drag/start","resizeChildAfterDragStart");
this._dragManager=dojox.mdnd.areaManager();
this._dragManager.autoRefresh=this.autoRefresh;
this._dragManager.dragHandleClass=this.dragHandleClass;
if(this.doLayout){
this._border={h:_5("ie")?_8.getBorderExtents(this.gridContainerTable).h:0,w:(_5("ie")==6)?1:0};
}else{
_7.set(this.domNode,"overflowY","hidden");
_7.set(this.gridContainerTable,"height","auto");
}
},startup:function(){
if(this._started){
return;
}
if(this.isAutoOrganized){
this._organizeChildren();
}else{
this._organizeChildrenManually();
}
_b.forEach(this.getChildren(),function(_19){
_19.startup();
});
if(this._isShown()){
this.enableDnd();
}
this.inherited(arguments);
},resizeChildAfterDrop:function(_1a,_1b,_1c){
if(this._disabled){
return false;
}
if(_10.getEnclosingWidget(_1b.node)==this){
var _1d=_10.byNode(_1a);
if(_1d.resize&&_c.isFunction(_1d.resize)){
_1d.resize();
}
_1d.set("column",_1a.parentNode.cellIndex);
if(this.doLayout){
var _1e=this._contentBox.h,_1f=_8.getContentBox(this.gridContainerDiv).h;
if(_1f>=_1e){
_7.set(this.gridContainerTable,"height",(_1e-this._border.h)+"px");
}
}
return true;
}
return false;
},resizeChildAfterDragStart:function(_20,_21,_22){
if(this._disabled){
return false;
}
if(_10.getEnclosingWidget(_21.node)==this){
this._draggedNode=_20;
if(this.doLayout){
_8.setMarginBox(this.gridContainerTable,{h:_8.getContentBox(this.gridContainerDiv).h-this._border.h});
}
return true;
}
return false;
},getChildren:function(){
var _23=new _16();
_b.forEach(this._grid,function(_24){
_4("> [widgetId]",_24.node).map(_10.byNode).forEach(function(_25){
_23.push(_25);
});
});
return _23;
},_isShown:function(){
if("open" in this){
return this.open;
}else{
var _26=this.domNode;
return (_26.style.display!="none")&&(_26.style.visibility!="hidden")&&!_6.contains(_26,"dijitHidden");
}
},layout:function(){
if(this.doLayout){
var _27=this._contentBox;
_8.setMarginBox(this.gridContainerTable,{h:_27.h-this._border.h});
_8.setContentSize(this.domNode,{w:_27.w-this._border.w});
}
_b.forEach(this.getChildren(),function(_28){
if(_28.resize&&_c.isFunction(_28.resize)){
_28.resize();
}
});
},onShow:function(){
if(this._disabled){
this.enableDnd();
}
},onHide:function(){
if(!this._disabled){
this.disableDnd();
}
},_createCells:function(){
if(this.nbZones===0){
this.nbZones=1;
}
var _29=this.acceptTypes.join(","),i=0;
var _2a=this._computeColWidth();
while(i<this.nbZones){
this._grid.push({node:_9.create("td",{"class":"gridContainerZone",accept:_29,id:this.id+"_dz"+i,style:{"width":_2a[i]+"%"}},this.gridNode)});
i++;
}
},_getZonesAttr:function(){
return _4(".gridContainerZone",this.containerNode);
},enableDnd:function(){
var m=this._dragManager;
_b.forEach(this._grid,function(_2b){
m.registerByNode(_2b.node);
});
m._dropMode.updateAreas(m._areaList);
this._disabled=false;
},disableDnd:function(){
var m=this._dragManager;
_b.forEach(this._grid,function(_2c){
m.unregister(_2c.node);
});
m._dropMode.updateAreas(m._areaList);
this._disabled=true;
},_organizeChildren:function(){
var _2d=dojox.layout.GridContainerLite.superclass.getChildren.call(this);
var _2e=this.nbZones,_2f=Math.floor(_2d.length/_2e),mod=_2d.length%_2e,i=0;
for(var z=0;z<_2e;z++){
for(var r=0;r<_2f;r++){
this._insertChild(_2d[i],z);
i++;
}
if(mod>0){
try{
this._insertChild(_2d[i],z);
i++;
}
catch(e){
console.error("Unable to insert child in GridContainer",e);
}
mod--;
}else{
if(_2f===0){
break;
}
}
}
},_organizeChildrenManually:function(){
var _30=dojox.layout.GridContainerLite.superclass.getChildren.call(this),_31=_30.length,_32;
for(var i=0;i<_31;i++){
_32=_30[i];
try{
this._insertChild(_32,_32.column-1);
}
catch(e){
console.error("Unable to insert child in GridContainer",e);
}
}
},_insertChild:function(_33,_34,p){
var _35=this._grid[_34].node,_36=_35.childNodes.length;
if(typeof p==="undefined"||p>_36){
p=_36;
}
if(this._disabled){
_9.place(_33.domNode,_35,p);
_a.set(_33.domNode,"tabIndex","0");
}else{
if(!_33.dragRestriction){
this._dragManager.addDragItem(_35,_33.domNode,p,true);
}else{
_9.place(_33.domNode,_35,p);
_a.set(_33.domNode,"tabIndex","0");
}
}
_33.set("column",_34);
return _33;
},removeChild:function(_37){
if(this._disabled){
this.inherited(arguments);
}else{
this._dragManager.removeDragItem(_37.domNode.parentNode,_37.domNode);
}
},addService:function(_38,_39,p){
kernel.deprecated("addService is deprecated.","Please use  instead.","Future");
this.addChild(_38,_39,p);
},addChild:function(_3a,_3b,p){
_3a.domNode.id=_3a.id;
dojox.layout.GridContainerLite.superclass.addChild.call(this,_3a,0);
if(_3b<0||_3b===undefined){
_3b=0;
}
if(p<=0){
p=0;
}
try{
return this._insertChild(_3a,_3b,p);
}
catch(e){
console.error("Unable to insert child in GridContainer",e);
}
return null;
},_setColWidthsAttr:function(_3c){
this.colWidths=_c.isString(_3c)?_3c.split(","):(_c.isArray(_3c)?_3c:[_3c]);
if(this._started){
this._updateColumnsWidth();
}
},_updateColumnsWidth:function(_3d){
var _3e=this._grid.length;
var _3f=this._computeColWidth();
for(var i=0;i<_3e;i++){
this._grid[i].node.style.width=_3f[i]+"%";
}
},_computeColWidth:function(){
var _40=this.colWidths||[];
var _41=[];
var _42;
var _43=0;
var i;
for(i=0;i<this.nbZones;i++){
if(_41.length<_40.length){
_43+=_40[i]*1;
_41.push(_40[i]);
}else{
if(!_42){
_42=(100-_43)/(this.nbZones-i);
if(_42<0){
_42=100/this.nbZones;
}
}
_41.push(_42);
_43+=_42*1;
}
}
if(_43>100){
var _44=100/_43;
for(i=0;i<_41.length;i++){
_41[i]*=_44;
}
}
return _41;
},_selectFocus:function(_45){
if(this._disabled){
return;
}
var key=_45.keyCode,k=_e,_46=null,_47=_12.getFocus(),_48=_47.node,m=this._dragManager,_49,i,j,r,_4a,_4b,_4c;
if(_48==this.containerNode){
_4b=this.gridNode.childNodes;
switch(key){
case k.DOWN_ARROW:
case k.RIGHT_ARROW:
_49=false;
for(i=0;i<_4b.length;i++){
_4a=_4b[i].childNodes;
for(j=0;j<_4a.length;j++){
_46=_4a[j];
if(_46!==null&&_46.style.display!="none"){
_11.focus(_46);
_d.stop(_45);
_49=true;
break;
}
}
if(_49){
break;
}
}
break;
case k.UP_ARROW:
case k.LEFT_ARROW:
_4b=this.gridNode.childNodes;
_49=false;
for(i=_4b.length-1;i>=0;i--){
_4a=_4b[i].childNodes;
for(j=_4a.length;j>=0;j--){
_46=_4a[j];
if(_46!==null&&_46.style.display!="none"){
_11.focus(_46);
_d.stop(_45);
_49=true;
break;
}
}
if(_49){
break;
}
}
break;
}
}else{
if(_48.parentNode.parentNode==this.gridNode){
var _4d=(key==k.UP_ARROW||key==k.LEFT_ARROW)?"lastChild":"firstChild";
var pos=(key==k.UP_ARROW||key==k.LEFT_ARROW)?"previousSibling":"nextSibling";
switch(key){
case k.UP_ARROW:
case k.DOWN_ARROW:
_d.stop(_45);
_49=false;
var _4e=_48;
while(!_49){
_4a=_4e.parentNode.childNodes;
var num=0;
for(i=0;i<_4a.length;i++){
if(_4a[i].style.display!="none"){
num++;
}
if(num>1){
break;
}
}
if(num==1){
return;
}
if(_4e[pos]===null){
_46=_4e.parentNode[_4d];
}else{
_46=_4e[pos];
}
if(_46.style.display==="none"){
_4e=_46;
}else{
_49=true;
}
}
if(_45.shiftKey){
var _4f=_48.parentNode;
for(i=0;i<this.gridNode.childNodes.length;i++){
if(_4f==this.gridNode.childNodes[i]){
break;
}
}
_4a=this.gridNode.childNodes[i].childNodes;
for(j=0;j<_4a.length;j++){
if(_46==_4a[j]){
break;
}
}
if(_5("mozilla")||_5("webkit")){
i--;
}
_4c=_10.byNode(_48);
if(!_4c.dragRestriction){
r=m.removeDragItem(_4f,_48);
this.addChild(_4c,i,j);
_a.set(_48,"tabIndex","0");
_11.focus(_48);
}else{
_f.publish("/dojox/layout/gridContainer/moveRestriction",this);
}
}else{
_11.focus(_46);
}
break;
case k.RIGHT_ARROW:
case k.LEFT_ARROW:
_d.stop(_45);
if(_45.shiftKey){
var z=0;
if(_48.parentNode[pos]===null){
if(_5("ie")&&key==k.LEFT_ARROW){
z=this.gridNode.childNodes.length-1;
}
}else{
if(_48.parentNode[pos].nodeType==3){
z=this.gridNode.childNodes.length-2;
}else{
for(i=0;i<this.gridNode.childNodes.length;i++){
if(_48.parentNode[pos]==this.gridNode.childNodes[i]){
break;
}
z++;
}
if(_5("mozilla")||_5("webkit")){
z--;
}
}
}
_4c=_10.byNode(_48);
var _50=_48.getAttribute("dndtype");
if(_50===null){
if(_4c&&_4c.dndType){
_50=_4c.dndType.split(/\s*,\s*/);
}else{
_50=["text"];
}
}else{
_50=_50.split(/\s*,\s*/);
}
var _51=false;
for(i=0;i<this.acceptTypes.length;i++){
for(j=0;j<_50.length;j++){
if(_50[j]==this.acceptTypes[i]){
_51=true;
break;
}
}
}
if(_51&&!_4c.dragRestriction){
var _52=_48.parentNode,_53=0;
if(k.LEFT_ARROW==key){
var t=z;
if(_5("mozilla")||_5("webkit")){
t=z+1;
}
_53=this.gridNode.childNodes[t].childNodes.length;
}
r=m.removeDragItem(_52,_48);
this.addChild(_4c,z,_53);
_a.set(r,"tabIndex","0");
_11.focus(r);
}else{
_f.publish("/dojox/layout/gridContainer/moveRestriction",this);
}
}else{
var _54=_48.parentNode;
while(_46===null){
if(_54[pos]!==null&&_54[pos].nodeType!==3){
_54=_54[pos];
}else{
if(pos==="previousSibling"){
_54=_54.parentNode.childNodes[_54.parentNode.childNodes.length-1];
}else{
_54=_54.parentNode.childNodes[_5("ie")?0:1];
}
}
_46=_54[_4d];
if(_46&&_46.style.display=="none"){
_4a=_46.parentNode.childNodes;
var _55=null;
if(pos=="previousSibling"){
for(i=_4a.length-1;i>=0;i--){
if(_4a[i].style.display!="none"){
_55=_4a[i];
break;
}
}
}else{
for(i=0;i<_4a.length;i++){
if(_4a[i].style.display!="none"){
_55=_4a[i];
break;
}
}
}
if(!_55){
_48=_46;
_54=_48.parentNode;
_46=null;
}else{
_46=_55;
}
}
}
_11.focus(_46);
}
break;
}
}
}
},destroy:function(){
var m=this._dragManager;
_b.forEach(this._grid,function(_56){
m.unregister(_56.node);
});
this.inherited(arguments);
}});
gcl.ChildWidgetProperties={column:"1",dragRestriction:false};
_c.extend(_13,gcl.ChildWidgetProperties);
return gcl;
});
