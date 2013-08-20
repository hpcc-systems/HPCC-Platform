//>>built
define("dojox/mobile/_EditableListMixin",["dojo/_base/array","dojo/_base/connect","dojo/_base/declare","dojo/_base/event","dojo/_base/window","dojo/dom-class","dojo/dom-geometry","dojo/dom-style","dojo/touch","dojo/dom-attr","dijit/registry","./ListItem"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c){
return _3("dojox.mobile._EditableListMixin",null,{rightIconForEdit:"mblDomButtonGrayKnob",deleteIconForEdit:"mblDomButtonRedCircleMinus",isEditing:false,destroy:function(){
if(this._blankItem){
this._blankItem.destroy();
}
this.inherited(arguments);
},_setupMoveItem:function(_d){
_8.set(_d,{width:_7.getContentBox(_d).w+"px",top:_d.offsetTop+"px"});
_6.add(_d,"mblListItemFloat");
},_resetMoveItem:function(_e){
this.defer(function(){
_6.remove(_e,"mblListItemFloat");
_8.set(_e,{width:"",top:""});
});
},_onClick:function(e){
if(e&&e.type==="keydown"&&e.keyCode!==13){
return;
}
if(this.onClick(e)===false){
return;
}
var _f=_b.getEnclosingWidget(e.target);
for(var n=e.target;n!==_f.domNode;n=n.parentNode){
if(n===_f.deleteIconNode){
_2.publish("/dojox/mobile/deleteListItem",[_f]);
this.onDeleteItem(_f);
break;
}
}
},onClick:function(){
},_onTouchStart:function(e){
if(this.getChildren().length<=1){
return;
}
if(!this._blankItem){
this._blankItem=new _c();
}
var _10=this._movingItem=_b.getEnclosingWidget(e.target);
this._startIndex=this.getIndexOfChild(_10);
var _11=false;
for(var n=e.target;n!==_10.domNode;n=n.parentNode){
if(n===_10.rightIconNode){
_11=true;
_a.set(_10.rightIconNode,"aria-grabbed","true");
_a.set(this.domNode,"aria-dropeffect","move");
break;
}
}
if(!_11){
return;
}
var ref=_10.getNextSibling();
ref=ref?ref.domNode:null;
this.containerNode.insertBefore(this._blankItem.domNode,ref);
this._setupMoveItem(_10.domNode);
this.containerNode.appendChild(_10.domNode);
if(!this._conn){
this._conn=[this.connect(this.domNode,_9.move,"_onTouchMove"),this.connect(_5.doc,_9.release,"_onTouchEnd")];
}
this._pos=[];
_1.forEach(this.getChildren(),function(c,_12){
this._pos.push(_7.position(c.domNode,true).y);
},this);
this.touchStartY=e.touches?e.touches[0].pageY:e.pageY;
this._startTop=_7.getMarginBox(_10.domNode).t;
_4.stop(e);
},_onTouchMove:function(e){
var y=e.touches?e.touches[0].pageY:e.pageY;
var _13=this._pos.length-1;
for(var i=1;i<this._pos.length;i++){
if(y<this._pos[i]){
_13=i-1;
break;
}
}
var _14=this.getChildren()[_13];
var _15=this._blankItem;
if(_14!==_15){
var p=_14.domNode.parentNode;
if(_14.getIndexInParent()<_15.getIndexInParent()){
p.insertBefore(_15.domNode,_14.domNode);
}else{
p.insertBefore(_14.domNode,_15.domNode);
}
}
this._movingItem.domNode.style.top=this._startTop+(y-this.touchStartY)+"px";
},_onTouchEnd:function(e){
var _16=this._startIndex;
var _17=this.getIndexOfChild(this._blankItem);
var ref=this._blankItem.getNextSibling();
ref=ref?ref.domNode:null;
if(ref===null){
_17--;
}
this.containerNode.insertBefore(this._movingItem.domNode,ref);
this.containerNode.removeChild(this._blankItem.domNode);
this._resetMoveItem(this._movingItem.domNode);
_1.forEach(this._conn,_2.disconnect);
this._conn=null;
this.onMoveItem(this._movingItem,_16,_17);
_a.set(this._movingItem.rightIconNode,"aria-grabbed","false");
_a.remove(this.domNode,"aria-dropeffect");
},startEdit:function(){
this.isEditing=true;
_6.add(this.domNode,"mblEditableRoundRectList");
_1.forEach(this.getChildren(),function(_18){
if(!_18.deleteIconNode){
_18.set("rightIcon",this.rightIconForEdit);
if(_18.rightIconNode){
_a.set(_18.rightIconNode,"role","button");
_a.set(_18.rightIconNode,"aria-grabbed","false");
}
_18.set("deleteIcon",this.deleteIconForEdit);
_18.deleteIconNode.tabIndex=_18.tabIndex;
if(_18.deleteIconNode){
_a.set(_18.deleteIconNode,"role","button");
}
}
_18.rightIconNode.style.display="";
_18.deleteIconNode.style.display="";
if(typeof _18.rightIconNode.style.msTouchAction!="undefined"){
_18.rightIconNode.style.msTouchAction="none";
}
},this);
if(!this._handles){
this._handles=[this.connect(this.domNode,_9.press,"_onTouchStart"),this.connect(this.domNode,"onclick","_onClick"),this.connect(this.domNode,"onkeydown","_onClick")];
}
this.onStartEdit();
},endEdit:function(){
_6.remove(this.domNode,"mblEditableRoundRectList");
_1.forEach(this.getChildren(),function(_19){
_19.rightIconNode.style.display="none";
_19.deleteIconNode.style.display="none";
if(typeof _19.rightIconNode.style.msTouchAction!="undefined"){
_19.rightIconNode.style.msTouchAction="auto";
}
});
if(this._handles){
_1.forEach(this._handles,this.disconnect,this);
this._handles=null;
}
this.isEditing=false;
this.onEndEdit();
},onDeleteItem:function(_1a){
},onMoveItem:function(_1b,_1c,to){
},onStartEdit:function(){
},onEndEdit:function(){
}});
});
