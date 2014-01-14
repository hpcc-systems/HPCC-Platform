//>>built
define("dojox/app/controllers/Layout",["dojo/_base/declare","dojo/_base/lang","dojo/_base/array","dojo/_base/window","dojo/query","dojo/dom-geometry","dojo/dom-attr","dojo/dom-style","dijit/registry","./LayoutBase","../utils/layout","../utils/constraints"],function(_1,_2,_3,_4,_5,_6,_7,_8,_9,_a,_b,_c){
return _1("dojox.app.controllers.Layout",_a,{constructor:function(_d,_e){
},onResize:function(){
this._doResize(this.app);
this.resizeSelectedChildren(this.app);
},resizeSelectedChildren:function(w){
for(var _f in w.selectedChildren){
if(w.selectedChildren[_f]&&w.selectedChildren[_f].domNode){
this.app.log("in Layout resizeSelectedChildren calling resizeSelectedChildren calling _doResize for w.selectedChildren[hash].id="+w.selectedChildren[_f].id);
this._doResize(w.selectedChildren[_f]);
_3.forEach(w.selectedChildren[_f].domNode.children,function(_10){
if(_9.byId(_10.id)&&_9.byId(_10.id).resize){
_9.byId(_10.id).resize();
}
});
this.resizeSelectedChildren(w.selectedChildren[_f]);
}
}
},initLayout:function(_11){
this.app.log("in app/controllers/Layout.initLayout event=",_11);
this.app.log("in app/controllers/Layout.initLayout event.view.parent.name=[",_11.view.parent.name,"]");
if(!_11.view.domNode.parentNode){
_11.view.parent.domNode.appendChild(_11.view.domNode);
}
_7.set(_11.view.domNode,"data-app-constraint",_11.view.constraint);
this.inherited(arguments);
},_doResize:function(_12){
var _13=_12.domNode;
if(!_13){
this.app.log("Warning - View has not been loaded, in Layout _doResize view.domNode is not set for view.id="+_12.id+" view=",_12);
return;
}
var mb={};
if(!("h" in mb)||!("w" in mb)){
mb=_2.mixin(_6.getMarginBox(_13),mb);
}
if(_12!==this.app){
var cs=_8.getComputedStyle(_13);
var me=_6.getMarginExtents(_13,cs);
var be=_6.getBorderExtents(_13,cs);
var bb=(_12._borderBox={w:mb.w-(me.w+be.w),h:mb.h-(me.h+be.h)});
var pe=_6.getPadExtents(_13,cs);
_12._contentBox={l:_8.toPixelValue(_13,cs.paddingLeft),t:_8.toPixelValue(_13,cs.paddingTop),w:bb.w-pe.w,h:bb.h-pe.h};
}else{
_12._contentBox={l:0,t:0,h:_4.global.innerHeight||_4.doc.documentElement.clientHeight,w:_4.global.innerWidth||_4.doc.documentElement.clientWidth};
}
this.inherited(arguments);
},layoutView:function(_14){
if(_14.view){
this.inherited(arguments);
if(_14.doResize){
this._doResize(_14.parent||this.app);
this._doResize(_14.view);
}
}
},_doLayout:function(_15){
if(!_15){
console.warn("layout empty view.");
return;
}
this.app.log("in Layout _doLayout called for view.id="+_15.id+" view=",_15);
var _16;
var _17=_c.getSelectedChild(_15,_15.constraint);
if(_17&&_17.isFullScreen){
console.warn("fullscreen sceen layout");
}else{
_16=_5("> [data-app-constraint]",_15.domNode).map(function(_18){
var w=_9.getEnclosingWidget(_18);
if(w){
w._constraint=_7.get(_18,"data-app-constraint");
return w;
}
return {domNode:_18,_constraint:_7.get(_18,"data-app-constraint")};
});
if(_17){
_16=_3.filter(_16,function(c){
return c.domNode&&c._constraint;
},_15);
}
}
if(_15._contentBox){
_b.layoutChildren(_15.domNode,_15._contentBox,_16);
}
}});
});
