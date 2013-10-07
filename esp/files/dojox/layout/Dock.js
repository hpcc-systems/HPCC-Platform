//>>built
require({cache:{"url:dojox/layout/resources/FloatingPane.html":"<div class=\"dojoxFloatingPane\" id=\"${id}\">\n\t<div tabindex=\"0\" role=\"button\" class=\"dojoxFloatingPaneTitle\" dojoAttachPoint=\"focusNode\">\n\t\t<span dojoAttachPoint=\"closeNode\" dojoAttachEvent=\"onclick: close\" class=\"dojoxFloatingCloseIcon\"></span>\n\t\t<span dojoAttachPoint=\"maxNode\" dojoAttachEvent=\"onclick: maximize\" class=\"dojoxFloatingMaximizeIcon\">&thinsp;</span>\n\t\t<span dojoAttachPoint=\"restoreNode\" dojoAttachEvent=\"onclick: _restore\" class=\"dojoxFloatingRestoreIcon\">&thinsp;</span>\t\n\t\t<span dojoAttachPoint=\"dockNode\" dojoAttachEvent=\"onclick: minimize\" class=\"dojoxFloatingMinimizeIcon\">&thinsp;</span>\n\t\t<span dojoAttachPoint=\"titleNode\" class=\"dijitInline dijitTitleNode\"></span>\n\t</div>\n\t<div dojoAttachPoint=\"canvas\" class=\"dojoxFloatingPaneCanvas\">\n\t\t<div dojoAttachPoint=\"containerNode\" role=\"region\" tabindex=\"-1\" class=\"${contentClass}\">\n\t\t</div>\n\t\t<span dojoAttachPoint=\"resizeHandle\" class=\"dojoxFloatingResizeHandle\"></span>\n\t</div>\n</div>\n"}});
define("dojox/layout/Dock",["dojo/_base/lang","dojo/_base/window","dojo/_base/declare","dojo/_base/fx","dojo/on","dojo/_base/array","dojo/_base/sniff","dojo/window","dojo/dom","dojo/dom-class","dojo/dom-geometry","dojo/dom-construct","dijit/_TemplatedMixin","dijit/_WidgetBase","dijit/BackgroundIframe","dojo/dnd/Moveable","./ContentPane","./ResizeHandle","dojo/text!./resources/FloatingPane.html","dojo/domReady!"],function(_1,_2,_3,fx,on,_4,_5,_6,_7,_8,_9,_a,_b,_c,_d,_e,_f,_10,_11){
var _12=_3("dojox.layout.Dock",[_c,_b],{templateString:"<div class=\"dojoxDock\"><ul dojo-dojo-attach-point=\"containerNode\" class=\"dojoxDockList\"></ul></div>",_docked:[],_inPositioning:false,autoPosition:false,addNode:function(_13){
var div=_a.create("li",null,this.containerNode),_14=new _15({title:_13.title,paneRef:_13},div);
_14.startup();
return _14;
},startup:function(){
if(this.id=="dojoxGlobalFloatingDock"||this.isFixedDock){
this.own(on(window,"resize",_1.hitch(this,"_positionDock")),on(window,"scroll",_1.hitch(this,"_positionDock")));
if(_5("ie")){
this.own(on(this.domNode,"resize",_1.hitch(this,"_positionDock")));
}
}
this._positionDock(null);
this.inherited(arguments);
},_positionDock:function(e){
if(!this._inPositioning){
if(this.autoPosition=="south"){
setTimeout(_1.hitch(this,function(){
this._inPositiononing=true;
var _16=_6.getBox();
var s=this.domNode.style;
s.left=_16.l+"px";
s.width=(_16.w-2)+"px";
s.top=(_16.h+_16.t)-this.domNode.offsetHeight+"px";
this._inPositioning=false;
}),125);
}
}
}});
var _15=_3("dojox.layout._DockNode",[_c,_b],{title:"",paneRef:null,templateString:"<li data-dojo-attach-event=\"onclick: restore\" class=\"dojoxDockNode\">"+"<span data-dojo-attach-point=\"restoreNode\" class=\"dojoxDockRestoreButton\" data-dojo-attach-event=\"onclick: restore\"></span>"+"<span class=\"dojoxDockTitleNode\" data-dojo-attach-point=\"titleNode\">${title}</span>"+"</li>",restore:function(){
this.paneRef.show();
this.paneRef.bringToTop();
this.destroy();
}});
return _12;
});
