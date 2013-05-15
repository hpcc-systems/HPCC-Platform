//>>built
define("dojox/app/controllers/History",["dojo/_base/lang","dojo/_base/declare","dojo/on","../Controller","../utils/hash"],function(_1,_2,on,_3,_4){
return _2("dojox.app.controllers.History",_3,{constructor:function(_5){
this.events={"app-domNode":this.onDomNodeChange};
if(this.app.domNode){
this.onDomNodeChange({oldNode:null,newNode:this.app.domNode});
}
this.bind(window,"popstate",_1.hitch(this,this.onPopState));
},onDomNodeChange:function(_6){
if(_6.oldNode!=null){
this.unbind(_6.oldNode,"startTransition");
}
this.bind(_6.newNode,"startTransition",_1.hitch(this,this.onStartTransition));
},onStartTransition:function(_7){
var _8=_7.detail.url||"#"+_7.detail.target;
if(_7.detail.params){
_8=_4.buildWithParams(_8,_7.detail.params);
}
history.pushState(_7.detail,_7.detail.href,_8);
},onPopState:function(_9){
if(this.app.getStatus()!==this.app.lifecycle.STARTED){
return;
}
var _a=_9.state;
if(!_a){
if(window.location.hash){
_a={target:_4.getTarget(location.hash),url:location.hash,params:_4.getParams(location.hash)};
}else{
_a={target:this.app.defaultView};
}
}
if(_9._sim){
history.replaceState(_a,_a.title,_a.href);
}
this.app.emit("app-transition",{viewId:_a.target,opts:_1.mixin({reverse:true},_9.detail,{"params":_a.params})});
}});
});
