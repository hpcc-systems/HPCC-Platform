//>>built
define("dojox/app/controllers/Transition",["require","dojo/_base/lang","dojo/_base/declare","dojo/has","dojo/on","dojo/Deferred","dojo/when","dojo/dom-style","../Controller","../utils/constraints"],function(_1,_2,_3,_4,on,_5,_6,_7,_8,_9){
var _a;
return _3("dojox.app.controllers.Transition",_8,{proceeding:false,waitingQueue:[],constructor:function(_b,_c){
this.events={"app-transition":this.transition,"app-domNode":this.onDomNodeChange};
_1([this.app.transit||"dojox/css3/transit"],function(t){
_a=t;
});
if(this.app.domNode){
this.onDomNodeChange({oldNode:null,newNode:this.app.domNode});
}
},transition:function(_d){
var _e=_d.viewId||"";
this.proceedingSaved=this.proceeding;
var _f=_e.split("+");
var _10,_11;
if(_f.length>0){
while(_f.length>1){
_10=_f.shift();
_11=_2.clone(_d);
_11.viewId=_10;
this.proceeding=true;
this.proceedTransition(_11);
}
_10=_f.shift();
var _12=_10.split("-");
if(_12.length>0){
_10=_12.shift();
while(_12.length>0){
var _13=_12.shift();
_11=_2.clone(_d);
_11.viewId=_13;
this._doTransition(_11.viewId,_11.opts,_11.opts.params,_d.opts.data,this.app,true,_11._doResize);
}
}
if(_10.length>0){
this.proceeding=this.proceedingSaved;
_d.viewId=_10;
_d._doResize=true;
this.proceedTransition(_d);
}
}else{
_d._doResize=true;
this.proceedTransition(_d);
}
},onDomNodeChange:function(evt){
if(evt.oldNode!=null){
this.unbind(evt.oldNode,"startTransition");
}
this.bind(evt.newNode,"startTransition",_2.hitch(this,this.onStartTransition));
},onStartTransition:function(evt){
if(evt.preventDefault){
evt.preventDefault();
}
evt.cancelBubble=true;
if(evt.stopPropagation){
evt.stopPropagation();
}
var _14=evt.detail.target;
var _15=/#(.+)/;
if(!_14&&_15.test(evt.detail.href)){
_14=evt.detail.href.match(_15)[1];
}
this.transition({"viewId":_14,opts:_2.mixin({},evt.detail),data:evt.detail.data});
},proceedTransition:function(_16){
if(this.proceeding){
this.app.log("in app/controllers/Transition proceedTransition push event",_16);
this.waitingQueue.push(_16);
this.processingQueue=false;
return;
}
if(this.waitingQueue.length>0&&!this.processingQueue){
this.processingQueue=true;
this.waitingQueue.push(_16);
_16=this.waitingQueue.shift();
}
this.proceeding=true;
this.app.log("in app/controllers/Transition proceedTransition calling trigger load",_16);
if(!_16.opts){
_16.opts={};
}
var _17=_16.params||_16.opts.params;
this.app.emit("app-load",{"viewId":_16.viewId,"params":_17,"callback":_2.hitch(this,function(){
var _18=this._doTransition(_16.viewId,_16.opts,_17,_16.opts.data,this.app,false,_16._doResize);
_6(_18,_2.hitch(this,function(){
this.proceeding=false;
var _19=this.waitingQueue.shift();
if(_19){
this.proceedTransition(_19);
}
}));
})});
},_getTransition:function(_1a,_1b,_1c){
var _1d=_1a;
var _1e=null;
if(_1d.views[_1b]){
_1e=_1d.views[_1b].transition;
}
if(!_1e){
_1e=_1d.transition;
}
var _1f=_1d.defaultTransition;
while(!_1e&&_1d.parent){
_1d=_1d.parent;
_1e=_1d.transition;
if(!_1f){
_1f=_1d.defaultTransition;
}
}
return _1e||_1c.transition||_1f||"none";
},_getParamsForView:function(_20,_21){
var _22={};
for(var _23 in _21){
var _24=_21[_23];
if(_2.isObject(_24)){
if(_23==_20){
_22=_2.mixin(_22,_24);
}
}else{
if(_23&&_24!=null){
_22[_23]=_21[_23];
}
}
}
return _22;
},_doTransition:function(_25,_26,_27,_28,_29,_2a,_2b,_2c){
if(!_29){
throw Error("view parent not found in transition.");
}
this.app.log("in app/controllers/Transition._doTransition transitionTo=[",_25,"], removeView = [",_2a,"] parent.name=[",_29.name,"], opts=",_26);
var _2d,_2e,_2f,_30;
if(_25){
_2d=_25.split(",");
}else{
_2d=_29.defaultView.split(",");
}
_2e=_2d.shift();
_2f=_2d.join(",");
_30=_29.children[_29.id+"_"+_2e];
if(!_30){
if(_2a){
this.app.log("> in Transition._doTransition called with removeView true, but that view is not available to remove");
return;
}
throw Error("child view must be loaded before transition.");
}
var _31=_9.getSelectedChild(_29,_30.constraint);
_30.params=this._getParamsForView(_30.name,_27);
if(!_2f&&_30.defaultView){
_2f=_30.defaultView;
}
if(_2a){
if(_30!==_31){
this.app.log("> in Transition._doTransition called with removeView true, but that view is not available to remove");
return;
}
_30=null;
}
if(_30!==_31){
var _32=_9.getAllSelectedChildren(_31);
for(var i=0;i<_32.length;i++){
var _33=_32[i];
if(_33&&_33.beforeDeactivate){
this.app.log("< in Transition._doTransition calling subChild.beforeDeactivate subChild name=[",_33.name,"], parent.name=[",_33.parent.name,"], next!==current path");
_33.beforeDeactivate();
}
}
if(_31){
this.app.log("< in Transition._doTransition calling current.beforeDeactivate current name=[",_31.name,"], parent.name=[",_31.parent.name,"], next!==current path");
_31.beforeDeactivate(_30,_28);
}
if(_30){
this.app.log("> in Transition._doTransition calling next.beforeActivate next name=[",_30.name,"], parent.name=[",_30.parent.name,"], next!==current path");
_30.beforeActivate(_31,_28);
}
this.app.log("> in Transition._doTransition calling app.emit layoutView view next");
if(!_2a){
this.app.emit("app-layoutView",{"parent":_29,"view":_30});
}
if(_2b&&!_2f){
this.app.emit("app-resize");
}
var _34=true;
if(_a&&(!_2c||_31!=null)){
var _35=_2.mixin({},_26);
_35=_2.mixin({},_35,{reverse:(_35.reverse||_35.transitionDir===-1)?true:false,transition:this._getTransition(_29,_2e,_35)});
if(_30){
this.app.log("    > in Transition._doTransition calling transit for current ="+_30.name);
}
_34=_a(_31&&_31.domNode,_30&&_30.domNode,_35);
}
_6(_34,_2.hitch(this,function(){
if(_30){
this.app.log("    < in Transition._doTransition back from transit for next ="+_30.name);
}
if(_2a){
this.app.emit("app-layoutView",{"parent":_29,"view":_31,"removeView":true});
}
var _36=_9.getAllSelectedChildren(_31);
for(var i=0;i<_36.length;i++){
var _37=_36[i];
if(_37&&_37.beforeDeactivate){
this.app.log("  < in Transition._doTransition calling subChild.afterDeactivate subChild name=[",_37.name,"], parent.name=[",_37.parent.name,"], next!==current path");
_37.afterDeactivate();
}
}
if(_31){
this.app.log("  < in Transition._doTransition calling current.afterDeactivate current name=[",_31.name,"], parent.name=[",_31.parent.name,"], next!==current path");
_31.afterDeactivate(_30,_28);
}
if(_30){
this.app.log("  > in Transition._doTransition calling next.afterActivate next name=[",_30.name,"], parent.name=[",_30.parent.name,"], next!==current path");
_30.afterActivate(_31,_28);
}
if(_2f){
this._doTransition(_2f,_26,_27,_28,_30||_29,_2a,_2b,true);
}
}));
return _34;
}
this.app.log("< in Transition._doTransition calling next.beforeDeactivate refresh current view next name=[",_30.name,"], parent.name=[",_30.parent.name,"], next==current path");
_30.beforeDeactivate(_31,_28);
this.app.log("  < in Transition._doTransition calling next.afterDeactivate refresh current view next name=[",_30.name,"], parent.name=[",_30.parent.name,"], next==current path");
_30.afterDeactivate(_31,_28);
this.app.log("> in Transition._doTransition calling next.beforeActivate next name=[",_30.name,"], parent.name=[",_30.parent.name,"], next==current path");
_30.beforeActivate(_31,_28);
this.app.log("> in Transition._doTransition calling app.triggger layoutView view next name=[",_30.name,"], removeView = [",_2a,"], parent.name=[",_30.parent.name,"], next==current path");
this.app.emit("app-layoutView",{"parent":_29,"view":_30,"removeView":_2a});
if(_2b&&!_2f){
this.app.emit("app-resize");
}
this.app.log("  > in Transition._doTransition calling next.afterActivate next name=[",_30.name,"], parent.name=[",_30.parent.name,"], next==current path");
_30.afterActivate(_31,_28);
if(_2f){
return this._doTransition(_2f,_26,_27,_28,_30,_2a);
}
}});
});
