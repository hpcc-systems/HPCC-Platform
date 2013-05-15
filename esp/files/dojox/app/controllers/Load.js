//>>built
define("dojox/app/controllers/Load",["require","dojo/_base/lang","dojo/_base/declare","dojo/on","dojo/Deferred","dojo/when","../Controller"],function(_1,_2,_3,on,_4,_5,_6,_7){
return _3("dojox.app.controllers.Load",_6,{_waitingQueue:[],constructor:function(_8,_9){
this.events={"app-init":this.init,"app-load":this.load};
},init:function(_a){
_5(this.createView(_a.parent,null,null,{templateString:_a.templateString,controller:_a.controller},null,_a.type),function(_b){
_5(_b.start(),_a.callback);
});
},load:function(_c){
this.app.log("in app/controllers/Load event.viewId="+_c.viewId+" event =",_c);
var _d=_c.viewId||"";
var _e=[];
var _f=_d.split("+");
while(_f.length>0){
var _10=_f.shift();
_e.push(_10);
}
var def;
this.proceedLoadViewDef=new _4();
if(_e&&_e.length>1){
for(var i=0;i<_e.length-1;i++){
var _11=_2.clone(_c);
_11.callback=null;
_11.viewId=_e[i];
this._waitingQueue.push(_11);
}
this.proceedLoadView(this._waitingQueue.shift());
_5(this.proceedLoadViewDef,_2.hitch(this,function(){
var _12=_2.clone(_c);
_12.viewId=_e[i];
def=this.loadView(_12);
return def;
}));
}else{
def=this.loadView(_c);
return def;
}
},proceedLoadView:function(_13){
var def=this.loadView(_13);
_5(def,_2.hitch(this,function(){
this.app.log("in app/controllers/Load proceedLoadView back from loadView for event",_13);
var _14=this._waitingQueue.shift();
if(_14){
this.app.log("in app/controllers/Load proceedLoadView back from loadView calling this.proceedLoadView(nextEvt) for ",_14);
this.proceedLoadView(_14);
}else{
this._waitingQueue=[];
this.proceedLoadViewDef.resolve();
}
}));
},loadView:function(_15){
var _16=_15.parent||this.app;
var _17=_15.viewId||"";
var _18=_17.split(",");
var _19=_18.shift();
var _1a=_18.join(",");
var _1b=_15.params||"";
var def=this.loadChild(_16,_19,_1a,_1b);
if(_15.callback){
_5(def,_15.callback);
}
return def;
},createChild:function(_1c,_1d,_1e,_1f){
var id=_1c.id+"_"+_1d;
if(!_1f&&_1c.views[_1d].defaultParams){
_1f=_1c.views[_1d].defaultParams;
}
var _20=_1c.children[id];
if(_20){
if(_1f){
_20.params=_1f;
}
this.app.log("in app/controllers/Load createChild view is already loaded so return the loaded view with the new parms ",_20);
return _20;
}
var def=new _4();
_5(this.createView(_1c,id,_1d,null,_1f,_1c.views[_1d].type),function(_21){
_1c.children[id]=_21;
_5(_21.start(),function(_22){
def.resolve(_22);
});
});
return def;
},createView:function(_23,id,_24,_25,_26,_27){
var def=new _4();
var app=this.app;
_1([_27?_27:"../View"],function(_28){
var _29=new _28(_2.mixin({"app":app,"id":id,"name":_24,"parent":_23},{"params":_26},_25));
def.resolve(_29);
});
return def;
},loadChild:function(_2a,_2b,_2c,_2d){
if(!_2a){
throw Error("No parent for Child '"+_2b+"'.");
}
if(!_2b){
var _2e=_2a.defaultView?_2a.defaultView.split(","):"default";
_2b=_2e.shift();
_2c=_2e.join(",");
}
var _2f=new _4();
var _30;
try{
_30=this.createChild(_2a,_2b,_2c,_2d);
}
catch(ex){
_2f.reject("load child '"+_2b+"' error.");
return _2f.promise;
}
_5(_30,_2.hitch(this,function(_31){
if(!_2c&&_31.defaultView){
_2c=_31.defaultView;
}
var _32=_2c.split(",");
_2b=_32.shift();
_2c=_32.join(",");
if(_2b){
var _33=this.loadChild(_31,_2b,_2c,_2d);
_5(_33,function(){
_2f.resolve();
},function(){
_2f.reject("load child '"+_2b+"' error.");
});
}else{
_2f.resolve();
}
}),function(){
_2f.reject("load child '"+_2b+"' error.");
});
return _2f.promise;
}});
});
