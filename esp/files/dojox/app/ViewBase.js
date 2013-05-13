//>>built
define("dojox/app/ViewBase",["require","dojo/when","dojo/on","dojo/dom-attr","dojo/_base/declare","dojo/_base/lang","dojo/Deferred","./utils/model","./utils/constraints"],function(_1,_2,on,_3,_4,_5,_6,_7,_8){
return _4("dojox.app.ViewBase",null,{constructor:function(_9){
this.id="";
this.name="";
this.children={};
this.selectedChildren={};
this.loadedStores={};
this._started=false;
_5.mixin(this,_9);
if(this.parent.views){
_5.mixin(this,this.parent.views[this.name]);
}
},start:function(){
if(this._started){
return this;
}
this._startDef=new _6();
_2(this.load(),_5.hitch(this,function(){
this._createDataStore(this);
this._setupModel();
}));
return this._startDef;
},load:function(){
var _a=this._loadViewController();
_2(_a,_5.hitch(this,function(_b){
if(_b){
_5.mixin(this,_b);
}
}));
return _a;
},_createDataStore:function(){
if(this.parent.loadedStores){
_5.mixin(this.loadedStores,this.parent.loadedStores);
}
if(this.stores){
for(var _c in this.stores){
if(_c.charAt(0)!=="_"){
var _d=this.stores[_c].type?this.stores[_c].type:"dojo/store/Memory";
var _e={};
if(this.stores[_c].params){
_5.mixin(_e,this.stores[_c].params);
}
try{
var _f=_1(_d);
}
catch(e){
throw new Error(_d+" must be listed in the dependencies");
}
if(_e.data&&_5.isString(_e.data)){
_e.data=_5.getObject(_e.data);
}
if(this.stores[_c].observable){
try{
var _10=_1("dojo/store/Observable");
}
catch(e){
throw new Error("dojo/store/Observable must be listed in the dependencies");
}
this.stores[_c].store=_10(new _f(_e));
}else{
this.stores[_c].store=new _f(_e);
}
this.loadedStores[_c]=this.stores[_c].store;
}
}
}
},_setupModel:function(){
if(!this.loadedModels){
var _11;
try{
_11=_7(this.models,this.parent,this.app);
}
catch(e){
throw new Error("Error creating models: "+e.message);
}
_2(_11,_5.hitch(this,function(_12){
if(_12){
this.loadedModels=_5.isArray(_12)?_12[0]:_12;
}
this._startup();
}),function(err){
throw new Error("Error creating models: "+err.message);
});
}else{
this._startup();
}
},_startup:function(){
this._startLayout();
},_startLayout:function(){
this.app.log("  > in app/ViewBase _startLayout firing layout for name=[",this.name,"], parent.name=[",this.parent.name,"]");
if(!this.hasOwnProperty("constraint")){
this.constraint=_3.get(this.domNode,"data-app-constraint")||"center";
}
_8.register(this.constraint);
this.app.emit("app-initLayout",{"view":this,"callback":_5.hitch(this,function(){
this.startup();
this.app.log("  > in app/ViewBase calling init() name=[",this.name,"], parent.name=[",this.parent.name,"]");
this.init();
this._started=true;
if(this._startDef){
this._startDef.resolve(this);
}
})});
},_loadViewController:function(){
var _13=new _6();
var _14;
if(!this.controller){
this.app.log("  > in app/ViewBase _loadViewController no controller set for view name=[",this.name,"], parent.name=[",this.parent.name,"]");
_13.resolve(true);
return _13;
}else{
_14=this.controller.replace(/(\.js)$/,"");
}
var _15;
try{
var _16=_14;
var _17=_16.indexOf("./");
if(_17>=0){
_16=_14.substring(_17+2);
}
_15=_1.on("error",function(_18){
if(_13.isResolved()||_13.isRejected()){
return;
}
if(_18.info[0]&&(_18.info[0].indexOf(_16)>=0)){
_13.resolve(false);
_15.remove();
}
});
if(_14.indexOf("./")==0){
_14="app/"+_14;
}
_1([_14],function(_19){
_13.resolve(_19);
_15.remove();
});
}
catch(e){
_13.reject(e);
if(_15){
_15.remove();
}
}
return _13;
},init:function(){
},beforeActivate:function(){
},afterActivate:function(){
},beforeDeactivate:function(){
},afterDeactivate:function(){
},destroy:function(){
}});
});
