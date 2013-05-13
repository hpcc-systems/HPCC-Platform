//>>built
define("dojox/charting/bidi/Chart",["dojox/main","dojo/_base/declare","dojo/_base/lang","dojo/dom-style","dojo/_base/array","dojo/sniff","dojo/dom","dojo/dom-construct","dojox/gfx","dojox/gfx/_gfxBidiSupport","../axis2d/common","dojox/string/BidiEngine","dojox/lang/functional","dojo/dom-attr","./_bidiutils"],function(_1,_2,_3,_4,_5,_6,_7,_8,g,_9,da,_a,df,_b,_c){
var _d=new _a();
var dc=_3.getObject("charting",true,_1);
function _e(_f){
return /^(ltr|rtl|auto)$/.test(_f)?_f:null;
};
return _2(null,{textDir:"",dir:"",isMirrored:false,getTextDir:function(_10){
var _11=this.textDir=="auto"?_d.checkContextual(_10):this.textDir;
if(!_11){
_11=_4.get(this.node,"direction");
}
return _11;
},postscript:function(_12,_13){
var _14=_13?(_13["textDir"]?_e(_13["textDir"]):""):"";
_14=_14?_14:_4.get(this.node,"direction");
this.textDir=_14;
this.surface.textDir=_14;
this.htmlElementsRegistry=[];
this.truncatedLabelsRegistry=[];
var _15="ltr";
if(_b.has(_12,"direction")){
_15=_b.get(_12,"direction");
}
this.setDir(_13?(_13.dir?_13.dir:_15):_15);
},setTextDir:function(_16,obj){
if(_16==this.textDir){
return this;
}
if(_e(_16)!=null){
this.textDir=_16;
this.surface.setTextDir(_16);
if(this.truncatedLabelsRegistry&&_16=="auto"){
_5.forEach(this.truncatedLabelsRegistry,function(_17){
var _18=this.getTextDir(_17["label"]);
if(_17["element"].textDir!=_18){
_17["element"].setShape({textDir:_18});
}
},this);
}
var _19=df.keys(this.axes);
if(_19.length>0){
_5.forEach(_19,function(key,_1a,arr){
var _1b=this.axes[key];
if(_1b.htmlElements[0]){
_1b.dirty=true;
_1b.render(this.dim,this.offsets);
}
},this);
if(this.title){
var _1c=(g.renderer=="canvas"),_1d=_1c||!_6("ie")&&!_6("opera")?"html":"gfx",_1e=g.normalizedLength(g.splitFontString(this.titleFont).size);
_8.destroy(this.chartTitle);
this.chartTitle=null;
this.chartTitle=da.createText[_1d](this,this.surface,this.dim.width/2,this.titlePos=="top"?_1e+this.margins.t:this.dim.height-this.margins.b,"middle",this.title,this.titleFont,this.titleFontColor);
}
}else{
_5.forEach(this.htmlElementsRegistry,function(_1f,_20,arr){
var _21=_16=="auto"?this.getTextDir(_1f[4]):_16;
if(_1f[0].children[0]&&_1f[0].children[0].dir!=_21){
_8.destroy(_1f[0].children[0]);
_1f[0].children[0]=da.createText["html"](this,this.surface,_1f[1],_1f[2],_1f[3],_1f[4],_1f[5],_1f[6]).children[0];
}
},this);
}
}
return this;
},setDir:function(dir){
if(dir=="rtl"||dir=="ltr"){
if(this.dir!=dir){
this.isMirrored=true;
this.dirty=true;
}
this.dir=dir;
}
return this;
},isRightToLeft:function(){
return this.dir=="rtl";
},applyMirroring:function(_22,dim,_23){
_c.reverseMatrix(_22,dim,_23,this.dir=="rtl");
_4.set(this.node,"direction","ltr");
return this;
},formatTruncatedLabel:function(_24,_25,_26){
this.truncateBidi(_24,_25,_26);
},truncateBidi:function(_27,_28,_29){
if(_29=="gfx"){
this.truncatedLabelsRegistry.push({element:_27,label:_28});
if(this.textDir=="auto"){
_27.setShape({textDir:this.getTextDir(_28)});
}
}
if(_29=="html"&&this.textDir=="auto"){
_27.children[0].dir=this.getTextDir(_28);
}
},render:function(){
this.inherited(arguments);
this.isMirrored=false;
return this;
},_resetLeftBottom:function(_2a){
if(_2a.vertical&&this.isMirrored){
_2a.opt.leftBottom=!_2a.opt.leftBottom;
}
}});
});
