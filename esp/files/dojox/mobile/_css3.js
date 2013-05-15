//>>built
define("dojox/mobile/_css3",["dojo/_base/window","dojo/_base/array","dojo/has"],function(_1,_2,_3){
var _4=[],_5=[];
var _6=_1.doc.createElement("div").style;
var _7=["webkit"];
_3.add("css3-animations",function(_8,_9,_a){
var _b=_a.style;
return (_b["animation"]!==undefined&&_b["transition"]!==undefined)||_2.some(_7,function(p){
return _b[p+"Animation"]!==undefined&&_b[p+"Transition"]!==undefined;
});
});
var _c={name:function(p,_d){
var n=(_d?_5:_4)[p];
if(!n){
if(/End|Start/.test(p)){
var _e=p.length-(p.match(/End/)?3:5);
var s=p.substr(0,_e);
var pp=this.name(s);
if(pp==s){
n=p.toLowerCase();
}else{
n=pp+p.substr(_e);
}
}else{
if(p=="keyframes"){
var pk=this.name("animation",_d);
if(pk=="animation"){
n=p;
}else{
if(_d){
n=pk.replace(/animation/,"keyframes");
}else{
n=pk.replace(/Animation/,"Keyframes");
}
}
}else{
var cn=_d?p.replace(/-(.)/g,function(_f,p1){
return p1.toUpperCase();
}):p;
if(_6[cn]!==undefined){
n=p;
}else{
cn=cn.charAt(0).toUpperCase()+cn.slice(1);
_2.some(_7,function(_10){
if(_6[_10+cn]!==undefined){
if(_d){
n="-"+_10+"-"+p;
}else{
n=_10+cn;
}
}
});
}
}
}
if(!n){
n=p;
}
(_d?_5:_4)[p]=n;
}
return n;
},add:function(_11,_12){
for(var p in _12){
if(_12.hasOwnProperty(p)){
_11[_c.name(p)]=_12[p];
}
}
return _11;
}};
return _c;
});
