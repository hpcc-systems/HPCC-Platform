//>>built
define("dojox/encoding/digests/_base",[],function(){
var _1={};
_1.outputTypes={Base64:0,Hex:1,String:2,Raw:3};
_1.addWords=function(a,b){
var l=(a&65535)+(b&65535);
var m=(a>>16)+(b>>16)+(l>>16);
return (m<<16)|(l&65535);
};
var _2=8;
var _3=(1<<_2)-1;
_1.stringToWord=function(s){
var wa=[];
for(var i=0,l=s.length*_2;i<l;i+=_2){
wa[i>>5]|=(s.charCodeAt(i/_2)&_3)<<(i%32);
}
return wa;
};
_1.wordToString=function(wa){
var s=[];
for(var i=0,l=wa.length*32;i<l;i+=_2){
s.push(String.fromCharCode((wa[i>>5]>>>(i%32))&_3));
}
return s.join("");
};
_1.wordToHex=function(wa){
var h="0123456789abcdef",s=[];
for(var i=0,l=wa.length*4;i<l;i++){
s.push(h.charAt((wa[i>>2]>>((i%4)*8+4))&15)+h.charAt((wa[i>>2]>>((i%4)*8))&15));
}
return s.join("");
};
_1.wordToBase64=function(wa){
var p="=",_4="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",s=[];
for(var i=0,l=wa.length*4;i<l;i+=3){
var t=(((wa[i>>2]>>8*(i%4))&255)<<16)|(((wa[i+1>>2]>>8*((i+1)%4))&255)<<8)|((wa[i+2>>2]>>8*((i+2)%4))&255);
for(var j=0;j<4;j++){
if(i*8+j*6>wa.length*32){
s.push(p);
}else{
s.push(_4.charAt((t>>6*(3-j))&63));
}
}
}
return s.join("");
};
_1.stringToUtf8=function(_5){
var _6="";
var i=-1;
var x,y;
while(++i<_5.length){
x=_5.charCodeAt(i);
y=i+1<_5.length?_5.charCodeAt(i+1):0;
if(55296<=x&&x<=56319&&56320<=y&&y<=57343){
x=65536+((x&1023)<<10)+(y&1023);
i++;
}
if(x<=127){
_6+=String.fromCharCode(x);
}else{
if(x<=2047){
_6+=String.fromCharCode(192|((x>>>6)&31),128|(x&63));
}else{
if(x<=65535){
_6+=String.fromCharCode(224|((x>>>12)&15),128|((x>>>6)&63),128|(x&63));
}else{
if(x<=2097151){
_6+=String.fromCharCode(240|((x>>>18)&7),128|((x>>>12)&63),128|((x>>>6)&63),128|(x&63));
}
}
}
}
}
return _6;
};
return _1;
});
