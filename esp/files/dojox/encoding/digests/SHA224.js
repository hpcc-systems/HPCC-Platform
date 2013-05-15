//>>built
define("dojox/encoding/digests/SHA224",["./_sha-32"],function(_1){
var _2=[3238371032,914150663,812702999,4144912697,4290775857,1750603025,1694076839,3204075428];
var _3=function(_4,_5){
var _6=_5||_1.outputTypes.Base64;
_4=_1.stringToUtf8(_4);
var wa=_1.digest(_1.toWord(_4),_4.length*8,_2,224);
switch(_6){
case _1.outputTypes.Raw:
return wa;
case _1.outputTypes.Hex:
return _1.toHex(wa);
case _1.outputTypes.String:
return _1._toString(wa);
default:
return _1.toBase64(wa);
}
};
_3._hmac=function(_7,_8,_9){
var _a=_9||_1.outputTypes.Base64;
_7=_1.stringToUtf8(_7);
_8=_1.stringToUtf8(_8);
var wa=_1.toWord(_8);
if(wa.length>16){
wa=_1.digest(wa,_8.length*8,_2,224);
}
var _b=new Array(16),_c=new Array(16);
for(var i=0;i<16;i++){
_b[i]=wa[i]^909522486;
_c[i]=wa[i]^1549556828;
}
var r1=_1.digest(_b.concat(_1.toWord(_7)),512+_7.length*8,_2,224);
var r2=_1.digest(_c.concat(r1),512+160,_2,224);
switch(_a){
case _1.outputTypes.Raw:
return wa;
case _1.outputTypes.Hex:
return _1.toHex(wa);
case _1.outputTypes.String:
return _1._toString(wa);
default:
return _1.toBase64(wa);
}
};
return _3;
});
