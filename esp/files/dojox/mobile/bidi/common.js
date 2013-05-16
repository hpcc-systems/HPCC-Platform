//>>built
define("dojox/mobile/bidi/common",["dojo/_base/array","dijit/_BidiSupport"],function(_1,_2){
common={};
common.enforceTextDirWithUcc=function(_3,_4){
if(_4){
_4=(_4==="auto")?_2.prototype._checkContextual(_3):_4;
return ((_4==="rtl")?common.MARK.RLE:common.MARK.LRE)+_3+common.MARK.PDF;
}
return _3;
};
common.removeUCCFromText=function(_5){
if(!_5){
return _5;
}
return _5.replace(/\u202A|\u202B|\u202C/g,"");
};
common.setTextDirForButtons=function(_6){
var _7=_6.getChildren();
if(_7&&_6.textDir){
_1.forEach(_7,function(ch){
ch.set("textDir",_6.textDir);
},_6);
}
};
common.MARK={LRE:"‪",RLE:"‫",PDF:"‬"};
return common;
});
