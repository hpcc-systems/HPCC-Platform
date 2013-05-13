//>>built
define("dojox/form/uploader/_HTML5",["dojo/_base/declare","dojo/_base/lang","dojo/_base/array","dojo"],function(_1,_2,_3,_4){
return _1("dojox.form.uploader._HTML5",[],{errMsg:"Error uploading files. Try checking permissions",uploadType:"html5",postMixInProperties:function(){
this.inherited(arguments);
if(this.uploadType==="html5"){
}
},postCreate:function(){
this.connectForm();
this.inherited(arguments);
if(this.uploadOnSelect){
this.connect(this,"onChange",function(_5){
this.upload(_5[0]);
});
}
},_drop:function(e){
_4.stopEvent(e);
var dt=e.dataTransfer;
this._files=dt.files;
this.onChange(this.getFileList());
},upload:function(_6){
this.onBegin(this.getFileList());
this.uploadWithFormData(_6);
},addDropTarget:function(_7,_8){
if(!_8){
this.connect(_7,"dragenter",_4.stopEvent);
this.connect(_7,"dragover",_4.stopEvent);
this.connect(_7,"dragleave",_4.stopEvent);
}
this.connect(_7,"drop","_drop");
},uploadWithFormData:function(_9){
if(!this.getUrl()){
console.error("No upload url found.",this);
return;
}
var fd=new FormData(),_a=this._getFileFieldName();
_3.forEach(this._files,function(f,i){
fd.append(_a,f);
},this);
if(_9){
_9.uploadType=this.uploadType;
for(var nm in _9){
fd.append(nm,_9[nm]);
}
}
var _b=this.createXhr();
_b.send(fd);
},_xhrProgress:function(_c){
if(_c.lengthComputable){
var o={bytesLoaded:_c.loaded,bytesTotal:_c.total,type:_c.type,timeStamp:_c.timeStamp};
if(_c.type=="load"){
o.percent="100%";
o.decimal=1;
}else{
o.decimal=_c.loaded/_c.total;
o.percent=Math.ceil((_c.loaded/_c.total)*100)+"%";
}
this.onProgress(o);
}
},createXhr:function(){
var _d=new XMLHttpRequest();
var _e;
_d.upload.addEventListener("progress",_2.hitch(this,"_xhrProgress"),false);
_d.addEventListener("load",_2.hitch(this,"_xhrProgress"),false);
_d.addEventListener("error",_2.hitch(this,function(_f){
this.onError(_f);
clearInterval(_e);
}),false);
_d.addEventListener("abort",_2.hitch(this,function(evt){
this.onAbort(evt);
clearInterval(_e);
}),false);
_d.onreadystatechange=_2.hitch(this,function(){
if(_d.readyState===4){
clearInterval(_e);
try{
this.onComplete(JSON.parse(_d.responseText.replace(/^\{\}&&/,"")));
}
catch(e){
var msg="Error parsing server result:";
console.error(msg,e);
console.error(_d.responseText);
this.onError(msg,e);
}
}
});
_d.open("POST",this.getUrl());
_d.setRequestHeader("Accept","application/json");
_e=setInterval(_2.hitch(this,function(){
try{
if(typeof (_d.statusText)){
}
}
catch(e){
clearInterval(_e);
}
}),250);
return _d;
}});
});
