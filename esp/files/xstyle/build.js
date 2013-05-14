/*
 * This module can be executed standalone in node, to build CSS files, inlining imports, and 
 * pre-processing extensions for faster run-time execution. This module is also
 * used by the AMD build process.
 */
var operatorMatch = {
	'{': '}',
	'[': ']',
	'(': ')'
};
var nextId = 1;

document = {
	createElement: function(){
		return {style:
			{// TODO: may want to import static has features to determine if some of these exist
			}
		};
	},
	getElementsByTagName: function(){
		return [];
	},
	addEventListener: function(){}
};
navigator = {
	userAgent: "build"
};

var pseudoDefine = function(id, deps, factory){
		function pseudoRequire(deps){
			[].push.apply(requiredModules, deps);
		}
		pseudoRequire.isBuild = true;
		parse = factory(pseudoRequire);
	};
var requiredModules = [], base64Module;

if(typeof define == 'undefined'){
	var fs = require('fs'),
		pathModule = require('path'),
		parse;
	define = pseudoDefine;
	require('./core/parser');
}else{
	define(['build/fs', './build/base64'], function(fsModule, base64){
		fs = fsModule;
		base64Module = base64;
		// must create our own path module for Rhino :/
		pathModule = {
			resolve: function(base, target){
				return (base.replace(/[^\/]+$/, '') + target)
						.replace(/\/[^\/]*\/\.\./g, '')
						.replace(/\/\./g,'');
			},
			dirname: function(path){
				return path.replace(/[\/\\][^\/\\]*$/, '');
			},
			relative: function(basePath, path){
				return path.slice(this.dirname(basePath).length + 1);
			},
			join: function(base, target){
				return ((base[base.length - 1]  == '/' ? base : (base + '/'))+ target)
						.replace(/\/[^\/]*\/\.\./g, '')
						.replace(/\/\./g,'');
			}
		}
		return function(xstyleText){
			var define = pseudoDefine;
			eval(xstyleText);
			return processCss;
		};
	});
}
function main(source, target){
	var imported = {};
	var basePath = source.replace(/[^\/]*$/, '');
	var cssText = fs.readFileSync(source).toString("utf-8");
	var processed = processCss(cssText, basePath);
	var output = processed.standardCss;
	if(processed.xstyleCss){
		output += 'x-xstyle{content:"' + 
				processed.xstyleCss.replace(/["\\\n\r]/g, '\\$&') + 
					'";}';
	}
	if(target){
		fs.writeFileSync(target, output);
	}else{
		console.log(output);
	}
}
function minify(cssText){
	return cssText.
			replace(/\/\*([^\*]|\*[^\/])*\*\//g, ' ').
			replace(/\s*("(\\\\|[^\"])*"|'(\\\\|[^\'])*'|[;}{:])\s*/g,"$1");	
}
var mimeTypes = {
	eot: "application/vnd.ms-fontobject",
	woff: "application/font-woff",
	gif: "image/gif",
	jpg: "image/jpeg",
	jpeg: "image/jpeg",
	png: "image/png"	
}
function processCss(cssText, basePath, inlineAllResources){
	function XRule(){
		
	}
	XRule.prototype = {
		newCall: function(name){
			return new Call(name);
		},
		newRule: function(){
			return new XRule();
		},
		getDefinition: function(name, includeRules){
			var parentRule = this;
			do{
				var target = parentRule.properties && parentRule.properties[name]
					|| (includeRules && parentRule.rules && parentRule.rules[name]);
				parentRule = parentRule.parent;
			}while(!target && parentRule);
			return target;
		},
		declareProperty: function(name, value, conditional){
			// TODO: access staticHasFeatures to check conditional
			xstyleCss.push(name + '=' + value);
			var properties = (this.properties || (this.properties = {}));
			properties[name] = true;
		},
		setValue: function(name, value){
			var target = this.getDefinition(name);
			if(!target || !this.xstyleStarted){
				if(!this.ruleStarted){
					this.ruleStarted = true;
					this.selectorIndex = browserCss.push(this.selector);
					browserCss.push('{');
				}
				if(!target){
					browserCss.push(name, ':', value, ';');
				}
			}
			if(target){
				if(!this.xstyleStarted){
					this.xstyleStarted = true;
					var starter = '/' + (this.id = nextId++) + '{';
					browserCss[this.selectorIndex] += ',#' + this.id;
				}
				
				xstyleCss.push((starter || '') + name + ':' + value, '}');
			}
		},
		onRule: function(){
			browserCss.push('}');
			if(this.xstyleStarted){
				xstyleCss.push('}')
			}
		}
	};
	// a class representing function calls
	function Call(value){
		// we store the caller and the arguments
		this.caller = value;
		this.args = [];
	}
	var CallPrototype = Call.prototype = new XRule;
	CallPrototype.declareProperty = CallPrototype.setValue = function(name, value){
		// handle these both as addition of arguments
		this.args.push(value);
	};
	CallPrototype.toString = function(){
		var operator = this.operator;
		return operator + this.args + operatorMatch[operator]; 
	};
	
	function insertRule(cssText){
		//browserCss.push(cssText);
	}
	function correctUrls(cssText, path){
		// correct all the URLs in the stylesheets
		// determine the directory path
		path = pathModule.dirname(path) + '/';
		//console.log("starting path", basePath , path);
		// compute the relative path from where we are to the base path where the stylesheet will go
		var relativePath = pathModule.relative(basePath, path);
		return cssText.replace(/url\s*\(\s*['"]?([^'"\)]*)['"]?\s*\)/g, function(t, url){
			if(inlineAllResources || /#inline$/.test(url)){
				// we can inline the resource
				suffix = url.match(/\.(\w+)(#|\?|$)/);
				suffix = suffix && suffix[1];
				url = url.replace(/[\?#].*/,'');
				
				if(base64Module){
					// reading binary is hard in rhino
					var file = new java.io.File(pathModule.join(path, url));
					var length = file.length();
					var fis = new java.io.FileInputStream(file);
					var bytes = java.lang.reflect.Array.newInstance(java.lang.Byte.TYPE, length); 
					fis.read(bytes, 0, length);
					var jsBytes = new Array(length);
					for(var i = 0; i < bytes.length;i++){
						var singleByte = bytes[i];
						if(singleByte < 0){
							singleByte = 256 + singleByte;
						}
						jsBytes[i] = singleByte;
					}
					var moduleText =base64Module.encode(jsBytes);
				}else{
					// in node base64 encoding is easy
					var moduleText = fs.readFileSync(pathModule.join(path, url)).toString("base64");
				}
				return 'url(data:' + (mimeTypes[suffix] || 'application/octet-stream') + 
							';base64,' + moduleText + ')';
			}
			// or we adjust the URL
			return 'url("' + pathModule.join(relativePath, url).replace(/\\/g, '/') + '")';
		});
	}
	parse.getStyleSheet = function(importRule, sequence, styleSheet){
		var path = pathModule.resolve(styleSheet.href, sequence[1].value);
		var localSource = '';
		try{
			localSource = fs.readFileSync(path).toString("utf-8");
		}catch(e){
			console.error(e);
		}
		//browserCss.push(correctUrls(localSource, path));
		return {
			localSource: localSource,
			href: path || '.',
			insertRule: insertRule,
			cssRules: []
		}
	};
	var browserCss = [];//[correctUrls(cssText, basePath + "placeholder.css")];
	var xstyleCss = [];
	var rootRule = new XRule;
	rootRule.root = true;
	parse(rootRule, cssText, {href:basePath || '.', cssRules:[], insertRule: insertRule});
	rootRule.properties = {
		Math:1,
		require:1,
		item: 1,
		'native': 1,
		prefixed: 1
	}
	function visit(parent){
		//browserCss.push(parent.selector + '{' + parent.cssText + '}'); 
		/*for(var i in parent.variables){
			if(!intrinsicVariables.hasOwnProperty(i)){
				xstyleCss.push(i,'=',parent.variables[i]);
			}
		}*/
	}
	visit(rootRule);
	//console.log('browserCss', browserCss);
	return {
		standardCss: minify(browserCss.join('')),
		xstyleCss: xstyleCss.join(';'),
		requiredModules: requiredModules
	};
}
if(typeof module != 'undefined' && require.main == module){
	main.apply(this, process.argv.slice(2));
}
