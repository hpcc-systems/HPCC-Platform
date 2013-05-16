define("xstyle/core/parser", [], function(){
	// regular expressions used to parse CSS
	var cssScan = /\s*((?:[^{\}\[\]\(\)\\'":=;]|\[(?:[^\]'"]|'(?:\\.|[^'])*'|"(?:\\.|[^"])*")\])*)([=:]\??\s*([^{\}\[\]\(\)\\'":;]*))?([{\}\[\]\(\)\\'":;]|$)/g;
									// name: value 	operator
	var singleQuoteScan = /((?:\\.|[^'])*)'/g;
	var doubleQuoteScan = /((?:\\.|[^"])*)"/g;
	var commentScan = /\/\*[\w\W]*?\*\//g; // preserve carriage returns to retain line numbering once we do line based error reporting 
	var operatorMatch = {
		'{': '}',
		'[': ']',
		'(': ')'
	};
	var nextId = 0;
	var trim = ''.trim ? function (str){
		return str.trim();
	} : function(str){
		return str.replace(/^\s+|\s+$/g, '');
	};
	function toStringWithoutCommas(){
		return this.join('');
	}
	function arrayWithoutCommas(array){
		array.toString = toStringWithoutCommas;
		return array;
	}
	function LiteralString(string){
		this.value = string;
	}
	LiteralString.prototype.toString = function(){
		return '"' + this.value.replace(/["\\\n\r]/g, '\\$&') + '"';
	}
	
	function convertCssNameToJs(name){
		return name.replace(/-(\w)/g, function(t, firstLetter){
			return firstLetter.toUpperCase();
		});
	}
	var supportedTags = {};
	function isTagSupported(tag){
		// test to see if a tag is supported by the browser
		if(tag in supportedTags){
			return supportedTags[tag];
		}
		var elementString = (element = document.createElement(tag)).toString();
		return supportedTags[tag] = !(elementString == "[object HTMLUnknownElement]" || elementString == "[object]");
	}
	
	function parse(model, textToParse, styleSheet){
		// tracks the stack of rules as they get nested
		var stack = [model];
		model.parse = parseSheet;
		parseSheet(textToParse, styleSheet);
		function parseSheet(textToParse, styleSheet){
			// parse the CSS, finding each rule
			function addInSequence(operand){
				if(sequence){
					// we had a string so we are accumulated sequences now
					sequence.push ?
						typeof sequence[sequence.length - 1] == 'string' && typeof operand == 'string' ?
							sequence[sequence.length - 1] += operand : // just append the string to last segment
							operand && sequence.push(operand) : // add to the sequence
						typeof sequence == 'string' && typeof operand == 'string' ?
							sequence += operand : // keep appending to the string
							sequence = arrayWithoutCommas([sequence, operand]); // start a new sequence array
				}else{
					sequence = operand;
				}
			}
			textToParse = textToParse.replace(commentScan, '');
			target = model; // start at root
			cssScan.lastIndex = 0; // start at zero
			var ruleIndex = 0, browserUnderstoodRule = true, selector = '', assignNextName = true;
			while(true){
				// parse the next block in the CSS
				// we could perhaps use a simplified regex when we are in a property value 
				var match = cssScan.exec(textToParse);
				// the next block is parsed into several parts that comprise some operands and an operator
				var operator = match[4],
					first = match[1],
					assignment = match[2],
					value = match[3],
					assignmentOperator, name, sequence,
					conditionalAssignment;
				value = value && trim(value);
				
				first = trim(first);
				if(assignNextName){
					// we are at the beginning of a new property
					if(assignment){
						// remember the name, so can assign to it
						name = first;
						//	selector = match[1] + assignment;
						// remember the operator (could be ':' for a property assignment or '=' for a property declaration)
						assignmentOperator = assignment.charAt(0);
						conditionalAssignment = assignment.charAt(1) == '?';
					}else{
						value = first;
					}
					// store in the sequence, the sequence can contain values from multiple rounds of parsing
					sequence = value;
					// we have the assigned property name now, and don't need to assign again
					assignNextName = false;
				}else{
					// subsequent part of a property
					value = assignment ? first + assignment : first;
					// add to the current sequence
					addInSequence(value);	
				}
				if(operator != '{'){
					selector += match[0];
				}
				switch(operator){
					case "'": case '"':
						// encountered a quoted string, parse through to the end of the string and add to the current sequence
						var quoteScan = operator == "'" ? singleQuoteScan : doubleQuoteScan;
						quoteScan.lastIndex = cssScan.lastIndex; // find our current location in the parsing
						var parsed = quoteScan.exec(textToParse);
						if(!parsed){ // no match for the end of the string
							error("unterminated string");
						}
						var str = parsed[1]; // the contents of the string
						// move the css parser up to the end of the string position
						cssScan.lastIndex = quoteScan.lastIndex; 
						// push the string on the current value and keep parsing
						addInSequence(new LiteralString(str));
						selector += parsed[0];
						continue;
					case '\\':
						// escaping
						var lastIndex = quoteScan.lastIndex++;
						// add the escaped character to the sequence
						addInSequence(textToParse.charAt(lastIndex));
						continue;
					case '(': case '{': case '[':
						// encountered a new contents of a rule or a function call
						var newTarget;
						var doExtend = false;
						if(operator == '{'){
							// it's a rule
							assignNextName = true; // enter into the beginning of property mode
							// normalize the selector
							if(assignmentOperator == ':'){
								first += assignment;
							}
							selector = trim((selector + first).replace(/\s+/g, ' ').replace(/([\.#:])\S+|\w+/g,function(t, operator){
								// make tag names be lower case 
								return operator ? t : t.toLowerCase();
							}));	
							// add this new rule to the current parent rule
							addInSequence(newTarget = target.newRule(selector));
							
							// todo: check the type
							if(assignmentOperator == '='){
								browserUnderstoodRule = false;
								sequence.creating = true;
								if(value){
									// extend the referenced target value
									doExtend = true;
								}
							}
							if(assignmentOperator == ':' && !target.root){
								// we will assume that we are in a property in this case. We will need to do some adjustments to support nested pseudo selectors
								sequence.creating = true;
							}
							var nextRule = null;
							var lastRuleIndex = ruleIndex;
							if(target.root && browserUnderstoodRule){
								// we track the native CSSOM rule that we are attached to so we can add properties to the correct rule
								var cssRules = styleSheet.cssRules || styleSheet.rules;
								while((nextRule = cssRules[ruleIndex++])){									
									if(nextRule.selectorText == selector){
										// found it
										newTarget.cssRule = nextRule;
										break;
									}
								}
							}
							if(!nextRule){
								// didn't find it
								newTarget.ruleIndex = ruleIndex = lastRuleIndex;
								newTarget.styleSheet = styleSheet;									
								//console.warn("Unable to find rule ", selector, "existing rule did not match", nextRule.selectorText); 
							}
							if(sequence.creating){
								// in generation, we auto-generate selectors so we can reference them
								newTarget.selector = '.' + (assignmentOperator == '=' ? first.replace(/[^\w-]/g,'') : '') + '-x-' + nextId++;
							}else{							
								newTarget.selector = target.root ? selector : target.selector + ' ' + selector;
							}
							selector = '';
						}else{
							// it's a call, add it in the current sequence
							var callParts = value.match(/(.*?)([\w-]*)$/);
							addInSequence(newTarget = target.newCall(callParts[2], sequence, target));
							newTarget.ref = target.getDefinition(callParts[2]);
							(sequence.calls || (sequence.calls = [])).push(newTarget);
						}
						// make the parent reference
						newTarget.parent = target;
						if(doExtend){
							value.replace(/(?:^|,|>)\s*([\w-]+)/g, function(t, base){
								var ref = target.getDefinition(base);
								if(ref){
									ref.extend(newTarget, true);
								}else{
									// extending a native element
									newTarget.tagName = base;
									if(!isTagSupported(base)){
										error("Extending undefined definition " + base);
									}
								}
							});
						}
						
						// store the current state information so we can restore it when exiting this rule or call
						target.currentName = name;
						target.currentSequence = sequence;
						target.assignmentOperator = assignmentOperator;
						// if it has a pseudo, call the pseudo handler
						if(assignmentOperator == ':' && operator == '{'){
							// TODO: use when()
							var pseudoHandler = target.getDefinition(':' + value);
							if(pseudoHandler && pseudoHandler.pseudo){
								pseudoHandler.pseudo(target, value);
							}
						}

						// add to the stack
						stack.push(target = newTarget);
						target.operator = operator;
						target.start = cssScan.lastIndex;
						name = null;
						sequence = null;
						continue;
				}
				if(sequence){
					// now see if we need to process an assignment or directive
					var first = sequence[0] || sequence;
					if(first.charAt && first.charAt(0) == "@"){
						// it's a directive
						if(sequence[0].slice(1,7) == "import"){
							// get the stylesheet
							var importedSheet = parse.getStyleSheet((styleSheet.cssRules || styleSheet.imports)[ruleIndex++], sequence, styleSheet);
							//waiting++;
							// preserve the current index, as we are using a single regex to be shared by all parsing executions
							var currentIndex = cssScan.lastIndex;
							// parse the imported stylesheet
							parseSheet(importedSheet.localSource, importedSheet);
							// now restore our state
							cssScan.lastIndex = currentIndex;
						}
					}else if(assignmentOperator){
						// need to do an assignment
						try{
							target[assignmentOperator == ':' ? 'setValue' : 'declareProperty'](name, sequence, conditionalAssignment);
						}catch(e){
							error(e);
						}
					}
				}
				switch(operator){
					case ':':
						// assignment can happen after a property declaration
						if(assignmentOperator == '='){
							assignNextName = true;
							assignmentOperator = ':';
						}else{
							// a double pseudo
							addInSequence(':');
						}
						break;
					case '}': case ')': case ']':
						// end of a rule or function call
						// clear the name now
						if(operatorMatch[target.operator] != operator){
							error('Incorrect opening operator ' + target.operator + ' with closing operator ' + operator); 
						}
						name = null;
						// record the cssText
						var ruleCssText = textToParse.slice(target.start, cssScan.lastIndex - 1);
						target.cssText = target.cssText ? 
							target.cssText + ';' + ruleCssText : ruleCssText;
							
						if(operator == '}'){
							
							if(lastOperator == '}'){
								var parentSelector = target.parent.selector;
								if(parentSelector && !parentSelector.charAt(0) == '@'){
									// we throw an error for this because it so catastrophically messes up the browser's CSS parsing, not because we can't handle it fine
									error("A nested rule must end with a semicolon");
								}
							}
							// if it is rule, call the rule handler 
							target.onRule(target.selector, target);
							// TODO: remove this conditional, now that we use assignment
							/*if(target.selector.slice(0,2) != "x-"){// don't trigger the property for the property registration
								target.eachProperty(onProperty);
							}*/
							browserUnderstoodRule = true;
							selector = '';
						}/*else if(operator == ')'){
							// call handler
							onCall(target.caller, target);
						}*/
						// now pop the call or rule off the stack and restore the state
						stack.pop();
						target = stack[stack.length - 1];				
						sequence = target.currentSequence;
						name = target.currentName;
						assignmentOperator = target.assignmentOperator;
						if(target.root && operator == '}'){
							// CSS ASI
							if(assignmentOperator){
								// may still need to do an assignment
								try{
									target[assignmentOperator == ':' ? 'setValue' : 'declareProperty'](name, sequence[1] || sequence, conditionalAssignment);
								}catch(e){
									error(e);
								}
							}
							assignNextName = true;
							assignmentOperator = false;
						}
						break;
					case "":
						// no operator means we have reached the end of the text to parse
						return;
					case ';':
						// end of a property, end the sequence return to the beginning of propery mode
						sequence = null;
						assignNextName = true;
						browserUnderstoodRule = false;
						assignmentOperator = false;
						selector = '';
				}
				var lastOperator = operator;
			}
		}
		function error(e){
			console.error(e.message || e, "line " + textToParse.slice(0, cssScan.lastIndex).split('\n').length + " in " + (styleSheet.href || "in-page stylesheet"));
			if(e.stack){
				console.error(e.stack);
			}			
		}
	}
	return parse;
});