/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

// Java like string buffer class
function StringBuffer() { this.buffer = []; }
StringBuffer.prototype.append = function(string) {
  this.buffer.push(string);
  return this;
}

StringBuffer.prototype.toString = function() {
  return this.buffer.join("");
}
