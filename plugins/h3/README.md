ECL Example Plugin
================

This is the ECL plugin to utilize the (add plugin name).
It utilizes the (Add related API details).

Installation and Dependencies
----------------------------

To build the h3 plugin with the HPCC-Platform, https://github.com/uber/h3 is required.
```
git submodule update --init --recursive
```

*Note:* Add notes such as min versions etc.


Getting started
---------------

(Add relevant content)

The Actual Plugin
-----------------

(Add relevant content)

###Sub Section

(Add details)

###An ECL Example
```c
IMPORT h3 FROM lib_h3;
IMPORT Wrapper FROM lib_h3;

myWrapper := Wrapper('param1', 'param2');

UNSIGNED4 param3 := 7;
example1 := myWrapper.func1(param3);
example2 := myWrapper.func2('MaKEALLlOWeRCASe');

OUTPUT(example1);
OUTPUT(example2);
```
Yields the results '8' and 'makealllowercase'

etc etc.

Behaviour and Implementation Details
------------------------------------

(Add relevant content)
