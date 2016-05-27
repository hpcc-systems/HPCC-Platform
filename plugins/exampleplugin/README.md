ECL Example Plugin
================

This is the ECL plugin to utilize the Blah Blah Blah.
It utilizes the API Blah Blah Blah.

Installation and Dependencies
----------------------------

To build the <plugin name> plugin with the HPCC-Platform, <dependency> is required.
```
sudo apt-get install <dependency>
```

*Note:* Add notes such as min versions etc.


Getting started
---------------

Blah Blah Blah.

The Actual Plugin
-----------------

Blah Blah Blah.

###Sub Section

Blah Blah Blah.

###An ECL Example
```c
IMPORT example-plugin FROM lib_example-plugin;
IMPORT Wrapper FROM lib_example-plugin;

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

Blah Blah Blah.