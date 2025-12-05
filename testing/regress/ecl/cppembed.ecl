#OPTION('compileOptions', '-std=c++17');

STRING mol() := EMBED(C++)
    #include "cppembed.hpp"

#body
    mol(__lenResult, __result);
ENDEMBED;

mol();
