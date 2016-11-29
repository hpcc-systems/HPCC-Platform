<Archive>
<Query>

//#import(legacy)

#ISDEFINED(StringLib);

//#import(modern)

NOT #ISDEFINED(StringLib);

x() := macro
//#import(legacy)
#ISDEFINED(StringLib);
endmacro;

x();

NOT #ISDEFINED(StringLib);

import mod1;
mod1.attr1;

output(1 +
//#99 balloons
2 = 3);

output(TRUE AND
//#import(  legacy  )
#ISDEFINED(StringLib) AND
//#import( modern )
NOT #ISDEFINED(StringLib));


</Query>
    <Module name="mod1">
        <Attribute name="attr1">

            //#import(legacy)
            output(#ISDEFINED(mod1));
            output(#ISDEFINED(attr2));

            //#import(modern)
            output(NOT #ISDEFINED(mod1));
            output(NOT #ISDEFINED(attr2));
        </Attribute>
        <Attribute name="attr2">
            export attr2 := 3;
        </Attribute>
    </Module>
</Archive>
