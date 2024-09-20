//class=codesigning
//nokey
//fail
//version version=0
//version version=1
//version version=2
//version version=3

import  ^ as root;
v := #IFDEFINED(root.version, 0);

IMPORT Std, $.signing;

#IF (v = 0)
    signing.Outer_Nested_Macro(4);
#ELSEIF (v = 1)
    signing.Outer_Macro(4);
#ELSEIF (v = 2)
    signing.Outer_Function(4);
#ELSEIF (v = 3)
    UNSIGNED8 Factorial(UNSIGNED1 n) := EMBED(C++)
        #option pure;
        unsigned __int64 result = 0;

        if (n <= 65)
        {
            result = 1;

            for (unsigned int x = 2; x <= n; x++)
            {
                result *= x;
            }
        }
        
        return result;
    ENDEMBED;

    Factorial(5);
#END
 