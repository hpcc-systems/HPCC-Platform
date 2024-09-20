// FUNCTIONMACRO for testing signing of ECL code
EXPORT Outer_Macro(num) := FUNCTIONMACRO

    UNSIGNED8 _Inner(UNSIGNED8 n) := EMBED(C++)
        #option pure;
        return n;
    ENDEMBED;

    RETURN _Inner(num);
ENDMACRO;
