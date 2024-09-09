EXPORT Outer_Nested_Macro(num) := FUNCTIONMACRO
    EXPORT _Inner_Macro(_num) := FUNCTIONMACRO
        UNSIGNED8 _Inner(UNSIGNED8 n) := EMBED(C++)
            #option pure;
            return n;
        ENDEMBED;
        RETURN _Inner(_num);
    ENDMACRO;
    RETURN _Inner_Macro(num);
ENDMACRO;
