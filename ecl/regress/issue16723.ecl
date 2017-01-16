   STRING str := 'aaa';

   assign(a, b) := MACRO
    a := b
   ENDMACRO;

    irow() := MACRO
       TRANSFORM({STRING one,STRING two}, assign(SELF.one,'str'), assign(SELF.two, str))
    ENDMACRO;

   DATASET([irow()]);
