// Embedded function for testing signing of ECL code
EXPORT UNSIGNED8 Outer_Function(UNSIGNED8 n) := EMBED(C++)
    #option pure;
    return n;
ENDEMBED;
