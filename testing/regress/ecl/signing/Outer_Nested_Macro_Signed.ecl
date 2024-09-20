-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA512

EXPORT Outer_Nested_Macro_Signed(num) := FUNCTIONMACRO
    EXPORT _Inner_Macro(_num) := FUNCTIONMACRO
        UNSIGNED8 _Inner(UNSIGNED8 n) := EMBED(C++)
            #option pure;
            return n;
        ENDEMBED;
        RETURN _Inner(_num);
    ENDMACRO;
    RETURN _Inner_Macro(num);
ENDMACRO;
-----BEGIN PGP SIGNATURE-----

iQGzBAEBCgAdFiEE4djZnkL+KNmMiFOew35zYQd+QVQFAmboJHYACgkQw35zYQd+
QVRU0gv9FZQKvDRrfXxpaaRFNYIimImdD5negdWKNv1zj5bXX9hDJvH01L2wdv88
Km/Y1jP986rnJaCUd7JDuE0sUhgAT3w00foaJ1zUIAYfIYAJiFBwKg9fuo+z7+uu
Lh6280AqS1jwJx7mjcmpmCRlCcb2R1VkUv6j0I75ePkwNndSJQluiY/8pPeqF068
LSFmZmAKui4NdFHSjgVW2peI8v0rI/A71UkWfBuLqVSlsTIw4EKrenSpiuyOzij6
d5c3jQETgIxf/6ppa5pEWcs1js41tqDtdwuEf1ZBkw1Hf7D7XOvlWv9eOg/4RFwD
Km8Ba0RMf8G79p7HWEstxrxyd4xJqKjlywePj0IlDx4bjzzuQbCZwNppixdshMw5
bJkpP0zDEoIst87/T9WXUYJsJ7yl/zNuHDNXu8wXNzOzUbQ6MYTajLEb9GezqJuK
1UIUBfQRbEbTSJHgwA7X37aeNG/kskkAfYax5vUpFe0qqys/bEIvE0ke7P4ByGj7
vWGd5LVx
=cbyj
-----END PGP SIGNATURE-----
