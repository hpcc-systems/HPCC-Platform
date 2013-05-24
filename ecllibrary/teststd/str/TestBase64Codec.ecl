/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.  All rights reserved.
############################################################################## */
IMPORT Std.Str;

EXPORT TestBase64Codec := MODULE

  EXPORT TestConst := MODULE
    EXPORT Test01 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'ca')) = x'ca');
    EXPORT Test02 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafe')) = x'cafe');
    EXPORT Test03 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafeba')) = x'cafeba');
    EXPORT Test04 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafebabe')) = x'cafebabe');
    EXPORT Test05 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafebabeca')) = x'cafebabeca');
    EXPORT Test06 := ASSERT(Str.DecodeBase64(Str.EncodeBase64(x'cafebabecafe')) = x'cafebabecafe');
    
    EXPORT Test07 := ASSERT(Str.EncodeBase64(x'ca') = 'yg==');
    EXPORT Test08 := ASSERT(Str.EncodeBase64(x'cafe') = 'yv4=');
    EXPORT Test09 := ASSERT(Str.EncodeBase64(x'cafeba') = 'yv66');
    EXPORT Test10 := ASSERT(Str.EncodeBase64(x'cafebabe') = 'yv66vg==');
    EXPORT Test11 := ASSERT(Str.EncodeBase64(x'cafebabeca') = 'yv66vso=');
    EXPORT Test12 := ASSERT(Str.EncodeBase64(x'cafebabecafe') = 'yv66vsr+');

    EXPORT Test13 := ASSERT(Str.DecodeBase64('yg==') = x'ca');
    EXPORT Test14 := ASSERT(Str.DecodeBase64('yv4=') = x'cafe');
    EXPORT Test15 := ASSERT(Str.DecodeBase64('yv66') = x'cafeba');
    EXPORT Test16 := ASSERT(Str.DecodeBase64('yv66vg==') = x'cafebabe');
    EXPORT Test17 := ASSERT(Str.DecodeBase64('yv66vso=') = x'cafebabeca');
    EXPORT Test18 := ASSERT(Str.DecodeBase64('yv66vsr+') = x'cafebabecafe');

    /* Invalid printable character e.g.'@' or '#' in the encoded string */	
    EXPORT Test19 := ASSERT(Str.DecodeBase64('yg@=') = d'') ;
    EXPORT Test20 := ASSERT(Str.DecodeBase64('#yg=') = d'') ;
    EXPORT Test21 := ASSERT(Str.DecodeBase64('y#g=') = d'') ;
    EXPORT Test22 := ASSERT(Str.DecodeBase64('yg#=') = d'') ;
    EXPORT Test23 := ASSERT(Str.DecodeBase64('yg=#') = d'') ;

    /* Invalid non-printable character e.g.'\t' or '\b' in the encoded string */
    EXPORT Test24 := ASSERT(Str.DecodeBase64('y'+x'09'+'g=') = d'');
    EXPORT Test25 := ASSERT(Str.DecodeBase64('y'+x'08'+'g=') = d'');

    /* Missing pad character from encoded string (length error) */	
    EXPORT Test26 := ASSERT(Str.DecodeBase64('yg=') = d'') ;
    EXPORT Test27 := ASSERT(Str.DecodeBase64('yv66vg=') = d'');

    /* Length error in encoded string. It doesn't multiple of 4 */	
    EXPORT Test28 := ASSERT(Str.DecodeBase64('yv66v==') = d'') ;
    EXPORT Test29 := ASSERT(Str.DecodeBase64('yv66vg=') = d'') ;

    /* Space(s) in the encoded string */	
    EXPORT Test30 := ASSERT(Str.DecodeBase64('y g==') = x'ca') ;
    EXPORT Test31 := ASSERT(Str.DecodeBase64('y g = = ') = x'ca') ;
	
    /* Long text encoding. Encoder inserts '\n' to break long output lines. */
    EXPORT DATA text := 
           d'Man is distinguished, not only by his reason, but by this singular passion from other animals, '
         + d'which is a lust of the mind, that by a perseverance of delight in the continued and '
         + d'indefatigable generation of knowledge, exceeds the short vehemence of any carnal pleasure.';

    EXPORT STRING encodedText :=
           'TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0'+'\n'
         + 'aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1'+'\n'
         + 'c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0'+'\n'
         + 'aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdl'+'\n'
         + 'LCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=';

    EXPORT TEST32 := ASSERT(Str.EncodeBase64(text) = encodedText);

    /* Long encoded text decoding. Decoder skips '\n' characters in input string. */
    EXPORT DATA decodedText := Str.DecodeBase64(encodedText);
    EXPORT TEST33 := ASSERT(decodedText = text);

    /* Test encoder for zero length and full zero data input string. */
    EXPORT TEST34 := ASSERT(Str.DecodeBase64('') = d'');
    EXPORT TEST35 := ASSERT(Str.DecodeBase64(''+x'00000000') = d'');
  END;

  EXPORT Main := [EVALUATE(TestConst)]; 
END;

