/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestTranslate := MODULE

  EXPORT TestConst := MODULE
    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.Translate(U'',U'',U'')+U'!' = U'!', CONST);
    EXPORT Test02 := ASSERT(Uni.Translate(U'x',U'x',U'')+U'!' = U'x!', CONST);
    EXPORT Test03 := ASSERT(Uni.Translate(U'x',U'',U'u')+U'!' = U'x!', CONST);
    EXPORT Test04 := ASSERT(Uni.Translate(U'x',U'',U'')+U'!' = U'x!', CONST);
    EXPORT Test05 := ASSERT(Uni.Translate(U'',U'x',U'u')+U'!' = U'!', CONST);
    EXPORT Test06 := ASSERT(Uni.Translate(U'',U'x',U'')+U'!' = U'!', CONST);
    EXPORT Test07 := ASSERT(Uni.Translate(U'',U'',U'x')+U'!' = U'!', CONST);
    //Check action on a string containing a single word - with various whitespace.
    EXPORT Test08 := ASSERT(Uni.Translate(U'x',U'x',U'u')+U'!' = U'u!');
    EXPORT Test09 := ASSERT(Uni.Translate(U'x',U'x',U'uu')+U'!' = U'x!');
    EXPORT Test10 := ASSERT(Uni.Translate(U'x',U'xx',U'u')+U'!' = U'x!');
    EXPORT Test11 := ASSERT(Uni.Translate(U'x',U'xy',U'uv')+U'!' = U'u!');
    //Functionality Test.
    EXPORT Test12 := ASSERT(Uni.Translate(U'x',U'xx',U'uv')+U'!' = U'v!');
    EXPORT Test13 := ASSERT(Uni.Translate(U' \377ABCDEF FEDCBA ', 'AB', '$!')+U'!' = U' \377$!CDEF FEDC!$ !');
    EXPORT Test14 := ASSERT(Uni.Translate(U'xy',U'xy',U'yw')+U'!' = U'yw!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.Translate(U' xyz abc ',U'xyzabc',U'uvwdef')+U'!' = U' uvw def !');
    EXPORT Test16 := ASSERT(Uni.Translate(U' xyz abc ',U'zyx',U'wvu')+U'!' = U' uvw abc !');
    EXPORT Test17 := ASSERT(Uni.Translate(U' xyz abc ',U'a',U'd')+U'!' = U' xyz dbc !');
    EXPORT Test18 := ASSERT(Uni.Translate(U' xyz abc ',U'b',U'e')+U'!' = U' xyz aec !');
    EXPORT Test19 := ASSERT(Uni.Translate(U' xyz abc ',U'xb z',U'ue w')+U'!' = U' uyw aec !');
    EXPORT Test20 := ASSERT(Uni.Translate(U' xyz abc ',U'xyz',U'')+U'!' = U' xyz abc !');
    //Check action on a string containing punctuation characters.
    EXPORT Test21 := ASSERT(Uni.Translate(U' ,&%$@ ',U',$',U'uv')+U'!' = U' u&%v@ !');
    EXPORT Test22 := ASSERT(Uni.Translate(U' xyz zyx ',U'xxx',U'$%!')+U'!' = U' !yz zy! !');
    //Check action on a string containing an apostrophe.
    EXPORT Test23 := ASSERT(Uni.Translate(U'I couldn\'t hear you!',U'\'',U'X')+U'!' = U'I couldnXt hear you!!');
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test24 := ASSERT(Uni.Translate(U'1 234 123abc 23.6 abc123',U'213',U'546')+U'!' = U'4 564 456abc 56.6 abc456!');
    //Test other space characters (< 0x20).
    EXPORT Test25 := ASSERT(Uni.Translate(U'  a n\nt \t  def    ',U'n',U'X')+U'!' = U'  a X\nt \t  def    !');
    EXPORT Test26 := ASSERT(Uni.Translate(U'  a n\nt \t  def    ',U't',U'X')+U'!' = U'  a n\nX \t  def    !');
    //Check action on a string containing latin diacritical marks.
    EXPORT Test27 := ASSERT(Uni.Translate(U'À à',U'À',U'x')+U'!' = U'x à!');
    EXPORT Test28 := ASSERT(Uni.Translate(U'ȭ š',U'ȭ',U'x')+U'!' = U'x š!');
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test29 := ASSERT(Uni.Translate(U'El difunto cambió las niñas',U'óxña',U'UvWx')+U'!' = U'El difunto cxmbiU lxs niWxs!');
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test30 := ASSERT(Uni.Translate(U'我是電腦',U'是',U'X')+U'!' = U'我X電腦!');
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test31 := ASSERT(Uni.Translate(U' Έρχεσαι συχνά εδώ; ',U'σχά',U'123')+U'!' = U' Έρ2ε1αι 1υ2ν3 εδώ; !');
    //Testcases 32 and 33 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test32 := ASSERT(Uni.Translate(U'صباح الخير',U'ص',U'X')+U'!' = U'Xباح الخير!');
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test33 := ASSERT(Uni.Translate(U'קָמָץ שִׁי״ן',U'קי',U'YX')+U'!' = U'Yָמָץ שִׁX״ן!');
    //Check action on surrogate pairs.
    EXPORT Test34 := ASSERT(Uni.Translate(U'x𐐀',U'x𐐀',U'\uD808\uDC00y')+U'!' = U'\uD808\uDC00y!');
    EXPORT Test35 := ASSERT(Uni.Translate(U'𐐀',U'𐐀',U'u')+'!' = U'u!');
    EXPORT Test36 := ASSERT(Uni.Translate(U'x',U'x',U'𐐀')+'!' = U'𐐀!');
    EXPORT Test37 := ASSERT(Uni.Translate(U'𐐀',U'𐐀',U'xy')+'!' = U'𐐀!');
    //Check action on replacing characters with combining characters like a cedilla.
    EXPORT Test38 := ASSERT(Uni.Translate(U'CX',U'X',U'̧')+'!' = U'Ç!');
  END;

END;
