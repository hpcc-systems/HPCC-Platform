/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestTranslate := MODULE

  EXPORT TestConst := MODULE
    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.Translate('','','')+'!' = '!', CONST);
    EXPORT Test02 := ASSERT(Uni.Translate('x','x','')+'!' = 'x!', CONST);
    EXPORT Test03 := ASSERT(Uni.Translate('x','','u')+'!' = 'x!', CONST);
    EXPORT Test04 := ASSERT(Uni.Translate('x','','')+'!' = 'x!', CONST);
    EXPORT Test05 := ASSERT(Uni.Translate('','x','u')+'!' = '!', CONST);
    EXPORT Test06 := ASSERT(Uni.Translate('','x','')+'!' = '!', CONST);
    EXPORT Test07 := ASSERT(Uni.Translate('','','x')+'!' = '!', CONST);
    //Check action on a string containing a single word - with various whitespace.
    EXPORT Test08 := ASSERT(Uni.Translate('x','x','u')+'!' = 'u!');
    EXPORT Test09 := ASSERT(Uni.Translate('x','x','uu')+'!' = 'x!');
    EXPORT Test10 := ASSERT(Uni.Translate('x','xx','u')+'!' = 'x!');
    EXPORT Test11 := ASSERT(Uni.Translate('x','xy','uv')+'!' = 'u!');
    //Functionality Test.
    EXPORT Test12 := ASSERT(Uni.Translate('x','xx','uv')+'!' = 'v!');
    EXPORT Test13 := ASSERT(Uni.Translate(' \377ABCDEF FEDCBA ', 'AB', '$!')+'!' = ' \377$!CDEF FEDC!$ !', CONST);
    EXPORT Test14 := ASSERT(Uni.Translate('xy','xy','yw')+'!' = 'yw!');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.Translate(' xyz abc ','xyzabc','uvwdef')+'!' = ' uvw def !');
    EXPORT Test16 := ASSERT(Uni.Translate(' xyz abc ','zyx','wvu')+'!' = ' uvw abc !');
    EXPORT Test17 := ASSERT(Uni.Translate(' xyz abc ','a','d')+'!' = ' xyz dbc !');
    EXPORT Test18 := ASSERT(Uni.Translate(' xyz abc ','b','e')+'!' = ' xyz aec !');
    EXPORT Test19 := ASSERT(Uni.Translate(' xyz abc ','xb z','ue w')+'!' = ' uyw aec !');
    EXPORT Test20 := ASSERT(Uni.Translate(' xyz abc ','xyz','')+'!' = ' xyz abc !');
    //Check action on a string containing punctuation characters.
    EXPORT Test21 := ASSERT(Uni.Translate(' ,&%$@ ',',$','uv')+'!' = ' u&%v@ !');
    EXPORT Test22 := ASSERT(Uni.Translate(' xyz zyx ','xxx','$%!')+'!' = ' !yz zy! !');
    //Check action on a string containing an apostrophe.
    EXPORT Test23 := ASSERT(Uni.Translate('I couldn\'t hear you!','\'','X')+'!' = 'I couldnXt hear you!!');
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test24 := ASSERT(Uni.Translate('1 234 123abc 23.6 abc123','213','546')+'!' = '4 564 456abc 56.6 abc456!');
    //Test other space characters (< 0x20).
    EXPORT Test25 := ASSERT(Uni.Translate('  a n\nt \t  def    ','n','X')+'!' = '  a X\nt \t  def    !');
    EXPORT Test26 := ASSERT(Uni.Translate('  a n\nt \t  def    ','t','X')+'!' = '  a n\nX \t  def    !');
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
    EXPORT Test34 := ASSERT(Uni.Translate(U'x𐐀',U'x𐐀',U'𒀀y')+'!' = U'𒀀y!');
    EXPORT Test35 := ASSERT(Uni.Translate(U'𐐀',U'𐐀',U'u')+'!' = U'u!');
    EXPORT Test36 := ASSERT(Uni.Translate(U'x',U'x',U'𐐀')+'!' = U'𐐀!');
    EXPORT Test37 := ASSERT(Uni.Translate(U'𐐀',U'𐐀',U'xy')+'!' = U'𐐀!');
	//Check action on combining cedilla to ensure correct Normalization.
    EXPORT Test38 := ASSERT(Uni.Translate(U'CX',U'X',U'̧')+'!' = U'Ç!');
  END;

END;
