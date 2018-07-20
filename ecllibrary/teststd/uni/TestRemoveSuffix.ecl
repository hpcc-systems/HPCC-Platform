/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestRemoveSuffix := MODULE

  EXPORT TestConst := MODULE

    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.RemoveSuffix('','','') = '', CONST);
    EXPORT Test03 := ASSERT(Uni.RemoveSuffix('x','','') = 'x', CONST);
    EXPORT Test05 := ASSERT(Uni.RemoveSuffix('','x','') = '', CONST);
    EXPORT Test07 := ASSERT(Uni.RemoveSuffix('','','x') = '', CONST);
    EXPORT Test02 := ASSERT(Uni.RemoveSuffix('x','x','') = '', CONST);
    //Check action on a string containing a single word - with various whitespace.
    EXPORT Test08 := ASSERT(Uni.RemoveSuffix(' x ','  x ','') = ' x ');
    EXPORT Test09 := ASSERT(Uni.RemoveSuffix(' x','x  ','') = ' ');
    EXPORT Test10 := ASSERT(Uni.RemoveSuffix('x ','x ','') = '');
    EXPORT Test11 := ASSERT(Uni.RemoveSuffix('  x','x','') = ' ');
    //Functionality Test.
    EXPORT Test12 := ASSERT(Uni.RemoveSuffix('x','xx','') = 'x');
    EXPORT Test14 := ASSERT(Uni.RemoveSuffix('','','yw') = '');
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.RemoveSuffix(' xyz abc','xyz abc','') = ' ');
    EXPORT Test16 := ASSERT(Uni.RemoveSuffix(' xyz abc','xyz','') = ' xyz abc');
    EXPORT Test17 := ASSERT(Uni.RemoveSuffix(' xyz abc','c','') = ' xyz ab');
    EXPORT Test18 := ASSERT(Uni.RemoveSuffix(' xyz abc','bc','') = ' xyz a');
    EXPORT Test19 := ASSERT(Uni.RemoveSuffix(' xyz abc','ab','') = ' xyz abc');
    EXPORT Test20 := ASSERT(Uni.RemoveSuffix(' xyz abc','b c','') = ' xyz abc');
    //Check action on a string containing punctuation characters.
    EXPORT Test21 := ASSERT(Uni.RemoveSuffix(',&%$@','$@','') = ',&%');
    //Check action on a string containing an apostrophe.
    EXPORT Test23 := ASSERT(Uni.RemoveSuffix('I couldn\'t hear you!','couldn\'t hear you!','') = 'I ');
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test24 := ASSERT(Uni.RemoveSuffix('123abc 23.6 abc123','23.6 abc123','') = '123abc ');
    //Test other space characters (< 0x20) - implicit trim semantics have changed in 7.0.
    EXPORT Test26 := ASSERT(Uni.RemoveSuffix('\nt\t','t','') = '\nt\t');
    EXPORT Test26a := ASSERT(Uni.RemoveSuffix('\nt\t','\t','') = '\nt');
    //Check action on a string containing latin diacritical marks.
    EXPORT Test27 := ASSERT(Uni.RemoveSuffix(U'À à',U'à','') = U'À ');
    EXPORT Test28 := ASSERT(Uni.RemoveSuffix(U'ȭ š',U'š','') = U'ȭ ');
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test29 := ASSERT(Uni.RemoveSuffix(U'El difunto cambió las niñas',U'niñas','') = U'El difunto cambió las ');
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test30 := ASSERT(Uni.RemoveSuffix(U'我是電腦',U'腦','') = U'我是電');
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test31 := ASSERT(Uni.RemoveSuffix(U'Έρχεσαι συχνά εδώ;',U'εδώ;','') = U'Έρχεσαι συχνά ');
    //Testcases 32 and 33 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test32 := ASSERT(Uni.RemoveSuffix(U'صباح الخير',U'الخير','') = U'صباح');
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test33 := ASSERT(Uni.RemoveSuffix(U'קָמָץ שִׁי״ן',U'שִׁי״ן','') = U'קָמָץ');
    //Check action on surrogate pairs.
    EXPORT Test34 := ASSERT(Uni.RemoveSuffix(U'x𐐀',U'x𐐀','') = U'');
    EXPORT Test35 := ASSERT(Uni.RemoveSuffix(U'𐐀',U'𐐀x','') = U'𐐀');
    EXPORT Test37 := ASSERT(Uni.RemoveSuffix(U'x𐐀',U'𐐀','') = U'x');
    //Check action with normalization forms
    EXPORT Test39 := ASSERT(Uni.RemoveSuffix(U'Ç̌',U'Ç̌','') = U'');
    EXPORT Test40 := ASSERT(Uni.RemoveSuffix(U'Ç̌',U'Ç̌','NFC') = U'');
    DATA r1 := x'43002703';
    SHARED UNICODE t1a := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    SHARED UNICODE t2a := TRANSFER(r2, UNICODE);
    EXPORT Test41 := ASSERT(Uni.RemoveSuffix(t1a,t2a,'NFC') = U'');
    EXPORT Test42 := ASSERT(Uni.RemoveSuffix(t1a,t2a,'NFD') = U'');
    EXPORT Test43 := ASSERT(Uni.RemoveSuffix(t1a,t2a,'NFKC') = U'');
    EXPORT Test44 := ASSERT(Uni.RemoveSuffix(t1a,t2a,'NFKD') = U'');
    DATA r1 := x'43000C032703';
    SHARED UNICODE t1b := TRANSFER(r1, UNICODE);
    DATA r2 := x'0C032703';
    SHARED UNICODE t2b := TRANSFER(r2, UNICODE);
    EXPORT Test45 := ASSERT(Uni.RemoveSuffix(t1b,t2b,'NFC') = U'Ç̌');
    EXPORT Test46 := ASSERT(Uni.RemoveSuffix(t1b,t2b,'NFD') = U'C');
    //Test various combinations of space padding.
    DATA r1 := x'4300270320002000';
    SHARED UNICODE t1c := TRANSFER(r1, UNICODE);
    DATA r2 := x'c70020002000';
    SHARED UNICODE t2c := TRANSFER(r2, UNICODE);
    EXPORT Test47 := ASSERT(Uni.RemoveSuffix(t1c,t2c,'NFC') = U'');
    EXPORT Test48 := ASSERT(Uni.RemoveSuffix(t1a,t2c,'NFC') = U'');
    EXPORT Test49 := ASSERT(Uni.RemoveSuffix(t1c,t2a,'NFC') = U'');
  END;
END;
