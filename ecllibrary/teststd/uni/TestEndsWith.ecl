/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestEndsWith := MODULE

  EXPORT TestConst := MODULE

    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.EndsWith('','','') = FALSE, CONST);
    EXPORT Test03 := ASSERT(Uni.EndsWith('x','','') = FALSE, CONST);
    EXPORT Test05 := ASSERT(Uni.EndsWith('','x','') = FALSE, CONST);
    EXPORT Test07 := ASSERT(Uni.EndsWith('','','x') = FALSE, CONST);
    EXPORT Test02 := ASSERT(Uni.EndsWith('x','x','') = TRUE, CONST);
    //Check action on a string containing a single word - with various whitespace.
    EXPORT Test08 := ASSERT(Uni.EndsWith(' x ','  x ','') = FALSE);
    EXPORT Test09 := ASSERT(Uni.EndsWith(' x','x  ','') = TRUE);
    EXPORT Test10 := ASSERT(Uni.EndsWith('x ','x ','') = TRUE);
    EXPORT Test11 := ASSERT(Uni.EndsWith('  x','x','') = TRUE);
    //Functionality Test.
    EXPORT Test12 := ASSERT(Uni.EndsWith('x','xx','') = FALSE);
    EXPORT Test14 := ASSERT(Uni.EndsWith('','','yw') = FALSE);
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.EndsWith(' xyz abc','xyz abc','') = TRUE);
    EXPORT Test16 := ASSERT(Uni.EndsWith(' xyz abc','xyz','') = FALSE);
    EXPORT Test17 := ASSERT(Uni.EndsWith(' xyz abc','c','') = TRUE);
    EXPORT Test18 := ASSERT(Uni.EndsWith(' xyz abc','bc','') = TRUE);
    EXPORT Test19 := ASSERT(Uni.EndsWith(' xyz abc','ab','') = FALSE);
    EXPORT Test20 := ASSERT(Uni.EndsWith(' xyz abc','b c','') = FALSE);
    //Check action on a string containing punctuation characters.
    EXPORT Test21 := ASSERT(Uni.EndsWith(',&%$@','$@','') = TRUE);
    //Check action on a string containing an apostrophe.
    EXPORT Test23 := ASSERT(Uni.EndsWith('I couldn\'t hear you!','couldn\'t hear you!','') = TRUE);
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test24 := ASSERT(Uni.EndsWith('123abc 23.6 abc123','23.6 abc123','') = TRUE);
    //Test other space characters (< 0x20) - implicit trim semantics have changed in 7.0.
    EXPORT Test26 := ASSERT(Uni.EndsWith('\nt\t','t','') = FALSE);
    EXPORT Test26a := ASSERT(Uni.EndsWith('\nt\t','t\t','') = TRUE);
    //Check action on a string containing latin diacritical marks.
    EXPORT Test27 := ASSERT(Uni.EndsWith(U'À à',U'à','') = TRUE);
    EXPORT Test28 := ASSERT(Uni.EndsWith(U'ȭ š',U'š','') = TRUE);
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test29 := ASSERT(Uni.EndsWith(U'El difunto cambió las niñas',U'niñas','') = TRUE);
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test30 := ASSERT(Uni.EndsWith(U'我是電腦',U'腦','') = TRUE);
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test31 := ASSERT(Uni.EndsWith(U'Έρχεσαι συχνά εδώ;',U'εδώ;','') = TRUE);
    //Testcases 32 and 33 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test32 := ASSERT(Uni.EndsWith(U'صباح الخير',U'الخير','') = TRUE);
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test33 := ASSERT(Uni.EndsWith(U'קָמָץ שִׁי״ן',U'שִׁי״ן','') = TRUE);
    //Check action on surrogate pairs.
    EXPORT Test34 := ASSERT(Uni.EndsWith(U'x𐐀',U'x𐐀','') = TRUE);
    EXPORT Test35 := ASSERT(Uni.EndsWith(U'𐐀',U'𐐀x','') = FALSE);
    EXPORT Test37 := ASSERT(Uni.EndsWith(U'x𐐀',U'𐐀','') = TRUE);
    //Check action with normalization forms
    EXPORT Test39 := ASSERT(Uni.EndsWith(U'Ç̌',U'Ç̌','') = TRUE);
    EXPORT Test40 := ASSERT(Uni.EndsWith(U'Ç̌',U'Ç̌','NFC') = TRUE);
    DATA r1 := x'43002703';
    SHARED UNICODE t1a := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    SHARED UNICODE t2a := TRANSFER(r2, UNICODE);
    EXPORT Test41 := ASSERT(Uni.EndsWith(t1a,t2a,'NFC') = TRUE);
    EXPORT Test42 := ASSERT(Uni.EndsWith(t1a,t2a,'NFD') = TRUE);
    EXPORT Test43 := ASSERT(Uni.EndsWith(t1a,t2a,'NFKC') = TRUE);
    EXPORT Test44 := ASSERT(Uni.EndsWith(t1a,t2a,'NFKD') = TRUE);
    DATA r1 := x'43000C032703';
    SHARED UNICODE t1b := TRANSFER(r1, UNICODE);
    DATA r2 := x'0C032703';
    SHARED UNICODE t2b := TRANSFER(r2, UNICODE);
    EXPORT Test45 := ASSERT(Uni.EndsWith(t1b,t2b,'NFC') = FALSE);
    EXPORT Test46 := ASSERT(Uni.EndsWith(t1b,t2b,'NFD') = TRUE);
    //Test various combinations of space padding.
    DATA r1 := x'4300270320002000';
    SHARED UNICODE t1c := TRANSFER(r1, UNICODE);
    DATA r2 := x'c70020002000';
    SHARED UNICODE t2c := TRANSFER(r2, UNICODE);
    EXPORT Test47 := ASSERT(Uni.EndsWith(t1c,t2c,'NFC') = TRUE);
    EXPORT Test48 := ASSERT(Uni.EndsWith(t1a,t2c,'NFC') = TRUE);
    EXPORT Test49 := ASSERT(Uni.EndsWith(t1c,t2a,'NFC') = TRUE);
  END;
END;
