/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestStartsWith := MODULE

  EXPORT TestConst := MODULE

    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.StartsWith('','','') = FALSE, CONST);
    EXPORT Test03 := ASSERT(Uni.StartsWith('x','','') = FALSE, CONST);
    EXPORT Test05 := ASSERT(Uni.StartsWith('','x','') = FALSE, CONST);
    EXPORT Test07 := ASSERT(Uni.StartsWith('','','x') = FALSE, CONST);
    EXPORT Test02 := ASSERT(Uni.StartsWith('x','x','') = TRUE, CONST);
    //Check action on a string containing a single word - with various whitespace.
    EXPORT Test08 := ASSERT(Uni.StartsWith(' x ','  x ','') = FALSE);
    EXPORT Test09 := ASSERT(Uni.StartsWith(' x','x  ','') = FALSE);
    EXPORT Test10 := ASSERT(Uni.StartsWith('x ','x ','') = TRUE);
    EXPORT Test11 := ASSERT(Uni.StartsWith('  x','x','') = FALSE);
    //Functionality Test.
    EXPORT Test12 := ASSERT(Uni.StartsWith('x','xx','') = FALSE);
    EXPORT Test14 := ASSERT(Uni.StartsWith('','','yw') = FALSE);
    //Check action on a string containg multiple words - with various whitespace combinations.
    EXPORT Test15 := ASSERT(Uni.StartsWith('xyz abc ','xyz abc','') = TRUE);
    EXPORT Test16 := ASSERT(Uni.StartsWith('xyz abc ','abc','') = FALSE);
    EXPORT Test17 := ASSERT(Uni.StartsWith('xyz abc ','x','') = TRUE);
    EXPORT Test18 := ASSERT(Uni.StartsWith('xyz abc ','xy','') = TRUE);
    EXPORT Test19 := ASSERT(Uni.StartsWith('xyz abc ','yz','') = FALSE);
    EXPORT Test20 := ASSERT(Uni.StartsWith('xyz abc ','x y','') = FALSE);
    //Check action on a string containing punctuation characters.
    EXPORT Test21 := ASSERT(Uni.StartsWith(',&%$@ ',',&','') = TRUE);
    //Check action on a string containing an apostrophe.
    EXPORT Test23 := ASSERT(Uni.StartsWith('I couldn\'t hear you!','I couldn\'t','') = TRUE);
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test24 := ASSERT(Uni.StartsWith('123abc 23.6 abc123','123abc 23.6','') = TRUE);
    //Test other space characters (< 0x20).
    EXPORT Test25 := ASSERT(Uni.StartsWith('a n\nt \t  def    ','a n\nt \t','') = TRUE);
    EXPORT Test26 := ASSERT(Uni.StartsWith('\nt \t  def    ','t','') = FALSE);
    //Check action on a string containing latin diacritical marks.
    EXPORT Test27 := ASSERT(Uni.StartsWith(U'À à',U'À','') = TRUE);
    EXPORT Test28 := ASSERT(Uni.StartsWith(U'ȭ š',U'ȭ','') = TRUE);
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test29 := ASSERT(Uni.StartsWith(U'El difunto cambió las niñas',U'El difunto cambió','') = TRUE);
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test30 := ASSERT(Uni.StartsWith(U'我是電腦',U'我是','') = TRUE);
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test31a := ASSERT(Uni.StartsWith(U'Έρχεσαι συχνά εδώ; ',U' Έρχεσαι συ','') = FALSE);
    EXPORT Test31b := ASSERT(Uni.StartsWith(U'Έρχεσαι συχνά εδώ; ',U'Έρχεσαι συ','') = TRUE);
    //Testcases 32 and 33 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test32 := ASSERT(Uni.StartsWith(U'صباح الخير',U'صباح','') = TRUE);
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test33 := ASSERT(Uni.StartsWith(U'קָמָץ שִׁי״ן',U'קָמָץ','') = TRUE);
    //Check action on surrogate pairs.
    EXPORT Test34 := ASSERT(Uni.StartsWith(U'x𐐀',U'x𐐀','') = TRUE);
    EXPORT Test35 := ASSERT(Uni.StartsWith(U'𐐀',U'𐐀x','') = FALSE);
    EXPORT Test37 := ASSERT(Uni.StartsWith(U'𐐀',U'𐐀','') = TRUE);
    //Check action with normalization forms
    EXPORT Test39 := ASSERT(Uni.StartsWith(U'Ç̌',U'Ç̌','') = TRUE);
    EXPORT Test40 := ASSERT(Uni.StartsWith(U'Ç̌',U'Ç̌','NFC') = TRUE);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test41 := ASSERT(Uni.StartsWith(t1,t2,'NFC') = TRUE);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test42 := ASSERT(Uni.StartsWith(t1,t2,'NFD') = TRUE);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test43 := ASSERT(Uni.StartsWith(t1,t2,'NFKC') = TRUE);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test44 := ASSERT(Uni.StartsWith(t1,t2,'NFKD') = TRUE);
    DATA r1 := x'43000C032703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'4300';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test45 := ASSERT(Uni.StartsWith(t1,t2,'NFC') = FALSE);
    DATA r1 := x'43000C032703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'4300';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test46 := ASSERT(Uni.StartsWith(t1,t2,'NFD') = TRUE);
  END;

END;
