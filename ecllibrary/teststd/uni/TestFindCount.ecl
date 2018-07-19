/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std.Uni;

EXPORT TestFindCount := MODULE
  EXPORT TestConst := MODULE
    //Check action on strings with no entries: empty source string, search string, or return string.
    EXPORT Test01 := ASSERT(Uni.FindCount(U'', U'', '') = 0);
    EXPORT Test02 := ASSERT(Uni.FindCount(U' ', U'x', '') = 0);
    EXPORT Test03 := ASSERT(Uni.FindCount(U'x', U' ', '') = 0);
    EXPORT Test04 := ASSERT(Uni.FindCount(U'x', U'x', '') = 1);
    EXPORT Test05 := ASSERT(Uni.FindCount(U'  ', U' ', '') = 2);
    EXPORT Test06 := ASSERT(Uni.FindCount(U'x ', U' ', '') = 1);
    EXPORT Test07 := ASSERT(Uni.FindCount(U' x', U' ', '') = 1);
    EXPORT Test08 := ASSERT(Uni.FindCount(U' x ', U' ', '') = 2);
    EXPORT Test09 := ASSERT(Uni.FindCount(U' abc def ', U' ', '') = 3);
    EXPORT Test10 := ASSERT(Uni.FindCount(U' abc   def ', U'b', '') = 1);
    EXPORT Test11 := ASSERT(Uni.FindCount(U' a b c   def ', U' ', '') = 7);
    EXPORT Test12 := ASSERT(Uni.FindCount(U' abc   def', U'abc ', '') = 1);
    EXPORT Test13 := ASSERT(Uni.FindCount(U'$', U'$$', '') = 0);
    EXPORT Test14 := ASSERT(Uni.FindCount(U'$x', U'$$', '') = 0);
    EXPORT Test15 := ASSERT(Uni.FindCount(U'$$', U'$$', '') = 1);
    EXPORT Test16 := ASSERT(Uni.FindCount(U'$$$', U'$$', '') = 1);
    EXPORT Test17 := ASSERT(Uni.FindCount(U'$$$$', U'$$', '') = 2);
    EXPORT Test18 := ASSERT(Uni.FindCount(U'$$x$$', U'$$', '') = 2);
    EXPORT Test19 := ASSERT(Uni.FindCount(U'$$x$$y', U'$$', '') = 2);
    EXPORT Test20 := ASSERT(Uni.FindCount(U'$$x$$xy', U'$$x', '') = 2);
    EXPORT Test21 := ASSERT(Uni.FindCount(U'a,c,d', U',', '') = 2);
    EXPORT Test22 := ASSERT(Uni.FindCount(U'a,,d', U',', '') = 2);
    EXPORT Test23 := ASSERT(Uni.FindCount(U',,,', U',', '') = 3);
    EXPORT Test24 := ASSERT(Uni.FindCount(U' \377ABCDEF FEDCBA ', U'ABCD', '') = 1);
    //Check action on a string containing punctuation characters.
    EXPORT Test25 := ASSERT(Uni.FindCount(U' ,&%$@ ',U'%$', '') = 1);
    //Check action on a string containing an apostrophe.
    EXPORT Test26 := ASSERT(Uni.FindCount(U'I couldn\'t hear you!',U'\'', '') = 1);
    //Check action on a string containing different variations/combinations of numbers and other characters.
    EXPORT Test27 := ASSERT(Uni.FindCount(U'1 234 123abc 23.6 abc123',U'2', '') = 4);
    //Test other space characters (< 0x20).
    EXPORT Test28 := ASSERT(Uni.FindCount(U'an\nt\tdef',U' ', '') = 0);
    EXPORT Test29 := ASSERT(Uni.FindCount(U'  a n\nt \t  def    ',U't', '') = 1);
    //Check action on a string containing latin diacritical marks.
    EXPORT Test30 := ASSERT(Uni.FindCount(U'À à',U'À', '') = 1);
    EXPORT Test31 := ASSERT(Uni.FindCount(U'ȭ š',U'ȭ', '') = 1);
    //Check action on a string containing Spanish words with latin accents.
    //Translation: "The deceased changed the girls"
    EXPORT Test32 := ASSERT(Uni.FindCount(U'El difunto cambió las niñas',U'cambió', '') = 1);
    //Check action on a string containing Chinese characters.
    //Translation: "I am a computer"
    EXPORT Test33 := ASSERT(Uni.FindCount(U'我是電腦',U'是', '') = 1);
    //Check action on a string containing Modern Greek characters.
    //Translation: "Do you come here often?"
    EXPORT Test34 := ASSERT(Uni.FindCount(U' Έρχεσαι συχνά εδώ; ',U'χ', '') = 2);
    //Testcases 35 and 36 test for bidirectional capabilities with scripts in arabic and hebrew.
    //Check action on arabic lettering with accent marks. Bidirectional.
    //Translation: "Good morning"
    EXPORT Test35 := ASSERT(Uni.FindCount(U'صباح الخير',U'ا', '') = 2);
    //Check action on hebrew lettering with accent marks (called pointing). Bidirectional.
    //Translation: (not a phrase, 2 different words separated by a space)
    EXPORT Test36 := ASSERT(Uni.FindCount(U'קָמָץ שִׁי״ן',U'קָ', '') = 1);
    //Check action on surrogate pairs.
    EXPORT Test37 := ASSERT(Uni.FindCount(U'x𐐀x𐐀',U'𐐀', '') = 2);
    EXPORT Test38 := ASSERT(Uni.FindCount(U'𐐀',U'𐐀', '') = 1);
    EXPORT Test39 := ASSERT(Uni.FindCount(U'x',U'𐐀', '') = 0);
    EXPORT Test40 := ASSERT(Uni.FindCount(U'𐐀xx𐐀𐐀',U'x', '') = 2);
    //Don't stop for 0 bytes
    EXPORT Test41 := ASSERT(Uni.FindCount('xx' + x'00' + 'xx', 'xx', '') = 2, CONST);
    //Check action with normalization forms
    EXPORT Test42 := ASSERT(Uni.FindCount(U'Ç̌',U'Ç̌','') = 1);
    EXPORT Test43 := ASSERT(Uni.FindCount(U'Ç̌',U'Ç̌','NFC') = 1);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test44 := ASSERT(Uni.FindCount(t1,t2,'NFC') = 1);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test45 := ASSERT(Uni.FindCount(t1,t2,'NFD') = 1);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test46 := ASSERT(Uni.FindCount(t1,t2,'NFKC') = 1);
    DATA r1 := x'43002703';
    UNICODE t1 := TRANSFER(r1, UNICODE);
    DATA r2 := x'c700';
    UNICODE t2 := TRANSFER(r2, UNICODE);
    EXPORT Test47 := ASSERT(Uni.FindCount(t1,t2,'NFKD') = 1);
    EXPORT Test48 := ASSERT(Uni.FindCount(U'AABC',U'ABC','') = 1);
  END;
END;
