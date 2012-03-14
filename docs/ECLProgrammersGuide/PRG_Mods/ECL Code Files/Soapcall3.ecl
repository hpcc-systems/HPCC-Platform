//
//  Example code - use without restriction.  
//
ParseWords(STRING LineIn) := FUNCTION
	PATTERN Ltrs := PATTERN('[A-Za-z]');
	PATTERN Char := Ltrs | '-' | '\'';
	TOKEN   Word := Char+;
					ds 	 := DATASET([{LineIn}],{STRING line});
	RETURN PARSE(ds,line,Word,{STRING Pword := MATCHTEXT(Word)});
END;	

EXPORT Soapcall3() := FUNCTION
  STRING UID     := '' : STORED('UIDstr');
  STRING LeftIn  := '' : STORED('LeftInStr');
  STRING RightIn := '' : STORED('RightInStr');
  BOOLEAN TokenMatch := FUNCTION
    P1 := ParseWords(LeftIn);
    P2 := ParseWords(RightIn);
    SetSrch := SET(P1,Pword);
    ProjRes := PROJECT(P2,
		                   TRANSFORM({BOOLEAN Fnd},
					                       SELF.Fnd := LEFT.Pword IN SetSrch));
    AllRes  := DEDUP(SORT(ProjRes,Fnd));
    RETURN COUNT(AllRes) = 1 AND AllRes[1].Fnd = TRUE;
  END;
  RETURN OUTPUT(DATASET([{UID,TokenMatch}],{STRING UID,BOOLEAN res}));
END;

