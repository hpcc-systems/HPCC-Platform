//
//  Example code - use without restriction.  
//
IMPORT std;

SetBadWords := ['JUNK', 'GARBAGE', 'CRUD'];
BadWordDS := DATASET(SetBadWords,{STRING10 word});

SearchDS := DATASET([{1,'FRED','FLINTSTONE'},
                     {2,'GEORGE','KRUEGER'},
                     {3,'CRUDOLA','BAR'},
                     {4,'JUNKER','KNIGHT'},
                     {5,'GARBAGEGUY','MANGIA'},
                     {6,'FREDDY','KRUEGER'},
                     {7,'TIM','TINY'},
                     {8,'JOHN','JONES'},
                     {9,'MIKE','JETSON'}],
										{UNSIGNED6 ID,STRING10 firstname,STRING10 lastname});

outrec := RECORD
  SearchDS.ID;
	SearchDS.firstname;
	BOOLEAN FoundWord;
END;

{BOOLEAN Found} FindWord(BadWordDS L, STRING10 inword)	:= TRANSFORM
  SELF.Found := Std.Str.Find(inword,TRIM(L.word),1)>0;
END;

outrec CheckWords(SearchDS L)	:= TRANSFORM
  SELF.FoundWord := EXISTS(PROJECT(BadWordDS,FindWord(LEFT,L.firstname))(Found=TRUE));
	SELF := L;
END;

result := PROJECT(SearchDS,CheckWords(LEFT));

OUTPUT(result(FoundWord=TRUE));
OUTPUT(result(FoundWord=FALSE));

MAC_FindBadWords(BadWordSet,InFile,IDfld,SeekFld,ResAttr,MatchType=1) := MACRO
  #UNIQUENAME(BadWordDS)
  %BadWordDS% := DATASET(BadWordSet,{STRING word{MAXLENGTH(50)}});

  #UNIQUENAME(outrec)
  %outrec% := RECORD
    InFile.IDfld;
    InFile.SeekFld;
    BOOLEAN FoundWord := FALSE;
    UNSIGNED2 FoundPos := 0;
  END;

  #UNIQUENAME(ChkTbl)
  %ChkTbl% := TABLE(InFile,%outrec%);

  #UNIQUENAME(FindWord)
  {BOOLEAN Found,UNSIGNED2 FoundPos} %FindWord%(%BadWordDS% L,INTEGER C,STRING inword) := TRANSFORM
  #IF(MatchType=1) //"contains" search
    SELF.Found := Std.Str.Find(inword,TRIM(L.word),1) > 0;
  #END
  #IF(MatchType=2) //"exact match" search
    SELF.Found := inword = L.word;
  #END
  #IF(MatchType=3) //"starts with" search
    SELF.Found := Std.Str.Find(inword,TRIM(L.word),1) = 1;
  #END
    SELF.FoundPos := IF(SELF.FOUND=TRUE,C,0);
  END;
  #UNIQUENAME(CheckWords)
  %outrec% %CheckWords%(%ChkTbl% L) := TRANSFORM
    WordDS := PROJECT(%BadWordDS%,%FindWord%(LEFT,COUNTER,L.SeekFld));
    SELF.FoundWord := EXISTS(WordDS(Found=TRUE));
    SELF.FoundPos := WordDS(Found=TRUE)[1].FoundPos;
    SELF := L;
  END;
  ResAttr := PROJECT(%ChkTbl%,%CheckWords%(LEFT));
ENDMACRO;

MAC_FindBadWords(SetBadWords,SearchDS,ID,firstname,Results)
OUTPUT(Results(FoundWord=TRUE));
OUTPUT(Results(FoundWord=FALSE));

SetCartoonFirstNames := ['GEORGE','FRED', 'FREDDY'];
SetCartoonLastNames := ['JETSON','FLINTSTONE','KRUEGER'];

MAC_FindBadWords(SetCartoonFirstNames,SearchDS,ID,firstname,Res1,2)
MAC_FindBadWords(SetCartoonLastNames,SearchDS,ID,lastname,Res2,2)

Cartoons := JOIN(Res1(FoundWord=TRUE),
                 Res2(FoundWord=TRUE),
                 LEFT.ID=RIGHT.ID AND LEFT.FoundPos=RIGHT.FoundPos);

MAC_FindBadWords(SetBadWords,SearchDS,ID,firstname,Res3,3)
MAC_FindBadWords(SetBadWords,SearchDS,ID,lastname,Res4)
SetBadGuys := SET(Cartoons,ID) +
              SET(Res3(FoundWord=TRUE),ID) +
              SET(Res4(FoundWord=TRUE),ID);

GoodGuys := SearchDS(ID NOT IN SetBadGuys);
BadGuys  := SearchDS(ID IN SetBadGuys);
OUTPUT(BadGuys,NAMED('BadGuys'));
OUTPUT(GoodGuys,NAMED('GoodGuys'));