R := RECORD
  STRING Txt;
  END;

d :=  DATASET('~n:\\temp\\weblogs',R,CSV(SEPARATOR('')));

PATTERN Num := PATTERN('[0-9]')+;
PATTERN Tok_IP := Num '.' Num '.' Num '.' Num;
PATTERN Stuff := ANY+;
PATTERN QuoteString := '"' ANY* '"';
PATTERN ws := ' '+;
PATTERN DateForm := '[' ANY* ']';

PATTERN WebLine := FIRST Tok_IP ws Stuff ws Stuff ws DateForm ws QuoteString ws Num ws Num ws QuoteString ws QuoteString LAST;

ResForm := RECORD
  STRING15 Ip := MATCHTEXT(Tok_Ip);
 STRING1 S1 := MATCHTEXT(Stuff[1]);
 STRING19 S2 := MATCHTEXT(Stuff[2]);
 STRING28 Dte := MATCHTEXT(DateForm);
 UNSIGNED2 N1 := (UNSIGNED)MATCHTEXT(Num[1]);
 UNSIGNED2 N2 := (UNSIGNED)MATCHTEXT(Num[2]);
 STRING S3 := MATCHTEXT(QuoteString[1]);
 STRING S4 := MATCHTEXT(QuoteString[2]);
 STRING S5 := MATCHTEXT(QuoteString[3]);
  END;

p := PARSE(d,Txt,WebLine,ResForm);

Errs := RECORD
  STRING T := d.Txt;
  END;
e := PARSE(d,Txt,WebLine,Errs,NOT MATCHED ONLY);
//EXPORT File_Weblogs := p : PERSIST('TMP::ParsedWeblogs');
output(choosen(p,20000),,'out.tmp',overwrite);
import std.system.debug;
startTime := debug.msTick() : independent;
output('Time taken = ' + (debug.msTick() - startTime));
