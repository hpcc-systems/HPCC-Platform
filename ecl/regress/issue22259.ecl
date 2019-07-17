import Std.Str;

rec := RECORD
 string f1;
END;
ds := DATASET([{'a'},{'b'}], rec);

dofunc(string l) := FUNCTION
 o1 := OUTPUT(ds, , PIPE('/home/jsmith/tmp/pipecmd'));
 RETURN WHEN(true,o1,BEFORE);
END;
rec mytrans(rec l) := TRANSFORM
 SELF.f1 := IF(dofunc(l.f1), 'a', 'b');
END;
rec myfailtrans(STRING x) := TRANSFORM
 SELF.f1 := x;
END;

p2 := PROJECT(ds, mytrans(LEFT));
c2 := CATCH(NOFOLD(p2), ONFAIL(myFailTrans(FAILMESSAGE)));

SEQUENTIAL(
 OUTPUT(c2);
);
