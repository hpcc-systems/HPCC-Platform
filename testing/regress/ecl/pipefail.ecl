import Std.Str;

fakecmd := '/tmp/fakepipecmd';
rec := RECORD
 string f1;
END;
ds := DATASET([{'a'},{'b'}], rec);

rec myFailTrans(string failmsg) := TRANSFORM
 string tmp := REGEXREPLACE('^.*ERROR:', failmsg, 'ERROR:');
 SELF.f1 := REGEXREPLACE(' - PIPE.*$', tmp, '');
END;

pr := CATCH(PIPE(fakecmd, rec), ONFAIL(myFailTrans(FAILMESSAGE)));
pt := CATCH(PIPE(ds, fakecmd), ONFAIL(myFailTrans(FAILMESSAGE)));
//Ideally would have test that had CATCH around OUTPUT, but couldn't get to work
//pw := OUTPUT(ads, , PIPE(fakecmd));

SEQUENTIAL(
 OUTPUT(pr);
 OUTPUT(pt);
);
