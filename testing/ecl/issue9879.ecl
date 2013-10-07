IMPORT Std;

STD.Uni.LocaleFind(u'',u'abc',1,'');
STD.Uni.LocaleFind(u'abc',u'',1,'');
STD.Uni.LocaleFind(u'abc',u'b',1,'');

STD.Uni.LocaleFindAtStrength(u'',u'abc',1,'',1);
STD.Uni.LocaleFindAtStrength(u'abc',u'',1,'',1);
STD.Uni.LocaleFindAtStrength(u'abc',u'b',1,'',1);

STD.Uni.LocaleFindReplace(u'',u'abc',u'x','');
STD.Uni.LocaleFindReplace(u'abc',u'',u'x','');
STD.Uni.LocaleFindReplace(u'abc',u'B',u'x','');
STD.Uni.LocaleFindReplace(u'abc',u'b',u'x','');

STD.Uni.LocaleFindAtStrengthReplace(u'',u'abc',u'x','',1);
STD.Uni.LocaleFindAtStrengthReplace(u'abc',u'',u'x','',1);
STD.Uni.LocaleFindAtStrengthReplace(u'abc',u'B',u'x','',0);
STD.Uni.LocaleFindAtStrengthReplace(u'abc',u'B',u'x','',1);
STD.Uni.LocaleFindAtStrengthReplace(u'abc',u'B',u'x','',2);
