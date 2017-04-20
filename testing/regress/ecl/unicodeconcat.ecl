// output is u'', expected u'startend'

unicode s := u'start' : stored('s');
unicode m := u'' : stored('m');
unicode e := u'end' : stored('e');

output(s + m + e, named('unicode_empty_string'));
//e := u'end' : stored('e');
//output(u'start' + u'' + u'end', named('unicode_empty_string'));



utf8 s8 := nofold((utf8)s);
utf8 m8 := nofold(trim(nofold(u8'   ')));
utf8 e8 := nofold((utf8)e);

output(s8 + m8 + e8 = u8'startend', named('unicode_empty_string8'));

