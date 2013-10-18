// output is u'', expected u'startend'

unicode s := u'start' : stored('s');
unicode m := u'' : stored('m');
unicode e := u'end' : stored('e');

output(s + m + e, named('unicode_empty_string'));
//e := u'end' : stored('e');
//output(u'start' + u'' + u'end', named('unicode_empty_string'));



