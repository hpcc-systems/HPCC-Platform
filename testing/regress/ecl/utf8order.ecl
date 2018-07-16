u8null := u8'' : stored('u8null');
unull := u'' : stored('unull');

output('Cycles - which should never happen!');
output(u8'BC' > u8'BB\u20AC');
output(u8'BB\u20AC' > u8'Ba');
output(u8'Ba' > u8'BC');

output(u'BC' > u'BB\u20AC');
output(u'BB\u20AC' > u'Ba');
output(u'Ba' > u'BC');

output('Unicode:');
output(u'abcÈ' > u'abcE');
output(u'abcÈ'+unull > u'abcE');
output(u'abcÈ'+unull != u'abcE');

//Check correct length is used rather than size.
output(u'AB\u20ACX'+unull < u'AB\u20ACY');
output(u'AB\u20ACX'+unull != u'AB\u20ACY');

Output('Utf8:');
output(u8'abcÈ' > u8'abcE');
output(u8'abcÈ'+u8null > u8'abcE');
output(u8'abcÈ'+u8null != u8'abcE');

output(u8'AB\u20ACX'+u8null < u8'AB\u20ACY');
output(u8'AB\u20ACX'+u8null != u8'AB\u20ACY');

output(U'AB ' = U'AB');
output(U'AB ' != U'AB\t');
output(U8'AB ' = U8'AB');
output(U8'AB ' != U8'AB\t');
output('AB ' = 'AB');
output('AB ' != 'AB\t');

//Illustrate the different ordering or string v unicode v utf8
d1 := dataset([U'ABC',U'ABc',U'abc',U'abC',U'abcÈ',U'abcE'], { unicode4 text });
output(sort(d1, text));

d2 := dataset(['ABC','ABc','abc','abC','abcÈ','abcE'], { string4 text });
output(sort(d2, text));

d3 := dataset([u8'ABC',u8'ABc',u8'abc',u8'abC',u8'abcÈ',u8'abcE'], { utf8 text });
output(sort(d3, text));
