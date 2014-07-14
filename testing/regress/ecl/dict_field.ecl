resistorCodes := dataset([{0, 'Black'},
                            {1, 'Brown'},
                            {2, 'Red'},
                            {3, 'Orange'},
                            {4, 'Yellow'},
                            {5, 'Green'},
                            {6, 'Blue'},
                            {7, 'Violet'},
                            {8, 'Grey'},
                            {9, 'White'}], {unsigned1 value, string color});

color2code := DICTIONARY(resistorCodes, { STRING fcolor := color => value});
colorn2code := DICTIONARY(resistorCodes, { STRING6 ncolor := color => value});
colorv2code := DICTIONARY(resistorCodes, { VARSTRING vcolor := color => value});

ucolor2code := DICTIONARY(resistorCodes, { UNICODE fcolor := color => value});
ucolorn2code := DICTIONARY(resistorCodes, { UNICODE6 ncolor := color => value});
ucolorv2code := DICTIONARY(resistorCodes, { VARUNICODE vcolor := color => value});
ucoloru2code := DICTIONARY(resistorCodes, { UTF8 ucolor := color => value});

code1ToColor := DICTIONARY(resistorCodes, { value => color});

string c1 := 'White' : STORED('color1');
string c2 := 'White ' : STORED('color2');
string c3 := 'Whit' : STORED('color3');
string c4 := 'White or maybe not' : STORED('color4');

unicode u1 := u'White' : STORED('ucolor1');
unicode u2 := u'White ' : STORED('ucolor2');
unicode u3 := u'Whit' : STORED('ucolor3');
unicode u4 := u'White with a hint of yellow' : STORED('ucolor4');

c1 in color2code;
c2 in color2code;
c3 not in color2code;
c4 not in color2code;

c1 in colorn2code;
c2 in colorn2code;
c3 not in colorn2code;
c4 not in colorn2code;

c1 in colorv2code;
c2 in colorv2code;
c3 not in colorv2code;
c4 not in colorv2code;

1 in code1ToColor;
257 not in code1ToColor;

'---';

ROW({u1}, { unicode fcolor} ) IN ucolor2code;
ROW({u1}, { unicode6 ncolor} ) IN ucolorn2code;
ROW({u1}, { varunicode vcolor} ) IN ucolorv2code;
ROW({u1}, { utf8 ucolor} ) IN ucoloru2code;

u1 in ucolor2code;
u2 in ucolor2code;
u3 not in ucolor2code;
u4 not in ucolor2code;

u1 in ucolorn2code;
u2 in ucolorn2code;
u3 not in ucolorn2code;
u4 not in ucolorn2code;

u1 in ucolorv2code;
u2 in ucolorv2code;
u3 not in ucolorv2code;
u4 not in ucolorv2code;

u1 in ucoloru2code;
u2 in ucoloru2code;
u3 not in ucoloru2code;
u4 not in ucoloru2code;

'---';

stringrecord := { string fcolor; };
colorx2code := DICTIONARY(resistorCodes, { stringrecord s := row(transform(stringrecord, SELF.fcolor := color))  => value});
ROW({ {c1} }, { stringrecord s } ) IN colorx2code;
