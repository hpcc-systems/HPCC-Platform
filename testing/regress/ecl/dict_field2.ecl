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


color2code[c1].value;
color2code[c2].value;
color2code[c3].value;
color2code[c4].value;

colorn2code[c1].value;
colorn2code[c2].value;
colorn2code[c3].value;
colorn2code[c4].value;

colorv2code[c1].value;
colorv2code[c2].value;
colorv2code[c3].value;
colorv2code[c4].value;

code1ToColor[1].value;
code1ToColor[257].value;

'---';

ucolor2code[u1].value;
ucolor2code[u2].value;
ucolor2code[u3].value;
ucolor2code[u4].value;

ucolorn2code[u1].value;
ucolorn2code[u2].value;
ucolorn2code[u3].value;
ucolorn2code[u4].value;

ucolorv2code[u1].value;
ucolorv2code[u2].value;
ucolorv2code[u3].value;
ucolorv2code[u4].value;

ucoloru2code[u1].value;
ucoloru2code[u2].value;
ucoloru2code[u3].value;
ucoloru2code[u4].value;
