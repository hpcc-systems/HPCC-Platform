resistorCodesEN := dataset([{0, 'Black'},
                            {1, 'Brown'},
                            {2, 'Red'},
                            {3, 'Orange'},
                            {4, 'Yellow'},
                            {5, 'Green'},
                            {6, 'Blue'},
                            {7, 'Violet'},
                            {8, 'Grey'},
                            {9, 'White'}], {unsigned1 value, string color});

resistorCodesFR := dataset([{0, 'Noir'},
                            {1, 'Marron'},
                            {2, 'Rouge'},
                            {3, 'Orange'},
                            {4, 'Jaune'},
                            {5, 'Vert'},
                            {6, 'Bleu'},
                            {7, 'Violet'},
                            {8, 'Gris'},
                            {9, 'Blanc'}], {unsigned1 value, string color});

string2 lang := 'EN' : STORED('lang');

color2code := MAP(lang='EN'=>resistorCodesEN, lang='FR'=>resistorCodesFR, ERROR(resistorCodesEN, 'Unknown language ' + lang));

integer pow10(integer val) := CASE(val, 0=>1, 1=>10, 2=>100, ERROR('Out of range'));

bands := DATASET([{'Red'},{'Red'},{'Red'}], {string band}) : STORED('bands');

getBandValue(string band) := IF(exists(color2code(color=band)), color2code(color=band)[1].value, ERROR('Unrecognised band ' + band));

value := (getBandValue(bands[1].band)*10 + getBandValue(bands[2].band)) * pow10(getBandValue(bands[3].band));

output(if(count(bands)=3, value, ERROR('3 bands expected')));
