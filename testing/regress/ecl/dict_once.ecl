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

color2code := DICTIONARY(resistorCodes, { color => value}) : ONCE;
code2color := DICTIONARY(resistorCodes, { value => color}) : ONCE;

integer pow10(integer val) := CASE(val, 0=>1, 1=>10, 2=>100, ERROR('Out of range'));

bands := DATASET([{'Red'},{'Red'},{'Red'}], {string band}) : STORED('bands');

getBandValue(string band) := IF(band IN color2code, color2Code[band].value, ERROR('Unrecognised band ' + band));

value := (getBandValue(bands[1].band)*10 + getBandValue(bands[2].band)) * pow10(getBandValue(bands[3].band));

output(if(count(bands)=3, value, ERROR('3 bands expected')));
