resistorCodes := dataset([{0, 'Black'},
                          {1, 'Brown'},
                          {2, 'Red'},
                          {3, 'Orange'},
                          {4, 'Yellow'},
                          {5, 'Green'},
                          {6, 'Blue'},
                          {7, 'Violet'},
                          {7, 'Purple'},
                          {8, 'Grey'},
                          {9, 'White'}], {unsigned1 value, string color});

resistorCodes_nofold := dataset([{0, 'Black'},
                          {1, 'Brown'},
                          {2, 'Red'},
                          {3, 'Orange'},
                          {4, 'Yellow'},
                          {5, 'Green'},
                          {6, 'Blue'},
                          {7, 'Violet'},
                          {7, 'Green'},
                          {8, 'Grey'},
                          {9, 'White'}], {unsigned1 value, string color}) : stored('nofold');

code2color := DICTIONARY(resistorCodes, { value => color});
code2color_nofold := DICTIONARY(resistorCodes_nofold, { value => color});

code2color[7].color;
code2color_nofold[7].color;
