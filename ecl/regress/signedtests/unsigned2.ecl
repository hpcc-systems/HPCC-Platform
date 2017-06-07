Rawfile := DATASET('TST::postcodes', { string8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);
INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, 'TST::postcode.key');

match1 := INDX_Postcode( postcode = 'KT19 1AA' );

OUTPUT(match1);
