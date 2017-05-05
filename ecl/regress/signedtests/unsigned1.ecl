Rawfile := DATASET('TST::postcodes', { string8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);
INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, 'TST::postcode.key');

BUILD(INDX_Postcode);
