-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA1

Rawfile := DATASET('TST::postcodes', { string8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);
INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, 'TST::postcode.key');

match1 := INDX_Postcode( postcode = 'KT19 1AA' );

OUTPUT(match1);
-----BEGIN PGP SIGNATURE-----
Version: GnuPG v1

iQEcBAEBAgAGBQJZJupdAAoJEG37B3CNqkF9V4AH/RLzf+a57pIbYOFnJzFMm9Qi
I4MW83RFbjnwIy9+QXyCb5+43VtpbZY1/xSjU4ln9H2hrnTMbDVLfGsutiE4rIr/
ahmLAaLQ2wt64EiF3Bnd6iZUJSzTUGU+BcoEv/jnryryfh/KpCXQHQwcuUGTby4e
Lc+U675/+kUyDSGkUrK6NDir5bnpZOeWiCaV/lSzWcO9yhbdbE2XWQ5PpGzCvHNg
VLY4shgMUQAFosrjluMKXU024EdqZVeJRDjyObQYXxGwZ927STcDjwOIpiQtuCad
QrIskkxZrikOG/yz+/4NOUArhYaQvBtJHuk6GbOhV9JLER6eNOE+PQq+NOLaWlA=
=imCB
-----END PGP SIGNATURE-----
