-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA1

Rawfile := DATASET('TST::postcodes', { string8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);

INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, 'TST::postcode.key');
BuildIndexOp := BUILDINDEX(INDX_Postcode, OVERWRITE);

BuildIndexOp;
-----BEGIN PGP SIGNATURE-----
Version: GnuPG v1

iQEcBAEBAgAGBQJXu0KvAAoJEG37B3CNqkF9J9oH/1tJR4tHzaxI4B+nvL28rQ+v
l/MjWhNPlgyPBtovCb6nP71LNWeNgCGSMLEIGTozV4lFsaJUjsn/qmsdMd9slFnS
CrlykPzMrDszWz1b3cBtqQUTqpv0pLoaIPVqxH6hzN0O6j/pWKcRSKRWySHuhM1k
d/t9+wPsQuKeD1W/Efcpackj2IBwqHSQVNf5BC9JxilrdSUEhM0723XYUIRtMgNs
zH0/DV2UDrpZl4ugdhXXs/6uTtiwvZjRVGR7iGbtjvF//REZPd3Lce8OzqObnKmn
2fIqE2lTPCBbh8WykX2jMcy8C/CtQq3ehrwMKCD2ynZbIkDuG4X+GCzsZXApo3Q=
=ZkLN
-----END PGP SIGNATURE-----
