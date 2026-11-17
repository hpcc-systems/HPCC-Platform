rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

resultRec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED8 filepos;
END;

sourceData := DATASET('~regress::multi::dg_flat', rec, FLAT);
keyData := INDEX({STRING10 dg_firstname, STRING10 dg_lastname},
                 {UNSIGNED1 dg_prange, UNSIGNED8 filepos},
                 '~regress::multi::dg_index_evens');
matches := JOIN(sourceData, keyData,
                LEFT.dg_firstname = RIGHT.dg_firstname AND LEFT.dg_lastname = RIGHT.dg_lastname,
                TRANSFORM(resultRec,
                          SELF.dg_parentid := LEFT.dg_parentid;
                          SELF.dg_firstname := LEFT.dg_firstname;
                          SELF.dg_lastname := LEFT.dg_lastname;
                          SELF.filepos := RIGHT.filepos));
OUTPUT(SORT(matches, dg_parentid, dg_firstname, dg_lastname, filepos), NAMED('Result'));
