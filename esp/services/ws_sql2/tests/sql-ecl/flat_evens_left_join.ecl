rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

resultRec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    UNSIGNED4 even_parentid;
END;

sourceData := DATASET('~regress::multi::dg_flat', rec, FLAT);
evens := DATASET('~regress::multi::dg_flat_evens', rec, FLAT);
matches := JOIN(sourceData, evens, LEFT.dg_parentid = RIGHT.dg_parentid,
                TRANSFORM(resultRec,
                          SELF.dg_parentid := LEFT.dg_parentid;
                          SELF.dg_firstname := LEFT.dg_firstname;
                          SELF.even_parentid := RIGHT.dg_parentid),
                LEFT OUTER);
OUTPUT(SORT(matches, dg_parentid, dg_firstname, even_parentid), NAMED('Result'));
