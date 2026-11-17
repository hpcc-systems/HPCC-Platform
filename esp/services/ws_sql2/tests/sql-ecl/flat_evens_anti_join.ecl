rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

sourceData := DATASET('~regress::multi::dg_flat', rec, FLAT);
evens := DATASET('~regress::multi::dg_flat_evens', rec, FLAT);
unmatched := JOIN(sourceData, evens, LEFT.dg_parentid = RIGHT.dg_parentid,
                  TRANSFORM(rec, SELF := LEFT), LEFT ONLY);
OUTPUT(SORT(TABLE(unmatched, {dg_parentid, dg_firstname}),
            dg_parentid, dg_firstname), NAMED('Result'));
