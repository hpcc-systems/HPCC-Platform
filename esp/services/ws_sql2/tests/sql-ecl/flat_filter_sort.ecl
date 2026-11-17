rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

sourceData := DATASET('~regress::multi::dg_flat', rec, FLAT);
OUTPUT(SORT(sourceData(dg_parentid < 3), dg_parentid, dg_firstname, dg_lastname, dg_prange), NAMED('Result'));
