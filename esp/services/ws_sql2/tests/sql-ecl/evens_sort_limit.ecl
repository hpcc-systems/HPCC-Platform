rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

sourceData := DATASET('~regress::multi::dg_flat_evens', rec, FLAT);
sortedData := SORT(TABLE(sourceData, {dg_parentid, dg_firstname, dg_lastname}),
                   -dg_parentid, dg_firstname, dg_lastname);
OUTPUT(CHOOSEN(sortedData, 24), NAMED('Result'));
