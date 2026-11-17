rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

sourceData := UNGROUP(DATASET('~regress::multi::dg_grouped', rec, FLAT, __GROUPED__));
sortedData := SORT(TABLE(sourceData(dg_prange >= 2), {dg_parentid, dg_prange}),
                   dg_parentid, dg_prange);
OUTPUT(CHOOSEN(sortedData, 36), NAMED('Result'));
