rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

sourceData := DATASET('~regress::multi::dg_flat', rec, FLAT);
counts := TABLE(sourceData, {dg_firstname, n := COUNT(GROUP)}, dg_firstname);
OUTPUT(SORT(counts, dg_firstname), NAMED('Result'));
