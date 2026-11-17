rec := RECORD
    UNSIGNED4 dg_parentid;
    STRING10 dg_firstname;
    STRING10 dg_lastname;
    UNSIGNED1 dg_prange;
END;

sourceData := DATASET('~regress::multi::dg_flat', rec, FLAT);
filtered := TABLE(sourceData(dg_firstname = 'CLAIRE'),
                  {dg_parentid, dg_firstname, dg_lastname});
OUTPUT(SORT(filtered, dg_parentid, dg_lastname), NAMED('Result'));
