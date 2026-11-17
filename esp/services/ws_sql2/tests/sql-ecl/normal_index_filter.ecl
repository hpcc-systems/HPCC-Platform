keyData := INDEX({STRING10 dg_firstname, STRING10 dg_lastname},
                 {UNSIGNED1 dg_prange, UNSIGNED8 filepos},
                 '~regress::multi::dg_index');
matches := keyData(KEYED(dg_firstname >= 'CLAIRE' AND dg_firstname <= 'KELLY'));
sortedData := SORT(TABLE(matches, {dg_firstname, dg_lastname, filepos}),
                   dg_firstname, dg_lastname, filepos);
OUTPUT(CHOOSEN(sortedData, 32), NAMED('Result'));
