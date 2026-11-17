keyData := INDEX({STRING10 dg_firstname, STRING10 dg_lastname, UNSIGNED1 dg_prange},
                 {UNSIGNED8 filepos}, '~regress::multi::dg_keyed_index');
matches := keyData(KEYED(dg_firstname >= 'CLAIRE' AND dg_firstname <= 'KELLY'),
                   dg_prange >= 2);
sortedData := SORT(TABLE(matches, {dg_firstname, dg_lastname, dg_prange}),
                   dg_firstname, dg_lastname, dg_prange);
OUTPUT(CHOOSEN(sortedData, 32), NAMED('Result'));
