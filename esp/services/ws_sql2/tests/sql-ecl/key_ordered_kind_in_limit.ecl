keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
matches := keyData(KEYED(kind IN [1, 2]));
sortedData := SORT(TABLE(matches, {kind, doc, segment, wpos, wip, original, word, dpos}),
                   -kind, dpos, doc, segment, wpos, wip, original, word);
OUTPUT(CHOOSEN(sortedData, 5), NAMED('Result'));
