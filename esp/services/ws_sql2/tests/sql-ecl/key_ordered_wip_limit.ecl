keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
matches := keyData(KEYED(kind = 1), KEYED(word = 'p'));
sortedData := SORT(TABLE(matches, {wip, dpos}), -wip, dpos);
OUTPUT(CHOOSEN(sortedData, 5), NAMED('Result'));
