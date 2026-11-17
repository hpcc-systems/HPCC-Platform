keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
matches := keyData(KEYED(kind = 1), KEYED(word = 'p'),
                   KEYED(doc BETWEEN 281474976739771 AND 281474976746516));
sortedData := SORT(TABLE(matches, {kind, doc, dpos}), -doc, dpos);
OUTPUT(CHOOSEN(sortedData, 5), NAMED('Result'));
