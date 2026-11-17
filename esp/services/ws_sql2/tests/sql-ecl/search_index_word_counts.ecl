keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
counts := TABLE(keyData, {kind, word, UNSIGNED8 n := COUNT(GROUP)}, kind, word);
OUTPUT(CHOOSEN(SORT(counts, -n, kind, word), 100), NAMED('Result'));
