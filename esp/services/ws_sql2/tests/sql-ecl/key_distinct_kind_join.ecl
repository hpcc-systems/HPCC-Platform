keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
kinds := CHOOSEN(DEDUP(TABLE(keyData(KEYED(kind = 2)), {kind}), kind), 1);
matches := keyData(kind IN SET(kinds, kind), kind >= 1);
OUTPUT(TABLE(matches, {UNSIGNED8 row_count := COUNT(GROUP)}), NAMED('Result'));
