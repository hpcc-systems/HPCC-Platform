keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
matches := keyData(KEYED(kind = 1), KEYED(word = 'p'),
                   KEYED(doc IN [281474976739771, 281474976746516, 281474976740000]),
                   doc > 281474976739771);
OUTPUT(TABLE(matches, {UNSIGNED8 row_count := COUNT(GROUP), UNSIGNED8 dpos_total := SUM(GROUP, dpos)}),
       NAMED('Result'));
