keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
matches := keyData((INTEGER2)kind < 256);
OUTPUT(TABLE(matches, {UNSIGNED8 row_count := COUNT(GROUP), UNSIGNED8 dpos_total := SUM(GROUP, dpos)}),
       NAMED('Result'));
