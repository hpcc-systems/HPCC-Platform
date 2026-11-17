keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
docs := CHOOSEN(DEDUP(SORT(TABLE(keyData(KEYED(kind = 1), KEYED(word = 'p')), {doc}), doc), doc), 20);
matches := keyData(KEYED(kind = 1), doc IN SET(docs, doc));
OUTPUT(SORT(TABLE(matches, {doc, UNSIGNED8 row_count := COUNT(GROUP)}, doc), doc), NAMED('Result'));
