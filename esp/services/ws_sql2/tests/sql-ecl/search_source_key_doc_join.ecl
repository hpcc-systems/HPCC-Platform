rec := RECORD
    UNSIGNED1 kind;
    STRING20 word;
    UNSIGNED8 doc;
    UNSIGNED1 segment;
    UNSIGNED8 wpos;
END;

resultRec := RECORD
    UNSIGNED8 doc;
    UNSIGNED8 source_count;
    UNSIGNED8 key_count;
END;

sourceData := DATASET('~regress::multi::searchsource', rec, THOR);
keyData := INDEX({UNSIGNED1 kind, STRING20 word, UNSIGNED8 doc,
                  UNSIGNED1 segment, UNSIGNED8 wpos, UNSIGNED1 wip},
                 {UNSIGNED1 flags, STRING20 original, UNSIGNED8 dpos},
                 '~regress::multi::searchindex');
sourceDocs := TABLE(sourceData, {doc, UNSIGNED8 source_count := COUNT(GROUP)}, doc);
keyDocs := TABLE(keyData(KEYED(kind = 1), KEYED(word = 'a')),
                 {doc, UNSIGNED8 key_count := COUNT(GROUP)}, doc);
matches := JOIN(sourceDocs, keyDocs, LEFT.doc = RIGHT.doc,
                TRANSFORM(resultRec,
                          SELF.doc := LEFT.doc;
                          SELF.source_count := LEFT.source_count;
                          SELF.key_count := RIGHT.key_count));
OUTPUT(CHOOSEN(SORT(matches, doc), 512), NAMED('Result'));
