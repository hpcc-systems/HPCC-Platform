rec := RECORD
    UNSIGNED1 kind;
    STRING20 word;
    UNSIGNED8 doc;
    UNSIGNED1 segment;
    UNSIGNED8 wpos;
END;

sourceData := DATASET('~regress::multi::searchsource', rec, THOR);
counts := TABLE(sourceData(kind = 1),
                {word, UNSIGNED8 n := COUNT(GROUP)}, word);
OUTPUT(CHOOSEN(SORT(counts, -n, word), 100), NAMED('Result'));
