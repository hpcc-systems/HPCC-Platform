#option ('allowKeyedJoins', false);

rec := RECORD
    UNSIGNED4 id;
    UNSIGNED4 grp;
END;

fileRec := RECORD
    UNSIGNED4 id;
    UNSIGNED4 grp;
    UNSIGNED8 filepos {VIRTUAL(FILEPOSITION)};
END;

inputRows := DATASET([{1, 10}, {1, 20}, {2, 30}], rec);
sourceFile := DATASET('~regress::allowkeyedjoins::file', fileRec, FLAT);
sourceKey := INDEX(sourceFile, {id, grp, filepos}, '~regress::allowkeyedjoins::key');

SEQUENTIAL(
    OUTPUT(inputRows, , '~regress::allowkeyedjoins::file', OVERWRITE),
    BUILDINDEX(sourceKey, OVERWRITE),
    OUTPUT(JOIN(inputRows, sourceKey, LEFT.id = RIGHT.id, KEYED)),
    OUTPUT(JOIN(inputRows, sourceKey, KEYED(LEFT.id = RIGHT.id) AND WILD(RIGHT.grp), KEYED)),
    OUTPUT(JOIN(inputRows, sourceFile, LEFT.id = RIGHT.id, KEYED(sourceKey), KEYED)),
    OUTPUT(DENORMALIZE(inputRows, sourceKey, LEFT.id = RIGHT.id, TRANSFORM(rec, SELF := LEFT), KEYED)),
    OUTPUT(DENORMALIZE(inputRows, sourceKey, LEFT.id = RIGHT.id, GROUP, TRANSFORM(rec, SELF := LEFT), KEYED))
);