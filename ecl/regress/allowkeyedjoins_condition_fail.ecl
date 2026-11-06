//fail
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

inputRows := DATASET([{1, 10}, {2, 20}], rec);
sourceFile := DATASET('~regress::allowkeyedjoins::file', fileRec, FLAT);
sourceKey := INDEX(sourceFile, {id, grp, filepos}, '~regress::allowkeyedjoins::key');

OUTPUT(JOIN(inputRows, sourceKey, KEYED(LEFT.id = RIGHT.id)));