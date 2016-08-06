import perform.system, perform.format;

export files := MODULE
    export prefix := '~perform::' + __PLATFORM__ + '::';
    export simpleName := prefix + 'simple';
    export paddedName := prefix + 'padded';

    export generateSimple(unsigned delta = 0) := DATASET(system.simpleRecordCount, format.createSimple(COUNTER + delta), DISTRIBUTED);

    export generateSimpleScaled(unsigned delta = 0, unsigned scale) := DATASET(system.simpleRecordCount DIV scale, format.createSimple(COUNTER + delta), DISTRIBUTED);

    export generatePadded() := DATASET(system.simpleRecordCount, format.createPadded(COUNTER), DISTRIBUTED);

    EXPORT suffix(boolean compressed) := IF(compressed, '_compressed', '_uncompressed');

    export diskSimple(boolean compressed) := DATASET(simpleName+suffix(compressed), format.simpleRec, FLAT);

    export csvSimple(boolean compressed) := DATASET(simpleName+suffix(compressed)+'_csv', format.simpleRec, CSV);

    export xmlSimple(boolean compressed) := DATASET(simpleName+suffix(compressed)+'_xml', format.simpleRec, XML('', NOROOT));

    export diskPadded(boolean compressed) := DATASET(paddedName+suffix(compressed), format.paddedRec, FLAT);

    export diskSplit(unsigned part) := DATASET(paddedName+suffix(false)+'_' + (string)part, format.simpleRec, FLAT);

END;
