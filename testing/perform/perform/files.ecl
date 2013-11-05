import perform.system, perform.format;

export files := MODULE
    export prefix := '~perform::' + __PLATFORM__ + '::';
    export simpleName := prefix + 'simple';
    export paddedName := prefix + 'padded';

    export generateSimple(unsigned delta = 0) := DATASET(system.simpleRecordCount, format.createSimple(COUNTER + delta), DISTRIBUTED);

    export generatePadded() := DATASET(system.simpleRecordCount, format.createPadded(COUNTER), DISTRIBUTED);

    EXPORT suffix(boolean compressed) := IF(compressed, '_compressed', '_uncompressed');

    export diskSimple(boolean compressed) := DATASET(simpleName+suffix(compressed), format.simpleRec, FLAT);

    export diskPadded(boolean compressed) := DATASET(paddedName+suffix(compressed), format.paddedRec, FLAT);

    export diskSplit(unsigned part) := DATASET(paddedName+suffix(false)+'_' + (string)part, format.simpleRec, FLAT);

END;
