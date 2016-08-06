import perform.system;
import perform.format;
import perform.files;

ds := DATASET(system.simpleRecordCount, format.createPadded(COUNTER), DISTRIBUTED);

OUTPUT(ds,,files.paddedName+'_uncompressed',overwrite);
