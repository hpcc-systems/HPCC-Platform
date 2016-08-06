import perform.system;
import perform.format;
import perform.files;

ds := DATASET(system.simpleRecordCount, format.createSimple(COUNTER), DISTRIBUTED);

//OUTPUT(ds,,files.simpleName+'_uncompressed_xml',overwrite,xml(NOROOT));
OUTPUT(ds,,files.simpleName+'_uncompressed_xml',overwrite,xml(HEADING('','')));
