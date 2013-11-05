import perform.system, perform.format, perform.files;

ds := files.diskSimple(false);

s := sort(ds, id3);

output(s,,files.simpleName+'_uncompressed_02ab',OVERWRITE);
