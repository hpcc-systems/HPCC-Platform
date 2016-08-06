import perform.system, perform.format, perform.files;

ds := files.diskSimple(false);

s := sort(ds, id3, local);

output(s,,files.simpleName+'_uncompressed_02aa',OVERWRITE);
