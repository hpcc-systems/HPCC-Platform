import perform.system, perform.format, perform.files;

ds := files.generateSimple();

s := sort(ds, id3, local);

output(COUNT(NOFOLD(s)));
