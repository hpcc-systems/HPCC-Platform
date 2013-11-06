import perform.system, perform.format, perform.files;

ds := files.generateSimple();

s := sort(ds, id3 % 0x100000, local);  // limit to 1M unique keys

output(COUNT(NOFOLD(s)));
