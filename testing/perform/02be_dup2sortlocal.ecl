import perform.system, perform.format, perform.files;

ds := files.generateSimple();

s := sort(ds, id3 % 0x1000, local);  // limit to 4K unique keys

output(COUNT(NOFOLD(s)));
