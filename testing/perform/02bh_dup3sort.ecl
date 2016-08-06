import perform.system, perform.format, perform.files;

ds := files.generateSimple();

s := sort(ds, (unsigned)(log(id3+1) / log(1.0001))); // generate skewed groups.

output(COUNT(NOFOLD(s)));
