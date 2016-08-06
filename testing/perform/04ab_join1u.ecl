import perform.system, perform.format, perform.files;

dsLeft := files.generateSimple(1);
dsRight := files.generateSimple(2);

j := JOIN(dsLeft, dsRight, LEFT.id1 = RIGHT.id1, UNORDERED); // all but one should match

output(COUNT(NOFOLD(j)));
