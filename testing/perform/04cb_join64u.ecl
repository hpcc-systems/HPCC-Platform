import perform.system, perform.format, perform.files;

dsLeft := files.generateSimpleScaled(1, 64);
dsRight := files.generateSimpleScaled(2, 64);

j := JOIN(dsLeft, dsRight, LEFT.id1 DIV 64 = RIGHT.id1 DIV 64, UNORDERED); // most should match 64 items

output(COUNT(NOFOLD(j)));
