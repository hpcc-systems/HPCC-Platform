import perform.system, perform.format, perform.files;

dsLeft := files.generateSimpleScaled(1, 4096);
dsRight := files.generateSimpleScaled(2, 4096);

j := JOIN(dsLeft, dsRight, LEFT.id1 DIV 4096 = RIGHT.id1 DIV 4096, MANY LOOKUP); // most should match 4096 items

output(COUNT(NOFOLD(j)));
