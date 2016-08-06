import perform.system, perform.format, perform.files;

dsLeft := files.generateSimpleScaled(1, 4);
dsRight := files.generateSimpleScaled(2, 4);

j := JOIN(dsLeft, dsRight, LEFT.id1 DIV 4 = RIGHT.id1 DIV 4); // most should match 4 items

output(COUNT(NOFOLD(j)));
