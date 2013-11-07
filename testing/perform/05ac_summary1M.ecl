import perform.system, perform.format, perform.files;

ds := files.generateSimple();
unsigned8 numBins := 1000000;

t := TABLE(ds, { id3 % numBins, cnt := COUNT(group) }, id3 % numBins);

output(COUNT(NOFOLD(t)));
