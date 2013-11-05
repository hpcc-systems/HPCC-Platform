import perform.system, perform.format, perform.files;
#option ('unlimitedResources', true); // generate all the sorts into a single graph

s(unsigned delta) := FUNCTION
    ds := files.generateSimple(delta);

    RETURN NOFOLD(sort(ds, id3));
END;

ds(unsigned i) := s(i+0x00000000) + s(i+0x10000000) + s(i+0x20000000) + s(i+0x30000000);

dsAll := ds(0);

output(COUNT(NOFOLD(dsAll)));
