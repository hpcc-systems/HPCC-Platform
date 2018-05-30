
outRecord := RECORD
    STRING10 name;
    unsigned1  id;
END;

dsx := DATASET([{'One', 10},{'Two', 2},{'Three',0}], outRecord);
output(dsx,,'myfile',overwrite);

ds := DATASET('myfile', outRecord, THOR, OPT);
ds2 := DATASET([{'Fred', 10},{'George', 2},{'Harry',0}], outRecord) : independent(few);

streamed dataset(outRecord) doRead(streamed dataset(outRecord) inds) := EMBED(C++ : distributed,time)
    return LINK(inds);
ENDEMBED;

output(doread(ds));
output(doread(ds2));
