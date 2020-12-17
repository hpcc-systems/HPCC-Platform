ds := DATASET('localout1', { unsigned c }, XML('Dataset/Row') );
ds2 := DATASET('subdir::localout1', { unsigned c }, XML('Dataset/Row') );

output(ds, { c+1 },'localout2',csv,overwrite);
output(ds, { c*2 },'subdir::localout2',csv,overwrite);
