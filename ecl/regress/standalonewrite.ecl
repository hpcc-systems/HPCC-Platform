ds := DATASET(10, transform({ unsigned c }, SELF.c := COUNTER));

output(ds,,'localout1',xml,overwrite);

output(ds,,'subdir::localout1',xml,overwrite);
