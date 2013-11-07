import perform.system, perform.format, perform.files;

ds := files.diskSimple(false);

OUTPUT(SUM(ds,id1)); // Allow input dataset to be projected and optimized
