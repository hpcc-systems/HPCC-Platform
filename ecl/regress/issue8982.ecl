pv_record := RECORD
UNSIGNED id1;
UNSIGNED id2;
UNSIGNED id3;
UNSIGNED pv;
END;

alldata := DATASET('~test::pv_fact', pv_record, CSV(SEPARATOR('\t')));

output(SUM(alldata, alldata.pv));
output(count(alldata));
