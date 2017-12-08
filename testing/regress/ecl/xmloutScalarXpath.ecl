import $.setup;
prefix := setup.Files(false, false).FilePrefix;

origReadRec :=       RECORD
   string fn {xpath('fname')};
   string ln {xpath('lname')};
   string pr {xpath('Rec/prange')};
   string st {xpath('Rec/street')};
   string zi {xpath('Rec/zips')};
   string ag {xpath('Rec/age')};
   integer8 ix {xpath('Rec/id')};
END;

L1rec := RECORD
   DATASET(origReadRec) deep{xpath('L1/L2')};
END;


x := u8'<root><L1>' +
       u8'<L2><fname>MOE</fname><lname>DOE</lname>' +
         u8'<Rec><prange>1</prange><street>11TH</street><zips>11</zips><age>31</age><id>315367</id></Rec>' +
       u8'</L2>' +
       u8'<L2><fname>JOE</fname><lname>POE</lname>' +
         u8'<Rec><prange>2</prange><street>22ND</street><zips>11</zips><age>32</age><id>315370</id></Rec>' +
       u8'</L2>' +
       u8'<L2><fname>XOE</fname><lname>ROE</lname>' +
         u8'<Rec><prange>3</prange><street>33RD</street><zips>11</zips><age>33</age><id>315204</id></Rec>' +
       u8'</L2>' +
     u8'</L1></root>';


xrow := FROMXML(L1Rec, x);
output(xrow, named('OriginalReadRow'));



scalarReadRec :=       RECORD
   string fn {xpath('fname')};
   string ln {xpath('lname')};
   string pr;
   string st;
   string zi;
   string ag;
   integer8 ix;
END;

readL1rec := RECORD
   DATASET(scalarReadRec) deep{xpath('L1/L2')};
END;

output(Dataset(xrow),,prefix+'TEMP_output_scalar_xpath.xml',overwrite, xml);
readWrittenXml := dataset(DYNAMIC(prefix+'TEMP_output_scalar_xpath.xml'), readL1Rec, xml('Dataset/Row'));
output(readWrittenXml, named('readWrittenDataset'));
