
IndexFilename := '~dg::Taxi::IDX';

  // Final Product Payload Index
Layout := RECORD
    UNSIGNED2 pulocationid;
    UNSIGNED2 dolocationid;
    UNSIGNED1 pudow;
    UNSIGNED1 puhour
    =>
    INTEGER8 grpcnt;
    DECIMAL5_2 avgdistance;
    DECIMAL7_2 avgfare;
    STRING10 avgduration;
    UNSIGNED8 __internal_fpos__;
END;

ProdData := DATASET('ds', Layout, THOR);

IDX := INDEX(Layout, IndexFilename);

BuildIDX1 := BUILD(ProdData,
                           {puLocationID,doLocationID,puDOW,puHour},
                           {ProdData},
                           IndexFilename,OVERWRITE);
BuildIDX2 := BUILD(ProdData, , IndexFilename,OVERWRITE);

BuildIDX2;
