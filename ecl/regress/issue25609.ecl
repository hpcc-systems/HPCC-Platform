colorVal := DATASET([{'Black' ,110 },
                     {'Brown' ,100 },
                     {'Red'   ,200 },
                     {'White' ,300 }],
                     {STRING color, INTEGER ID} //Inline record layout
                     );

ds := NOFOLD(colorVal);

OUTPUT(ds, NAMED('ds'), ALL);
OUTPUT(COUNT(ds), NAMED('ds_cnt'), ALL);

// OUTPUT(ds.ID, NAMED('ds_id'), ALL); // Error: "ds.id - no specified row for Table ds"
OUTPUT(COUNT(ds.ID), NAMED('ds_id_cnt'), ALL);
OUTPUT(SUM(ds.ID), NAMED('ds_id_sum'), ALL);
