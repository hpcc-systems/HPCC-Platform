
myIndex1 := INDEX({ unsigned8 id, string10 name }, { unsigned4 cnt }, 'i1', FILEPOSITION(FALSE));
myIndex2 := INDEX({ unsigned8 id, string10 name }, { unsigned4 cnt }, 'i2', FILEPOSITION(TRUE));

ds1 := DATASET([
        { 1, 'Gavin', 20 },
        { 2, 'James', 15 },
        { 3, 'Kelly', 27 }
        ], { unsigned id, string name, unsigned8 cnt });
        
BUILD(ds1, myIndex1);
BUILD(myIndex2, ds1, ds1);
BUILD(myIndex2, ds1, 'i2copy', 'i3copy');
