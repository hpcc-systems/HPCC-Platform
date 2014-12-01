
export MemCached := SERVICE
  DATASET MGetDataset(CONST VARSTRING servers, CONST VARSTRING key, CONST VARSTRING partitionKey = 'root') : cpp,action,context,entrypoint='MGetData';
END;

STRING servers := '--SERVER=myIP:11211';

myRec := RECORD
      integer i1;
      integer i2;
      real r1;
      string name;
END;

ds1 := DATASET([
		{2, 3, 3.14, 'pi'},
		{7, 11, 1.412, 'root2'}
], myRec);

output(ds1);

MemCached.MGetDataset(servers, 'ds1');
