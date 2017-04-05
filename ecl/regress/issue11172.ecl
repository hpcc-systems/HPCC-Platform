doWait := sequential(
   output('about to wait' ,named('waitBegin'),overwrite)
  ,wait('myEvent')
  ,output('done waiting' ,named('waitEnd'),overwrite)
);

if(workunit[1] = 'W' ,doWait);
