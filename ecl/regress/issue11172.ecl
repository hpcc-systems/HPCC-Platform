doWait := sequential(
   output('about to wait' ,named('waitBegin'),overwrite)
  ,wait('myTestEvent11172')
  ,output('done waiting' ,named('waitEnd'),overwrite)
);

if(workunit[1] = 'W' ,doWait);

notify(EVENT('myTestEvent11172','')) : when(cron('* * * * *'),count(2));
