import Std;
import lib_workunitservices;

dWUIds := sort(STD.System.Workunit.workunitlist(lowwuid := 'W20190901-000000',
                                                highwuid := 'W20190912-000000'), wuid);

rTest := record
    dataset(lib_workunitservices.WsStatistic) dStats;
end;

rTest xTest(dWUIds l) := transform
    self.dStats := STD.System.Workunit.WorkunitStatistics(l.wuid,false,'nested[0],stype[graph],stat[TimeElapsed]');
end;

output(project(dWUIds,xTest(left)));
