#option ('targetClusterType', 'roxie');

ds := dataset(['This','is','the', 'result'], { string line });

output(ds,,named('x'),thor);
