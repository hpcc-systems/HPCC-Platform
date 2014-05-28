IMPORT STD;

numNodes := IF(__PLATFORM__ = 'roxie', 1, CLUSTERSIZE);

ds1 := DATASET(2, TRANSFORM({UNSIGNED line}, SELF.line := COUNTER) , LOCAL);

summary := TABLE(ds1, { COUNT(GROUP) }, LOCAL);
COUNT(NOFOLD(summary)) = numNodes;
COUNT(ds1) = 2 * numNodes;
SUM(ds1, line) = 3 * numNodes;
