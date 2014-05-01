

dsx := dataset('x', { unsigned id, string x }, flat);
dsy := dataset('y', { unsigned id, string y }, flat);

j1 := JOIN(dsx, dsy, LEFT.id = RIGHT.id) : persist('p1');

dsz := dataset('z', { unsigned id, string z }, flat);

j2 := JOIN(j1, dsz, LEFT.id = RIGHT.id) : persist('p2');

dsi := dataset('i', { unsigned id, string i }, flat) : independent;

j3 := JOIN(j2, dsi, LEFT.id = RIGHT.id) : persist('p3');
output(j3);
