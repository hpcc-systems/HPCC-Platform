ds := dataset('ds', {String10 first_name; string20 last_name; }, FLAT);

total := count(ds(first_name='fred'));

total;
