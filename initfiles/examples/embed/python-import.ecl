IMPORT python;

/*
 This example illustrates a call to a Python functions defined in the Python module python_cat.py

 The python file must be reachable at runtime on the Roxie/Thor clusters via the standard Python
 search path. If a path is specified as in this example, then that path will be added to the start
 of the Python search path before the module load is attempted. You should ensure that the .py
 file exists at the specified location on every machine in the cluster.
 */


STRING pcat(STRING a, STRING b) := IMPORT(Python, '/opt/HPCCSystems/examples/embed/python_cat.cat');
pcat('Hello ', 'world!');
