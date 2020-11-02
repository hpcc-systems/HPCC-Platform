esdlcmd Regression Script
=========================

Runs esdl tool commands with known inputs to compare to known baseline results.

Currently supports the commands:

    - xsd
    - wsdl
    - java
    - cpp

The esdl file inputs are in the testing/esp/esdlcmd/inputs directory, and the known response
baselines are kept in the testing/esp/esdlcmd/baselines directory. Each entry in the baselines
directory must match the name of Test Case it is for.

For example, the Test Case named 'wsdl1' to generate the wsdl for WsDfu, has a file named

    'wsdl1.wsdl'

in the baselines directory. Likewise, the Test Case named

    'cpp-dfu1'

that generates c++ files for the WsDfu service has a folder named 'cpp-dfu1' in the baselines
directory.
