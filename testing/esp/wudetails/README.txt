usage: wutest.py [-h] [-o dir] [-d] [--logresp] [--logreq] [-n days]
                 [-t number] [--nosslverify] [-u url] [--user username]
                 [--pw password] [--httpdigestauth]

optional arguments:
  -h, --help            show this help message and exit
  -o dir, --outdir dir  Results directory
  -d, --debug           Enable debug
  --logresp             Log wudetails responses (in results directory)
  --logreq              Log wudetails requests (in results directory)
  -n days, --lastndays days
                        Use workunits from last 'n' days
  -t number, --testcase number
                        Execute give testcase
  --nosslverify         Disable SSL certificate verification
  -u url, --baseurl url
                        Base url for both WUQuery and WUDetails
  --user username       Username for authentication
  --pw password         Password for authentication
  --httpdigestauth      User HTTP digest authentication(Basic auth default)
