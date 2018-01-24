WUDetails Regression Script
===========================

wutest.py regression script executes WUDetails test cases against workunits produced
by the regression script.

PREREQUISITES
=============
1. The performance suite must be executed before the wutest.py script is executed and the resultant workunits must be available in Dali.  (The workunits produced by the regression suite are required by wutest.py.)
2. The zeep package is required: pip3 install zeep
3. WsWorkunits/WUQuery and WsWorkunits/WUDetails ESP services must be running
4. The script must be executed with python3: python3 wutest.py


Notes
=====

1. By default, no credentials are used in accessing the soap services.

- Authentication may be enabled by providing credentials with --pw and --user.
- By default, if credentials are provided http basic authentication is used.  Use --httpdigestauth to specify http digest authenticaation.

2. The default base URL is http://localhost.

- The base URL may be specified by --baseurl option.
- SSL encryption is supported - simply use 'https' prefix
- Where SSL has been enabled, the server certificates will be automatically verified.  To disable certificate  verification, use the --nosslverify option 

3. The script uses the most recent set of workunits in the past day

- To search for workunits beyond the past day the --lastndays should be used


USAGE
=====

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
                        Use workunits from last 'n' days.  By default, workunits from the past day is used.
  -t number, --testcase number
                        Execute give testcase
  --nosslverify         Disable SSL certificate verification
  -u url, --baseurl url
                        Base url for both WUQuery and WUDetails
  --user username       Username for authentication
  --pw password         Password for authentication
  --httpdigestauth      User HTTP digest authentication(Basic auth default)

