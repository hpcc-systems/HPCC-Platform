#! /usr/bin/python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

__version__ = "0.1"

import argparse
import filecmp
import inspect
import logging
import os
import subprocess
import sys
import traceback
from pathlib import Path

DESC = "Test the functioning of the esdl command. Version " + __version__

class DirectoryCompare(filecmp.dircmp):
    """
    Compare the content of dir1 and dir2. In contrast with filecmp.dircmp, this
    subclass compares the content of files with the same path.
    """
    def phase3(self):
        """
        Find out differences between common files.
        Ensure we are using content comparison with shallow=False.
        """
        fcomp = filecmp.cmpfiles(self.left, self.right, self.common_files,
                                 shallow=False)
        self.same_files, self.diff_files, self.funny_files = fcomp

class TestRun:
    """Common settings for all TestCases in a run."""

    def __init__(self, stats, exe_path, output_base, test_path):
        self.exe_path = exe_path
        self.output_base = output_base
        self.test_path = test_path
        self.stats = stats

class TestCaseBase:
    """Settings for a specific test case."""

    def __init__(self, run_settings, name, command, esdl_file, service, xsl_path, options=None):
        self.run_settings = run_settings
        self.name = name
        self.command = command
        self.esdl_path = (self.run_settings.test_path / 'inputs' / esdl_file)
        self.service = service
        self.xsl_path = xsl_path
        self.options = options
        self.output_path = Path(self.run_settings.output_base) / name
        self.args = [
            str(run_settings.exe_path),
            self.command,
            self.esdl_path,
            self.service,
            '--xslt',
            self.xsl_path,
            '--outdir',
            # must contain a trailing slash
            str(self.output_path) + '/'
        ]

        if options:
            self.args.extend(options)

        self.result = None


    def run_test(self):
        safe_mkdir(self.output_path)
        logging.debug("Test %s args: %s", self.name, str(self.args))
        self.result = subprocess.run(self.args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if self.result.returncode != 0:
            logging.error('Error running "esdl %s" for test "%s": %s', self.command, self.name, self.result.stderr)
            success = False
        else:
            success = self.validate_results()

        self.run_settings.stats.add_count(success)

    def validate_results(self):
        """Compare test case results to the known key.

        Return True if the two are identical or False otherwise.
        """
        logging.debug('TestCaseBase implementation called, no comparison run')
        return False


class TestCaseXSD(TestCaseBase):
    """Test case for the wsdl or xsd commands.

    Both generate a single file output, so test validation compares
    the contents of the output file with the key file.

    The path the xsl files not include the 'xslt' directory. The command
    assumes it needs to apped that directory itself.
    """

    def __init__(self, run_settings, name, command, esdl_file, service, xsl_path, options=None):
        super().__init__(run_settings, name, command, esdl_file, service, xsl_path, options)

    def validate_results(self):
        """Compare test case results to the known key.

        Return True if the two are identical or False otherwise.
        """
        suffix = '.' + self.command
        outName = (self.output_path / self.service.lower()).with_suffix(suffix)
        key = (self.run_settings.test_path / 'key' / self.name).with_suffix(suffix)

        if (not key.exists()):
            logging.error('Missing key file %s', str(key))
            return False

        if (not outName.exists()):
            logging.error('Missing output for test %s', self.name)
            return False

        if (not filecmp.cmp(str(key), str(outName))):
            logging.debug('Comparing key %s to output %s', str(key), str(outName))
            logging.error('Test failed: %s', self.name)
            return False
        else:
            logging.debug('Passed: %s', self.name)
            return True

class TestCaseCode(TestCaseBase):
    """Test case for the cpp or java commands.

    Both generate a directory full of output, so test validation compares
    the contents of the output directory with the key directory.

    The path the xsl files must be appended with 'xslt/' for the command
    to find the xslt files.
    """

    def __init__(self, run_settings, name, command, esdl_file, service, xsl_path, options=None):
        # must end in a slash esdl command doesn't
        # add a slash before appending the file name
        xsl_cpp_path = str((xsl_path / 'xslt'))
        xsl_cpp_path += '/'
        super().__init__(run_settings, name, command, esdl_file, service, xsl_cpp_path, options)

    def is_same(self, dir1, dir2):
        """
        Compare two directory trees content.
        Return False if they differ, True is they are the same.
        """
        compared = DirectoryCompare(dir1, dir2)
        if (compared.left_only or compared.right_only or compared.diff_files
            or compared.funny_files):
            return False
        for subdir in compared.common_dirs:
            if not self.is_same(os.path.join(dir1, subdir), os.path.join(dir2, subdir)):
                return False
        return True

    def validate_results(self):
        # output of cpp or java is a directory named 'source'
        outName =  (self.output_path / 'source')
        key = (self.run_settings.test_path / 'key' / self.name)

        if (not key.exists()):
            logging.error('Missing key file %s', str(key))
            return False

        if (not outName.exists()):
            logging.error('Missing output for test %s', self.name)
            return False

        if (not self.is_same(str(key), str(outName))):
            logging.debug('Comparing key %s to output %s', str(key), str(outName))
            logging.error('Test failed: %s', self.name)
            return False
        else:
            logging.debug('Passed: %s', self.name)
            return True


class Statistics:
    def __init__(self):
        self.successCount = 0
        self.failureCount = 0

    def add_count(self, success):
        if (success):
            self.successCount += 1
        else:
            self.failureCount += 1


def parse_options():
    """Parse any command-line options given returning both
    the parsed options and arguments.
    """

    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument('testroot',
                        help='Path of the root folder of the esdlcmd testing project')

    parser.add_argument('-o', '--outdir',
                        help='Directory name of output for tests',
                        default='esdlcmd-test-output')

    parser.add_argument('-e', '--esdlpath',
                        help='Path to the esdl executable to test')

    parser.add_argument('-x', '--xslpath',
                        help='Path to the folder containing xslt/*.xslt transforms',
                        default='/opt/HPCCSystems/componentfiles/')

    parser.add_argument('-d', '--debug',
                        help='Enable debug logging of test cases',
                        action='store_true', default=False)

    args = parser.parse_args()
    return args


def safe_mkdir(path):
    """Create a new directory, catching all exceptions.

    The directory may already exist, and any missing intermediate
    directories are created. The script is exited if unrecoverable
    exceptions are caught.
    """

    try:
        path.mkdir(parents=True, exist_ok=True)
    except FileExistsError as e:
        pass
    except (FileNotFoundError, PermissionError) as e:
        logging.error("'%s' \nExit." % (str(e)))
        exit(-1)
    except:
        print("Unexpected error:"
              + str(sys.exc_info()[0])
              + " (line: "
              + str(inspect.stack()[0][2])
              + ")" )
        traceback.print_stack()
        exit(-1)


def main():

    args = parse_options()
    stats = Statistics()

    test_path = Path(args.testroot)
    exe_path = Path(args.esdlpath) / 'esdl'
    xsl_base_path = Path(args.xslpath)

    if (args.debug):
        loglevel = logging.DEBUG
    else:
        loglevel=logging.INFO
    logging.basicConfig(level=loglevel, format='[%(levelname)s] %(message)s')

    stats = Statistics()
    run_settings = TestRun(stats, exe_path, args.outdir, test_path)

    test_cases = [
        # wsdl
        TestCaseXSD(run_settings, 'wstest-wsdl-default', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wstest-wsdl-noarrayof', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['--no-arrayof']),

        TestCaseXSD(run_settings, 'wstest-wsdl-iv1', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1']),

        TestCaseXSD(run_settings, 'wstest-wsdl-iv2', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '2']),

        TestCaseXSD(run_settings, 'wstest-wsdl-iv3', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '3']),

        TestCaseXSD(run_settings, 'wstest-wsdl-uvns', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '3', '-uvns']),

        TestCaseXSD(run_settings, 'wstest-wsdl-allannot', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['--annotate', 'all']),

        TestCaseXSD(run_settings, 'wstest-wsdl-noannot', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1', '--annotate', 'none']), # -iv for smaller output

        TestCaseXSD(run_settings, 'wstest-wsdl-opt', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1', '-opt', 'developer']),

        # --noopt isn't fully implemented, enable test case once it is
        #TestCaseXSD(run_settings, 'wstest-wsdl-noopt', 'wsdl', 'ws_test.ecm', 'WsTest',
        #            xsl_base_path, ['-iv', '1', '--noopt']),

        TestCaseXSD(run_settings, 'wstest-wsdl-tns', 'wsdl', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1', '-tns', 'urn:passed:name:space']),

        # xsd
        TestCaseXSD(run_settings, 'wstest-xsd-default', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wstest-xsd-noarrayof', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['--no-arrayof']),

        TestCaseXSD(run_settings, 'wstest-xsd-iv1', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1']),

        TestCaseXSD(run_settings, 'wstest-xsd-iv2', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '2']),

        TestCaseXSD(run_settings, 'wstest-xsd-iv3', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '3']),

        TestCaseXSD(run_settings, 'wstest-xsd-uvns', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '3', '-uvns']),

        TestCaseXSD(run_settings, 'wstest-xsd-allannot', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['--annotate', 'all']),

        TestCaseXSD(run_settings, 'wstest-xsd-noannot', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1', '--annotate', 'none']), # -iv for smaller output

        TestCaseXSD(run_settings, 'wstest-xsd-opt', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1', '-opt', 'developer']),

        # --noopt isn't fully implemented, enable test case once it is
        #TestCaseXSD(run_settings, 'wstest-xsd-noopt', 'xsd', 'ws_test.ecm', 'WsTest',
        #            xsl_base_path, ['-iv', '1', '--noopt']),

        TestCaseXSD(run_settings, 'wstest-xsd-tns', 'xsd', 'ws_test.ecm', 'WsTest',
                    xsl_base_path, ['-iv', '1', '-tns', 'urn:passed:name:space']),

        # cpp
        TestCaseCode(run_settings, 'wstest-cpp-installdir', 'cpp', 'ws_test.ecm', 'WsTest',
                     xsl_base_path)

    ]

    for case in test_cases:
        case.run_test()

    logging.info('Success count: %d', stats.successCount)
    logging.info('Failure count: %d', stats.failureCount)




if __name__ == "__main__":
  main()