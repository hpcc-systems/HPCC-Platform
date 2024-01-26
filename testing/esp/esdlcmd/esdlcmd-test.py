#! /usr/bin/python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.
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
import difflib
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

# This class is base for commands related to services: wsdl, xsd, cpp and java.
class TestCaseBase:

    def __init__(self, run_settings, name, command, options=None, expected_err=None):
        self.run_settings = run_settings
        self.name = name
        self.command = command
        self.options = options
        self.expected_err = expected_err
        self.result = None

    def run_test(self):
        logging.debug("Test %s args: %s", self.name, str(self.args))
        self.result = subprocess.run(self.args, capture_output=True)

        # expected_err is a tuple: (<expected_result_code>, <expected_stderr_message>)
        # It can be used to confirm a failure and error message, or a successful run
        # where the result code = 0 but a warning is printed to stderr
        # In the case where the expected result code = 0 (success) then we still validate
        # the results. In this case, passing the test consists of validating both the
        # warning message and the results.
        # The returned error need only contain the expected_stderr_message, it doesn't
        # need to be the entire message.

        if self.expected_err:
            if self.result.returncode != self.expected_err[0]:
                logging.error('Test failed: %s : Expected return code "%d" but saw return code "%d"', self.name, self.expected_err[0], self.result.returncode)
                success = False
            elif self.result.stderr.decode('UTF-8').find(self.expected_err[1]) == -1:
                logging.error('Test failed: %s : Expected error "%s" but saw error "%s"', self.name, self.expected_err[1], self.result.stderr)
                success = False
            else:
                if self.expected_err[0] == 0:
                    success = self.validate_results()
                else:
                    success = True
        else:
            if self.result.returncode != 0:
                logging.error('Test failed: %s : return code=%d, message=%s', self.name, self.result.returncode, str(self.result.stderr))
                success = False
            else:
                success = self.validate_results()

        self.run_settings.stats.add_count(success)

    def validate_results(self):
        """Compare test case results to the known key.

        Return True if the two are identical or False otherwise.
        """
        logging.debug('TestCaseBase implementation called for case "%s", no comparison run', self.name)
        return False


class TestCaseManifest(TestCaseBase):
    """
    Test case for the manifest command
    """

    def __init__(self, run_settings, name, manifest, options=None, expected_err=None):
        super().__init__(run_settings, name, 'manifest', options, expected_err)
        self.manifest = manifest
        self.output_path = Path(self.run_settings.output_base)
        self.output_file = self.output_path / (name+'.xml')
        self.args = [
            str(run_settings.exe_path),
            self.command,
            self.manifest,
            '--outfile', str(self.output_file),
        ]

        if options:
            self.args.extend(options)
        self.result = None

    def run_test(self):
        safe_mkdir(self.output_path)
        super().run_test()

    def validate_results(self):
        """
        Compare test case results to the known key.

        Return True if the two are identical or False otherwise.
        """
        key = (self.run_settings.test_path / 'key' / self.name).with_suffix('.xml')

        if (not key.exists()):
            logging.error('Missing key file %s', str(key))
            return False

        if (not self.output_file.exists()):
            logging.error('Missing output for test %s', self.name)
            return False

        if (not filecmp.cmp(str(key), str(self.output_file))):
            logging.debug('Comparing key %s to output %s shows differences:', str(key), str(self.output_file))
            if (logging.getLogger(__name__).isEnabledFor(logging.DEBUG)):
                with open(key) as ff:
                    keylines = ff.readlines()
                with open(self.output_file) as ff:
                    outlines = ff.readlines()
                diff = difflib.unified_diff(keylines, outlines)
                sys.stdout.writelines(diff)
            logging.error('Test failed: %s', self.name)
            return False
        else:
            logging.debug('Passed: %s', self.name)
            return True


class TestCaseEsdlBase(TestCaseBase):
    """
    Test case for commands that transform an ESDL Definition.
    """

    def __init__(self, run_settings, name, command, esdl_file, service, xsl_path, options=None, expected_err=None):
        super().__init__(run_settings, name, command, options, expected_err)
        self.esdl_path = (self.run_settings.test_path / 'inputs' / esdl_file)
        self.service = service
        self.xsl_path = xsl_path
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
        super().run_test()

# This class is the base for the 'transform' commands: ecl and xml.
# In a future update, investigate refactoring the base classes so there is a single
# parent with any shared capabilities.
#
# When writing to stdout, the key directory should contain a file named 'from-stdout.ecl'
# that contains the expected output.
class TestCaseTransformBase:
    def __init__(self, run_settings, name, command, esdl_file, xsl_path, use_stdout, expected_err=None, options=None):
        self.run_settings = run_settings
        self.name = name
        self.command = command
        self.esdl_path = (self.run_settings.test_path / 'inputs' / esdl_file)
        self.xsl_path = xsl_path
        self.options = options
        self.output_path = Path(self.run_settings.output_base) / name
        self.stdout = use_stdout
        self.expected_err = expected_err
        if self.stdout:
            self.args = [
                str(run_settings.exe_path),
                self.command,
                self.esdl_path,
                '-cde',
                self.xsl_path,
            ]
        else:
            self.args = [
                str(run_settings.exe_path),
                self.command,
                self.esdl_path,
                self.output_path,
                '-cde',
                self.xsl_path,
            ]

        if options:
            self.args.extend(options)

        self.result = None

    def run_test(self):
        safe_mkdir(self.output_path)
        logging.debug("Test %s args: %s", self.name, str(self.args))
        self.result = subprocess.run(self.args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if self.expected_err != None and self.expected_err == self.result.stderr:
            success = True
        elif self.result.returncode != 0:
            logging.error('Error running "esdl %s" for test "%s": %s', self.command, self.name, self.result.stderr)
            success = False
        else:
            success = self.validate_results()

        self.run_settings.stats.add_count(success)

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
        """Compare test case results to the known key.

        Return True if the two are identical or False otherwise.
        """
        outName = self.output_path
        key = (self.run_settings.test_path / 'key' / self.name)

        # When output was stdout, write the captured stdout text to a file named 'from-stdout.ecl'
        # in the output directory. This allows us to use the same method of comparing the key
        # and result.
        if self.stdout:
            if self.result.stdout != None and len(self.result.stdout) > 0:
                with open((outName / 'from-stdout.ecl'), 'w', encoding='utf-8') as f:
                    f.write(self.result.stdout)
            else:
                logging.error('Missing stdout output for test %s', self.name)
                return False

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


class TestCaseXSD(TestCaseEsdlBase):
    """Test case for the wsdl or xsd commands.

    Both generate a single file output, so test validation compares
    the contents of the output file with the key file.

    The path the xsl files not include the 'xslt' directory. The command
    assumes it needs to apped that directory itself.
    """

    def __init__(self, run_settings, name, command, esdl_file, service, xsl_path, options=None, expected_err=None):
        super().__init__(run_settings, name, command, esdl_file, service, xsl_path, options, expected_err)


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
            logging.debug('Comparing key %s to output %s shows differences:', str(key), str(outName))
            if (logging.getLogger(__name__).isEnabledFor(logging.DEBUG)):
                with open(key) as ff:
                    keylines = ff.readlines()
                with open(outName) as ff:
                    outlines = ff.readlines()
                diff = difflib.unified_diff(keylines, outlines)
                sys.stdout.writelines(diff)
            logging.error('Test failed: %s', self.name)
            return False
        else:
            logging.debug('Passed: %s', self.name)
            return True

class TestCaseCode(TestCaseEsdlBase):
    """Test case for the cpp or java commands.

    Both generate a directory full of output, so test validation compares
    the contents of the output directory with the key directory.

    The path the xsl files must be appended with 'xslt/' for the command
    to find the xslt files.
    """

    def __init__(self, run_settings, name, command, esdl_file, service, xsl_path, options=None, expected_err=None):
        # must end in a slash esdl command doesn't
        # add a slash before appending the file name
        xsl_cpp_path = str((xsl_path / 'xslt'))
        xsl_cpp_path += '/'
        super().__init__(run_settings, name, command, esdl_file, service, xsl_cpp_path, options, expected_err)
        self.compare_result = None


    def run_test(self):
        logging.debug("Test %s args: %s", self.name, str(self.args))
        self.result = subprocess.run(self.args, capture_output=True)

        # Special case behavior for this command because it returns -11 on success in some cases

        if self.expected_err:
            if self.result.returncode != self.expected_err[0]:
                logging.error('Test failed: %s : Expected return code "%d" but saw return code "%d"', self.name, self.expected_err[0], self.result.returncode)
                success = False
            elif self.result.stderr.decode('UTF-8').find(self.expected_err[1]) == -1:
                logging.error('Test failed: %s : Expected error "%s" but saw error "%s"', self.name, self.expected_err[1], self.result.stderr)
                success = False
            else:
                if self.expected_err[0] == 0:
                    success = self.validate_results()
                elif self.expected_err[0] == -11:
                    success = self.validate_results()
                else:
                    success = True
        else:
            if self.result.returncode != 0:
                logging.error('Test failed: %s : %s', self.name, str(self.result.stderr))
                success = False
            else:
                success = self.validate_results()
        self.run_settings.stats.add_count(success)


    def is_same(self, dir1, dir2):
        """
        Compare two directory trees content.
        Return False if they differ, True is they are the same.
        """
        compared = DirectoryCompare(dir1, dir2)
        if (compared.left_only or compared.right_only or compared.diff_files
            or compared.funny_files):
            self.compare_result = compared
            return False
        for subdir in compared.common_dirs:
            if not self.is_same(os.path.join(dir1, subdir), os.path.join(dir2, subdir)):
                self.compare_result = compared
                return False
        self.compare_result = compared
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
            # In future, check self.compare_result for more details if needed
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

    command_values = ['all', 'cpp', 'ecl', 'java', 'wsdl', 'xsd']

    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument('testroot',
                        help='Path of the root folder of the esdlcmd testing project.')

    parser.add_argument('-o', '--outdir',
                        help='Directory name of output for tests. Defaults to creating \'esdlcmd-test-output\' in the current directory.',
                        default='esdlcmd-test-output')

    parser.add_argument('-e', '--esdlpath',
                        help='Path to the esdl executable to test.')

    parser.add_argument('-x', '--xslpath',
                        help='Path to the folder containing ESP\'s xslt/*.xslt transforms. Defaults to /opt/HPCCSystems/componentfiles/',
                        default='/opt/HPCCSystems/componentfiles/')

    parser.add_argument('-d', '--debug',
                        help='Enable debug logging of test cases.',
                        action='store_true', default=False)

    parser.add_argument('-c', '--commands',
                         help='esdl commands to run tests for, use once for each command or pass "all" to test all commands. Defaults to "all".',
                         action="append", choices=command_values)

    args = parser.parse_args()

    if args.commands == None:
        args.commands = ['all']

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
    esdl_path = Path(args.esdlpath)
    if esdl_path.is_dir():
        exe_path = Path(args.esdlpath) / 'esdl'
    xsl_base_path = Path(args.xslpath)

    if (args.debug):
        loglevel = logging.DEBUG
    else:
        loglevel=logging.INFO
    logging.basicConfig(level=loglevel, format='[%(levelname)s] %(message)s')

    stats = Statistics()
    run_settings = TestRun(stats, exe_path, args.outdir, test_path)

    esdl_includes_path = str(test_path / 'inputs')

    expected_err_multi_file_incl = '\nOutput to stdout is not supported for multiple files. Either add the Rollup\noption or specify an output directory.\n'
    expected_err_multi_file_expanded = '\nOutput to stdout is not supported for multiple files. Remove the Output expanded\n XML option or specify an output directory.\n'

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
        # This command currently returns -11 for success, though its behaivor has changed across
        # platform builds- unsure of the reason why.
        TestCaseCode(run_settings, 'wstest-cpp-installdir', 'cpp', 'ws_test.ecm', 'WsTest',
                     xsl_base_path, None, expected_err=(-11,'Loading XML ESDL definition:')),

        # Testing exceptions_inline output
        TestCaseXSD(run_settings, 'wsexctest1-wsdl-default', 'wsdl', 'ws_exc_test_1.ecm', 'WsExcTest1',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wsexctest2-wsdl-default', 'wsdl', 'ws_exc_test_2.ecm', 'WsExcTest2',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wsexctest3-wsdl-default', 'wsdl', 'ws_exc_test_3.ecm', 'WsExcTest3',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wsexctest3-wsdl-no-exc', 'wsdl', 'ws_exc_test_3.ecm', 'WsExcTest3',
                    xsl_base_path, ['--no-exceptions-inline']),

        TestCaseXSD(run_settings, 'wsexctest1-xsd-default', 'xsd', 'ws_exc_test_1.ecm', 'WsExcTest1',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wsexctest2-xsd-default', 'xsd', 'ws_exc_test_2.ecm', 'WsExcTest2',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wsexctest3-xsd-default', 'xsd', 'ws_exc_test_3.ecm', 'WsExcTest3',
                    xsl_base_path),

        TestCaseXSD(run_settings, 'wsexctest3-xsd-no-exc', 'xsd', 'ws_exc_test_3.ecm', 'WsExcTest3',
                    xsl_base_path, ['--no-exceptions-inline']),

        # use_method_name tests

        # Shows how the name of the method is used as the xsd:element name for the request structure.
        # One element is created for each method that shares a request structure. Enabled when the
        # use_method_name option is present on the EsdlService definition.
#        TestCaseXSD(run_settings, 'use-method-name', 'wsdl', 'ws_usemethodname.ecm', 'WsUseMethodName',
#                    xsl_base_path),

        # Shows how the request structure name is used as the xsd:element name for the request structure.
        # A single element is created for each request structure defined. This is default behavior.
        #TestCaseXSD(run_settings, 'use-request-name', 'wsdl', 'ws_userequestname.ecm', 'WsUseRequestName',
        #            xsl_base_path),

        # ecl
        TestCaseTransformBase(run_settings, 'ecl-stdout-single', 'ecl', 'ws_test.ecm', xsl_base_path, use_stdout=True),

        TestCaseTransformBase(run_settings, 'ecl-stdout-incl-err', 'ecl', 'ws_test.ecm', xsl_base_path, use_stdout=True,
                              expected_err=expected_err_multi_file_incl, options=['-I', esdl_includes_path, '--includes']), 

        TestCaseTransformBase(run_settings, 'ecl-stdout-expanded-err', 'ecl', 'ws_test.ecm', xsl_base_path, use_stdout=True,
                              expected_err=expected_err_multi_file_expanded, options=['-x']),

        TestCaseTransformBase(run_settings, 'ecl-stdout-incl-rollup', 'ecl', 'ws_test.ecm', xsl_base_path, use_stdout=True,
                              options=['-I', esdl_includes_path, '--includes', '--rollup']),

        TestCaseTransformBase(run_settings, 'ecl-incl', 'ecl', 'ws_test.ecm', xsl_base_path, use_stdout=False,
                              options=['-I', esdl_includes_path, '--includes'])
    ]

    includes = ['-I', str(run_settings.test_path / 'inputs')]
    opts_w_format = ['--output-type', 'binding']
    opts_w_format.extend(includes)

    opts_w_service = ['--service', 'WsTest']
    opts_w_service.extend(includes)

    opts_bad_format = ['--output-type', 'invalidValue']
    opts_bad_format.extend(includes)

    manifest_cases = [

        # Expected error - can't find manifest file
        TestCaseManifest(run_settings, 'manifest-missing', 'missing-file.xml', includes,
            (1, 'Error reading manifest file (missing-file.xml): File missing-file.xml could not be opened\nError: Failed processsing manifest file. Exiting.\n')),

        # Basic test of all features- including script, XSLT and ESDL definition using search paths
        TestCaseManifest(run_settings, 'manifest-full-bundle', 'inputs/manifest-full-bundle.xml', includes),

        # Override the default Bundle output to write out a binding
        TestCaseManifest(run_settings, 'manifest-binding-option-override', 'inputs/manifest-full-bundle.xml', opts_w_format),

        # Test behavior to output bundle with no ESDL Definition
        TestCaseManifest(run_settings, 'manifest-no-esdl-defn', 'inputs/manifest-no-esdl-defn.xml', includes,
            (1, 'Generating EsdlBundle output without ESDL Definition is not supported.')),

        # Error returned when manifest namespace is incorrect
        TestCaseManifest(run_settings, 'manifest-bad-manifest-namespace', 'inputs/manifest-bad-namespace.xml', includes,
            (1, 'Error reading manifest file (inputs/manifest-bad-namespace.xml): Manifest file using incorrect namespace URI \'urn:wrong:namespace\', must be \'urn:hpcc:esdl:manifest\'\nError: Failed processsing manifest file. Exiting.\n')),

        # When an element doesn't have the expected manifest namespace the warning returned and results match what's expected
        TestCaseManifest(run_settings, 'manifest-bad-include-namespace', 'inputs/manifest-bad-include-namespace.xml', includes,
            (0, 'Found node named \'Include\' with namespace \'\', but the namespace \'urn:hpcc:esdl:manifest\' may have been intended.\nSuccess\n')),

        # Confirm error for bad --output-type option
        TestCaseManifest(run_settings, 'manifest-bad-format-option', 'inputs/manifest-bad-format.xml', opts_bad_format,
            (2, 'Unknown value \'invalidValue\' provided for --output-type option.\n')),

        # Confirm error for bad outputType attribute
        TestCaseManifest(run_settings, 'manifest-bad-format-attr', 'inputs/manifest-bad-format.xml', includes,
            (1, 'Error reading manifest file (inputs/manifest-bad-format.xml): Unknown value \'invalidValue\' provided for attribute Manifest/@outputType\nError: Failed processsing manifest file. Exiting.\n')),

        # Confirm error for missing ServiceBinding[@service] attribute
        TestCaseManifest(run_settings, 'manifest-bad-service-attr', 'inputs/manifest-bad-service.xml', includes,
            (1, 'Error reading manifest file (inputs/manifest-bad-service.xml): ESDL Service Definition not found for WsWillCauseFailure\nError: Failed processsing manifest file. Exiting.\n')),

        # Show that scripts inline in the Manifest are processed correctly
        TestCaseManifest(run_settings, 'manifest-inline-script-defn', 'inputs/manifest-inline.xml', includes),

        # Show how default namespace defined in root node of manifest is output for
        # parsed nodes that are using the default ns. Note that we can only detect
        # a default namespace when looking at a start tag and see it with no prefix
        # and a non-empty uri (though technically an empty uri can be a default ns uri)
        # This is why we see the default ns output for the first time on <Methods>
        # rather than on <Binding>. This is arguably incorrect, but is is also likely
        # incorrect to define a default ns on the root node that will apply to the
        # <Binding>. If we need a change in behavior it will require an update to xpp.
        TestCaseManifest(run_settings, 'manifest-ns-def-root', 'inputs/manifest-ns-def-root.xml', includes),

        # Confirm error for multiple ServiceBindings
        TestCaseManifest(run_settings, 'manifest-doublesvc', 'inputs/manifest-doublesvc.xml', includes,
            (1, 'Only one ServiceBinding per manifest is supported.')),

        # Shows how a namespace defn on the <em:Scripts> element is moved to the <Scripts>
        # enclosed in the CDATA section, and how the parser recognizes that the defn is not
        # required on the inline script, but remains in the Included script because it isn't parsed
        TestCaseManifest(run_settings, 'manifest-inline-ns', 'inputs/manifest-inline-ns.xml', includes),

        # Show how a different namespaces defined in the root <Scripts> element of multiple
        # included files are moved to the <Scripts> element enclosed in the CDATA section
        TestCaseManifest(run_settings, 'manifest-multi-include-diffprefix', 'inputs/manifest-multi-include-diffprefix.xml', includes),

        # Show how multiple included scripts in the same <Scripts> node have the namespaces
        # remain as defined in the Entry Point nodes.
        TestCaseManifest(run_settings, 'manifest-multi-include', 'inputs/manifest-multi-include.xml', includes),

        # Show handing of CDATA in different situations. When inline it is escaped.
        # When in an Included file that will be output in a CDATA section, any included CDATA end markup
        # is encoded as ]]]]><![CDATA[> to avoid nesting CDATA which is disallowed.
        # The ESDL Script in this example is not valid, but used to verify that the tool is correctly
        # encoding previously-encoded CDATA sections.
        TestCaseManifest(run_settings, 'manifest-cdata', 'inputs/manifest-cdata.xml', includes),

        # Show behavior of inline xslt with escaped content
        TestCaseManifest(run_settings, 'manifest-inline-xslt', 'inputs/manifest-inline-xslt.xml', includes),

        # Show behavior of inline xslt with CDATA content.
        TestCaseManifest(run_settings, 'manifest-inline-cdata-xslt', 'inputs/manifest-inline-cdata-xslt.xml', includes),

        # Show treatment of different types of attributes on the ServiceBinding element for binding output
        TestCaseManifest(run_settings, 'manifest-binding-attrs', 'inputs/manifest-binding-attrs.xml', opts_w_format),

        # Show treatment of different types of attributes on the ServiceBinding element for bundle output
        TestCaseManifest(run_settings, 'manifest-bundle-attrs', 'inputs/manifest-binding-attrs.xml', includes),

        # Confirm error for multiple ServiceBindings
        TestCaseManifest(run_settings, 'manifest-doubleesdl', 'inputs/manifest-doubleesdl.xml', includes,
            (1, 'Only one EsdlDefinition per manifest is supported.')),

        # Shows that unrecognized markup in the em namespace is copied through to output
        TestCaseManifest(run_settings, 'manifest-unknown-markup', 'inputs/manifest-unknown-markup.xml', includes),

    ]

    for case in test_cases:
        if 'all' in args.commands or case.command in args.commands:
            case.run_test()

    for case in manifest_cases:
        case.run_test()

    logging.info('Success count: %d', stats.successCount)
    logging.info('Failure count: %d', stats.failureCount)


if __name__ == "__main__":
  main()
