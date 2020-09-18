'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems(R).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

import logging
import os
import sys
import inspect
import traceback

from ...common.shell import Shell
from ...common.error import Error
from ...util.util import queryWuid, getConfig, clearOSCache

import xml.etree.ElementTree as ET

logger = logging.getLogger('RegressionTestEngine')

class ECLcmd(Shell):
    def __init__(self):
        self.defaults = []
        self.cmd = 'ecl'
        self.config = getConfig()

    def __ECLcmd(self):
        return self.command(self.cmd, *self.defaults)

    def runCmd(self, cmd, engine, cluster, eclfile, report, **kwargs):
        args = []
        args.append(cmd)
        args.append('-v')
        args.append('-fpickBestEngine=false')
        args.append('--target=' + engine)
        args.append('--cluster=' + cluster)
        args.append('--port=' + self.config.espSocket)
        if self.config.useSsl.lower() == 'true':
            args.append('--ssl')
        
        retryCount = int(kwargs.pop('retryCount',  1))
        args.append('--wait='+str(retryCount * eclfile.getTimeout() * 1000))  # ms

        server = kwargs.pop('server', False)
        if server:
            args.append('--server=' + server)

        username = kwargs.pop('username', False)
        if username:
                args.append("--username=" + username)

        password = kwargs.pop('password', False)
        if password:
            args.append("--password=" + password)

        args = args + eclfile.getFParameters()

        if cmd == 'publish':
            args.append(eclfile.getArchiveName())

            name = kwargs.pop('name', False)
            if not name:
                name = eclfile.getBaseEclName()

            args.append("--name=" + name)

        else:
            args.append('--exception-level=warning')
            args.append('--noroot')

            name = kwargs.pop('name', False)
            if not name:
                name = eclfile.getJobname()

            args.append("--name=" + name)

            args = args + eclfile.getDParameters()

            args = args + eclfile.getStoredInputParameters()


            args.append(eclfile.getArchiveName())

        data = ""
        wuid = "N/A"
        state = ""
        results=''
        try:
            if eclfile.flushDiskCache():
                # At the moment it is not a critical problem if the clearOSCache()
                # fails, the test case should execute anyway
                try:
                    clearOSCache(eclfile.getTaskId())
                    pass
                finally:
                    # Go on
                    pass
            results, stderr = self.__ECLcmd()(*args)
            logger.debug("%3d. results:'%s'", eclfile.getTaskId(),  results)
            logger.debug("%3d. stderr :'%s'", eclfile.getTaskId(),  stderr)
            data = '\n'.join(line for line in
                             results.split('\n') if line) + "\n"
            ret = data.split('\n')
            result = ""
            cnt = 0
            for i in ret:
                i = i
                logger.debug("%3d. i(%d):'%s'", eclfile.getTaskId(), cnt, i )

                if "wuid:" in i:
                    logger.debug("------ runCmd:" + repr(i) + "------")
                    wuid = i.split()[1]
                if "state:" in i:
                    state = i.split()[1]
                if "aborted" in i:
                    state = "aborted"
                if cnt > 4:
                    if i.startswith('<Warning>') or i.startswith('<Exception>'):
                        # Remove absolute path from filename to 
                        # enable to compare it with same part of keyfile
                        xml = ET.fromstring(i)
                        try:
                            path = xml.find('.//Filename').text
                            logger.debug("%3d. path:'%s'", eclfile.getTaskId(),  path )
                            filename = os.path.basename(path)
                            xml.find('.//Filename').text = filename
                        except:
                            logger.debug("%3d. Unexpected error: %s (line: %s) ", eclfile.getTaskId(), str(sys.exc_info()[0]), str(inspect.stack()[0][2]))
                        finally:
                            i = ET.tostring(xml).decode("utf-8")
                        logger.debug("%3d. ret:'%s'", eclfile.getTaskId(),  i )
                        pass
                    try:    
                        result += i + "\n"
                    except:
                        logger.error("%3d. type of i: '%s', i: '%s'", eclfile.getTaskId(), type(i), i )
                cnt += 1
            data = '\n'.join(line for line in
                             result.split('\n') if line) + "\n"

        except Error as err:
            data = str(err)
            logger.debug("------" + data + "------")
            raise err
        except:
            err = Error("6007")
            logger.critical(err)
            logger.critical(traceback.format_exc())
            raise err
        finally:
            res = queryWuid(eclfile.getJobname(), eclfile.getTaskId())
            logger.debug("%3d. in finally -> 'wuid':'%s', 'state':'%s', data':'%s', ", eclfile.getTaskId(), res['wuid'], res['state'], data)
            if wuid ==  'N/A':
                logger.debug("%3d. in finally queryWuid() -> 'result':'%s', 'wuid':'%s', 'state':'%s'", eclfile.getTaskId(),  res['result'],  res['wuid'],  res['state'])
                wuid = res['wuid']
                if res['result'] != "OK":
                    eclfile.diff=eclfile.getBaseEcl()+'\n\t'+res['state']+'\n'
                    logger.error("%3d. %s in queryWuid(%s)",  eclfile.getTaskId(),  res['state'],  eclfile.getJobname())

            try:
                eclfile.addResults(data, wuid)
            except IOError as e:
                logger.critical("Exception in eclfile.addResults() -> errNo: %d" % (e.errno))
                logger.critical(traceback.format_exc())
                if e.errno == 28:
                    # No space left on device
                    raise Error("9000")
                else:
                    raise Error("6007")
            except:
                logger.critical("Exception in eclfile.addResults()")
                logger.critical(traceback.format_exc())
                
            if cmd == 'publish':
                if state == 'compiled':
                    test = True
                else:
                    test = False
                    eclfile.diff = 'Error'
            else:
                if (res['state'] == 'aborted') or eclfile.isAborted():
                    eclfile.diff = ("%3d. Test: %s\n") % (eclfile.taskId, eclfile.getBaseEclRealName())
                    eclfile.diff += '\t'+'Aborted ( reason: '+eclfile.getAbortReason()+' )'
                    test = False
                elif eclfile.getIgnoreResult():
                    logger.debug("%3d. Ignore result (ecl:'%s')", eclfile.getTaskId(),  eclfile.getBaseEclRealName())
                    test = True
                elif eclfile.testFail():
                   if res['state'] == 'completed':
                        logger.debug("%3d. Completed but Fail is the expected result (ecl:'%s')", eclfile.getTaskId(),  eclfile.getBaseEclRealName())
                        test = False
                   else:
                        logger.debug("%3d. Fail is the expected result (ecl:'%s')", eclfile.getTaskId(),  eclfile.getBaseEclRealName())
                        test = True
                elif eclfile.testNoKey():
                    # keyfile comparaison disabled with //nokey tag
                    if eclfile.testNoOutput():
                        #output generation disabled with //nooutput tag
                        eclfile.diff = '-'
                    else:
                        eclfile.diff = ("%3d. Test: %s\n") % (eclfile.taskId, eclfile.getBaseEclRealName())
                        eclfile.diff += data
                    test = True
                elif (res['state'] == 'failed'):
                    logger.debug("%3d. in state == failed 'wuid':'%s', 'state':'%s', data':'%s', ", eclfile.getTaskId(), res['wuid'], res['state'], data)
                    resultLines = data.strip().split('\n')
                    resultLineIndex = 0;
                    while resultLineIndex < len(resultLines) and not resultLines[resultLineIndex].startswith('<'):
                        resultLineIndex += 1
                    logger.debug("%3d. State is fail (resultLineIndex:%d, resultLines:'%s' )", eclfile.getTaskId(), resultLineIndex,  resultLines)
                    data = '\n'.join(resultLines[resultLineIndex:])+ "\n"
                    eclfile.addResults(data, wuid)
                    logger.debug("%3d. State is fail (resultLineIndex:%d, data:'%s' )", eclfile.getTaskId(), resultLineIndex,  data)
                    test = eclfile.testResults()
                else:
                    test = eclfile.testResults()
            report.addResult(eclfile)
            if not test:
                return False
            else:
                return True
