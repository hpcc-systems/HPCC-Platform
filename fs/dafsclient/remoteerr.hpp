/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef REMOTEERR_HPP
#define REMOTEERR_HPP

#include "errorlist.h"

#ifdef REMOTE_EXPORTS
#define REMOTE_API DECL_EXPORT
#else
#define REMOTE_API DECL_IMPORT
#endif

#define RFSERR_InvalidCommand                   (REMOTE_ERROR_START + 0)
#define RFSERR_NullFileIOHandle                 (REMOTE_ERROR_START + 1)
#define RFSERR_InvalidFileIOHandle              (REMOTE_ERROR_START + 2)
#define RFSERR_TimeoutFileIOHandle              (REMOTE_ERROR_START + 3)
#define RFSERR_OpenFailed                       (REMOTE_ERROR_START + 4)
#define RFSERR_ReadFailed                       (REMOTE_ERROR_START + 5)
#define RFSERR_WriteFailed                      (REMOTE_ERROR_START + 6)
#define RFSERR_RenameFailed                     (REMOTE_ERROR_START + 7)
#define RFSERR_ExistsFailed                     (REMOTE_ERROR_START + 9)
#define RFSERR_RemoveFailed                     (REMOTE_ERROR_START + 10)
#define RFSERR_CloseFailed                      (REMOTE_ERROR_START + 11)
#define RFSERR_IsFileFailed                     (REMOTE_ERROR_START + 12)
#define RFSERR_IsDirectoryFailed                (REMOTE_ERROR_START + 13)
#define RFSERR_IsReadOnlyFailed                 (REMOTE_ERROR_START + 14)
#define RFSERR_SetReadOnlyFailed                (REMOTE_ERROR_START + 15)
#define RFSERR_GetTimeFailed                    (REMOTE_ERROR_START + 16)
#define RFSERR_SetTimeFailed                    (REMOTE_ERROR_START + 17)
#define RFSERR_CreateDirFailed                  (REMOTE_ERROR_START + 18)
#define RFSERR_GetDirFailed                     (REMOTE_ERROR_START + 19)
#define RFSERR_GetCrcFailed                     (REMOTE_ERROR_START + 20)
#define RFSERR_MoveFailed                       (REMOTE_ERROR_START + 21)
#define RFSERR_ExtractBlobElementsFailed        (REMOTE_ERROR_START + 22)
#define RFSERR_CopyFailed                       (REMOTE_ERROR_START + 23)
#define RFSERR_AppendFailed                     (REMOTE_ERROR_START + 24)
#define RFSERR_AuthenticateFailed               (REMOTE_ERROR_START + 25)
#define RFSERR_CopySectionFailed                (REMOTE_ERROR_START + 26)
#define RFSERR_TreeCopyFailed                   (REMOTE_ERROR_START + 27)
#define RFSERR_StreamReadFailed                 (REMOTE_ERROR_START + 28)
#define RFSERR_InternalError                    (REMOTE_ERROR_START + 29)


#define RFSERR_KeyIndexFailed                   (REMOTE_ERROR_START + 39)
#define RAERR_InvalidUsernamePassword           (REMOTE_ERROR_START + 40)
#define RFSERR_MasterSeemsToHaveDied            (REMOTE_ERROR_START + 41)
#define RFSERR_TimeoutWaitSlave                 (REMOTE_ERROR_START + 42)
#define RFSERR_TimeoutWaitConnect               (REMOTE_ERROR_START + 43)
#define RFSERR_TimeoutWaitMaster                (REMOTE_ERROR_START + 44)
#define RFSERR_NoConnectSlave                   (REMOTE_ERROR_START + 45)
#define RFSERR_NoConnectSlaveXY                 (REMOTE_ERROR_START + 46)
#define RFSERR_VersionMismatch                  (REMOTE_ERROR_START + 47)
#define RFSERR_SetThrottleFailed                (REMOTE_ERROR_START + 48)
#define RFSERR_MaxQueueRequests                 (REMOTE_ERROR_START + 49)

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define RFSERR_InvalidCommand_Text              "Unrecognised command %d"
#define RFSERR_NullFileIOHandle_Text            "Remote file operation on NULL fileio"
#define RFSERR_InvalidFileIOHandle_Text         "Remote file operation on invalid fileio"
#define RFSERR_TimeoutFileIOHandle_Text         "Remote fileio has been closed because of timeout"
#define RFSERR_MasterSeemsToHaveDied_Text       "Master program seems to have died..."
#define RFSERR_VersionMismatch_Text             "Slave version does not match, expected %d got %d"
#define RFSERR_SetThrottleFailed_Text           "Failed to set throttle limit"

#define RFSERR_TimeoutWaitSlave_Text            "Timeout waiting for slave %s to respond"
#define RFSERR_TimeoutWaitConnect_Text          "Timeout waiting to connect to slave %s"
#define RFSERR_TimeoutWaitMaster_Text           "Timeout waiting to connect to master"
#define RFSERR_NoConnectSlave_Text              "Failed to start slave program"
#define RFSERR_NoConnectSlaveXY_Text            "Failed to start slave program %s on %s"

#define RAERR_InvalidUsernamePassword_Text      "Invalid (upper case U) in username/password"

#define RFSERR_GetDirFailed_Text                "Failed to open file."

interface REMOTE_API IDAFS_Exception: extends IException
{ // Raise by dafilesrv calls
};


#endif
