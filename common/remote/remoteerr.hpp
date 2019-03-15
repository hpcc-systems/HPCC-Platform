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

#ifdef REMOTE_EXPORTS
#define REMOTE_API DECL_EXPORT
#else
#define REMOTE_API DECL_IMPORT
#endif

#define ERR_REMOTE_FIRST                        8000
#define ERR_REMOTE_LAST                         8049

#define RFSERR_InvalidCommand                   8000
#define RFSERR_NullFileIOHandle                 8001
#define RFSERR_InvalidFileIOHandle              8002
#define RFSERR_TimeoutFileIOHandle              8003
#define RFSERR_OpenFailed                       8004
#define RFSERR_ReadFailed                       8005
#define RFSERR_WriteFailed                      8006
#define RFSERR_RenameFailed                     8007
#define RFSERR_ExistsFailed                     8009
#define RFSERR_RemoveFailed                     8010
#define RFSERR_CloseFailed                      8011
#define RFSERR_IsFileFailed                     8012
#define RFSERR_IsDirectoryFailed                8013
#define RFSERR_IsReadOnlyFailed                 8014
#define RFSERR_SetReadOnlyFailed                8015
#define RFSERR_GetTimeFailed                    8016
#define RFSERR_SetTimeFailed                    8017
#define RFSERR_CreateDirFailed                  8018
#define RFSERR_GetDirFailed                     8019
#define RFSERR_GetCrcFailed                     8020
#define RFSERR_MoveFailed                       8021
#define RFSERR_ExtractBlobElementsFailed        8022
#define RFSERR_CopyFailed                       8023
#define RFSERR_AppendFailed                     8024
#define RFSERR_AuthenticateFailed               8025
#define RFSERR_CopySectionFailed                8026
#define RFSERR_TreeCopyFailed                   8027
#define RFSERR_StreamReadFailed                 8028
#define RFSERR_InternalError                    8029


#define RAERR_InvalidUsernamePassword           8040
#define RFSERR_MasterSeemsToHaveDied            8041
#define RFSERR_TimeoutWaitSlave                 8042
#define RFSERR_TimeoutWaitConnect               8043
#define RFSERR_TimeoutWaitMaster                8044
#define RFSERR_NoConnectSlave                   8045
#define RFSERR_NoConnectSlaveXY                 8046
#define RFSERR_VersionMismatch                  8047
#define RFSERR_SetThrottleFailed                8048
#define RFSERR_MaxQueueRequests                 8049
#define RFSERR_KeyIndexFailed                   8050

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
