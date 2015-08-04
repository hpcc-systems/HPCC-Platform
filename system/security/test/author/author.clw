;/*##############################################################################
;
;    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
;
;    This program is free software: you can redistribute it and/or modify
;    you may not use this file except in compliance with the License.
;    You may obtain a copy of the License at
;
;       http://www.apache.org/licenses/LICENSE-2.0
;
;    Unless required by applicable law or agreed to in writing, software
;    distributed under the License is distributed on an "AS IS" BASIS,
;    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;    See the License for the specific language governing permissions and
;    limitations under the License.
;############################################################################## */

; CLW file contains information for the MFC ClassWizard

[General Info]
Version=1
LastClass=CAuthorDlg
LastTemplate=CDialog
NewFileInclude1=#include "stdafx.h"
NewFileInclude2=#include "author.h"

ClassCount=2
Class1=CAuthorApp
Class2=CAuthorDlg

ResourceCount=3
Resource2=IDR_MAINFRAME
Resource3=IDD_AUTHOR_DIALOG

[CLS:CAuthorApp]
Type=0
HeaderFile=author.h
ImplementationFile=author.cpp
Filter=N

[CLS:CAuthorDlg]
Type=0
HeaderFile=authorDlg.h
ImplementationFile=authorDlg.cpp
Filter=D
LastObject=CAuthorDlg
BaseClass=CDialog
VirtualFilter=dWC



[DLG:IDD_AUTHOR_DIALOG]
Type=1
Class=CAuthorDlg
ControlCount=22
Control1=IDC_STATIC,static,1342308352
Control2=IDC_USER,edit,1350631552
Control3=IDC_STATIC,static,1342308352
Control4=IDC_PASSWORD,edit,1350631584
Control5=IDC_FEATURE_A,button,1342242819
Control6=IDC_FEATURE_B,button,1342242819
Control7=IDC_FEATURE_C,button,1342242819
Control8=IDC_FEATURE_D,button,1342242819
Control9=IDC_FEATURE_E,button,1342242819
Control10=IDC_FEATURE_F,button,1342242819
Control11=IDC_FEATURE_G,button,1342242819
Control12=IDC_FEATURE_H,button,1342242819
Control13=IDC_FEATURE_I,button,1342242819
Control14=IDC_BTN_AUTHORIZE,button,1342242816
Control15=IDC_STATIC,button,1342177287
Control16=IDC_STATIC,static,1342308352
Control17=IDC_SERVICE_NAME,edit,1350631552
Control18=IDC_BTN_CREATE_SEC_MGR,button,1342242816
Control19=IDC_STATIC,button,1342177287
Control20=IDC_PERMISSIONS,edit,1350635652
Control21=IDC_BTN_CREATE_USER,button,1342242816
Control22=IDC_BTN_BUILD_FEATURE_SET,button,1342242816

