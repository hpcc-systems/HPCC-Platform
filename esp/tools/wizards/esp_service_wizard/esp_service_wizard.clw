; CLW file contains information for the MFC ClassWizard

[General Info]
Version=1
LastClass=CEspMainWizDlg
LastTemplate=CDialog
NewFileInclude1=#include "stdafx.h"
NewFileInclude2=#include "esp_service_wizard.h"
LastPage=0

ClassCount=4
Class1=CEspMainWizDlg
Class2=CEspRequestWizDlg
Class3=CEspResponseWizDlg

ResourceCount=5
Resource1=IDD_CUSTOM4
Resource2=IDD_CUSTOM1
Resource3=IDD_CUSTOM2
Class4=CEspDefWizDlg
Resource4=IDD_CUSTOM3
Resource5=IDD_DIALOG1

[CLS:CEspMainWizDlg]
Type=0
HeaderFile=wiz_dlg_main.h
ImplementationFile=wiz_dlg_main.cpp
BaseClass=CAppWizStepDlg
LastObject=IDC_EXISTING_SCM_FILE

[CLS:CEspRequestWizDlg]
Type=0
BaseClass=CAppWizStepDlg
HeaderFile=request_wiz_dlg.h
ImplementationFile=request_wiz_dlg.cpp

[CLS:CEspResponseWizDlg]
Type=0
BaseClass=CAppWizStepDlg
HeaderFile=response_wiz_dlg.h
ImplementationFile=response_wiz_dlg.cpp

[DLG:IDD_CUSTOM1]
Type=1
Class=CEspMainWizDlg
ControlCount=10
Control1=IDC_SERVICE_NAME,edit,1350631552
Control2=IDC_ABBREV,edit,1350631552
Control3=IDC_STATIC,static,1342308352
Control4=IDC_STATIC,static,1342308352
Control5=IDC_EXISTING_SCM_FILE,edit,1350631552
Control6=IDC_STATIC,static,1342308352
Control7=IDC_BROWSE_SCM_FILE,button,1342242816
Control8=IDC_ESP_PATH,edit,1350631552
Control9=IDC_STATIC,static,1342308352
Control10=IDC_BROWSE_ESP_ROOT,button,1342242816

[DLG:IDD_CUSTOM3]
Type=1
Class=CEspRequestWizDlg
ControlCount=8
Control1=IDC_STATIC,static,1342308352
Control2=IDC_PARAMETER_LIST,listbox,1352728833
Control3=IDC_PARAMETER,edit,1350631552
Control4=IDC_STATIC,static,1342308352
Control5=IDC_BTN_ADD_PARAMETER,button,1342242816
Control6=IDC_PARAMETER_TYPE,combobox,1344339971
Control7=IDC_STATIC,static,1342308352
Control8=IDC_BTN_DELETE,button,1342242816

[DLG:IDD_CUSTOM4]
Type=1
Class=CEspResponseWizDlg
ControlCount=8
Control1=IDC_STATIC,static,1342308352
Control2=IDC_PARAMETER_LIST,listbox,1352728833
Control3=IDC_PARAMETER,edit,1350631552
Control4=IDC_STATIC,static,1342308352
Control5=IDC_BTN_ADD_PARAMETER,button,1342242816
Control6=IDC_PARAMETER_TYPE,combobox,1344339971
Control7=IDC_STATIC,static,1342308352
Control8=IDC_BTN_DELETE,button,1342242816

[DLG:IDD_CUSTOM2]
Type=1
Class=CEspDefWizDlg
ControlCount=5
Control1=IDC_METHOD,edit,1350631552
Control2=IDC_STATIC,static,1342308352
Control3=IDC_SCM_PATH,edit,1350631552
Control4=IDC_STATIC,static,1342308352
Control5=IDC_BROWSE_SCM_FILE,button,1342242816

[CLS:CEspDefWizDlg]
Type=0
HeaderFile=defines_wiz_dlg.h
ImplementationFile=defines_wiz_dlg.cpp
BaseClass=CDialog
Filter=D
VirtualFilter=dWC
LastObject=IDC_BROWSE_SCM_FILE

[DLG:IDD_DIALOG1]
Type=1
Class=?
ControlCount=2
Control1=IDOK,button,1342242817
Control2=IDCANCEL,button,1342242816

