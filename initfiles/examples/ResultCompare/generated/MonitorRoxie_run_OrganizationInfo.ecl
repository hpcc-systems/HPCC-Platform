/*** Generated Code do not hand edit ***/

/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

IMPORT cassandra;


IMPORT std;
IMPORT lib_timelib.TimeLib;


DiffStatus := MODULE

  EXPORT JoinRowType := MODULE
    export integer1 IsInner := 0;
    export integer1 OuterLeft := 1;
    export integer1 OuterRight := 2;
  END;

  // Records in the old and new results are matched by virtual record ID (VID) -- something,
  // which makes "this" record unique. Usually is defined by business logic.
  // If two records have the same VID, they are compared field by field.
  EXPORT State := MODULE
    export integer1 VOID           := 0; // no comparison occured (for example, no alerts were requested)
    export integer1 UNCHANGED      := 1; // no changes
    export integer1 UPDATED        := 2; // there are changes to a scalar field
    export integer1 ADDED          := 4; // record is new (with respect to VID)
    export integer1 DELETED        := 8; // record doesn't exist anymore
    export integer1 PREVIOUS      := 16; // for UPDATED records only: will only have populated fields which are changed
    export integer1 CHILD_UPDATED := 32; // a record has a child dataset which has some records ADDED, DELETED or UPDATED
  END;

  EXPORT string Convert (integer sts) :=
    MAP (sts = State.DELETED   => 'deleted',
         sts = State.ADDED     => 'added',
         sts = State.UPDATED   => 'updated',
         sts = State.UNCHANGED => '',
         '');
END;

request := MODULE

  EXPORT _lt_OrganizationInfoRequest := RECORD
    string OrgName {xpath('OrgName')};
  END;

END;

layouts := MODULE

  EXPORT DiffString := RECORD
    string7 _diff {XPATH('@diff')} := '';
    string value {XPATH('')} := '';
  END;

  EXPORT DiffStringRow := RECORD  (DiffString)
    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

  EXPORT DiffMetaRow := RECORD
    string name {XPATH ('@name')};
    string prior {XPATH ('@prior')};
  END;

  EXPORT DiffMetaRec := RECORD
    string7 _child_diff {XPATH('@child_diff')} := '';
    string7 _diff {XPATH('@diff')} := '';
    DATASET (DiffMetaRow) _diffmeta {XPATH ('DiffMeta/Field')} := DATASET ([], DiffMetaRow);
  END;
  EXPORT _lt_AddressInfo := RECORD (DiffMetaRec)
    string _type {xpath('type')};
    string Line1 {xpath('Line1')};
    string Line2 {xpath('Line2')};
    string City {xpath('City')};
    string State {xpath('State')};
    integer Zip {xpath('Zip')};
    integer Zip4 {xpath('Zip4')};
  END;

  EXPORT _lt_row_AddressInfo := RECORD  (_lt_AddressInfo)
    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

  EXPORT _lt_NameInfo := RECORD (DiffMetaRec)
    string First {xpath('First')};
    string Middle {xpath('Middle')};
    string Last {xpath('Last')};
    DATASET (DiffString) Aliases {xpath('Aliases/Alias'), MAXCOUNT(10)};
  END;

  EXPORT _lt_PersonInfo := RECORD (DiffMetaRec)
    _lt_NameInfo Name {xpath('Name')};
    dataset(_lt_AddressInfo) Addresses {xpath('Addresses/Address'), MAXCOUNT(10)};
  END;

  EXPORT _lt_row_PersonInfo := RECORD  (_lt_PersonInfo)
    integer _diff_ord {xpath('@diff_ord')} := 0;
  END;

  EXPORT _lt_OrganizationInfoResponse := RECORD
    string OrgName {xpath('OrgName')};
    _lt_AddressInfo Address {xpath('Address')};
    dataset(_lt_PersonInfo) Members {xpath('Members/Member'), MAXCOUNT(10)};
    dataset(_lt_PersonInfo) Guests {xpath('Guests/Guest'), MAXCOUNT(10)};
  END;

END;

difference := MODULE

  EXPORT boolean MonitorMembers := FALSE : STORED('Monitor_Members', FORMAT(sequence(11)));
  EXPORT boolean MonitorPeople := FALSE : STORED('Monitor_People', FORMAT(sequence(12)));
  EXPORT boolean MonitorAddresses := FALSE : STORED('Monitor_Addresses', FORMAT(sequence(13)));
  EXPORT boolean MonitorGuests := FALSE : STORED('Monitor_Guests', FORMAT(sequence(14)));



EXPORT _df_DiffString(boolean is_active, string path) := MODULE

  EXPORT layouts.DiffStringRow ProcessTxRow(layouts.DiffStringRow L, layouts.DiffStringRow R, integer1 joinRowType) :=TRANSFORM
    boolean is_deleted := joinRowType = DiffStatus.JoinRowType.OuterRight;
    boolean is_added := joinRowType = DiffStatus.JoinRowType.OuterLeft;


    integer _change := MAP (is_deleted  => DiffStatus.State.DELETED,
                      is_added    => DiffStatus.State.ADDED,
                      DiffStatus.State.UNCHANGED);

    SELF._diff := IF(is_active, DiffStatus.Convert (_change), '');
    SELF._diff_ord := IF (is_deleted, R._diff_ord, L._diff_ord);
    SELF := IF (is_deleted, R, L);

  END;

  EXPORT  integer1 CheckOuter(layouts.DiffString L, layouts.DiffString R) := FUNCTION
    boolean IsInner :=  (L.Value = R.Value);
    boolean IsOuterRight :=   (L.Value = '');
    return IF (IsInner, DiffStatus.JoinRowType.IsInner, IF (IsOuterRight, DiffStatus.JoinRowType.OuterRight, DiffStatus.JoinRowType.OuterLeft));
  END;

  EXPORT  AsDataset (dataset(layouts.DiffString) _n, dataset(layouts.DiffString) _o) := FUNCTION

    _new := PROJECT (_n, TRANSFORM (layouts.DiffStringRow, SELF._diff_ord := COUNTER, SELF := LEFT));
    _old := PROJECT (_o, TRANSFORM (layouts.DiffStringRow, SELF._diff_ord := 10000 + COUNTER, SELF := LEFT));
    ActiveJoin := JOIN (_new, _old,
                  LEFT.Value = RIGHT.Value,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter(LEFT, RIGHT)),
                  FULL OUTER,
                  LIMIT (0));
    PassiveJoin := JOIN (_new, _old,
                  LEFT.Value = RIGHT.Value,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter(LEFT, RIGHT)),
                  LEFT OUTER,
                  LIMIT (0));
    RETURN PROJECT(SORT(IF (is_active, ActiveJoin, PassiveJoin), _diff_ord), layouts.DiffString);
  END;

END;

  EXPORT _df_AddressInfo(boolean is_active, string path) := MODULE

  EXPORT DiffScalars (layouts._lt_AddressInfo L, layouts._lt_AddressInfo R, boolean is_deleted, boolean is_added) := MODULE
    shared boolean updated_Line1 := (L.Line1 != R.Line1);
    shared boolean updated_Line2 := (L.Line2 != R.Line2);
    shared boolean updated_City := (L.City != R.City);
    shared boolean updated_State := (L.State != R.State);
    Zip_active := CASE(path + '/Zip', '/Guests/Guest/Addresses/Address/Zip' => (false), is_active);
    shared boolean updated_Zip := Zip_active AND (L.Zip != R.Zip);
    Zip4_active := CASE(path + '/Zip4', '/Guests/Guest/Addresses/Address/Zip4' => (false), is_active);
    shared boolean updated_Zip4 := Zip4_active AND (L.Zip4 != R.Zip4);

    shared is_updated := false
      OR updated_Line1
      OR updated_Line2
      OR updated_City
      OR updated_State
      OR updated_Zip
      OR updated_Zip4;

    shared integer _change := MAP (is_deleted  => DiffStatus.State.DELETED,
                      is_added    => DiffStatus.State.ADDED,
                      is_updated  => DiffStatus.State.UPDATED,
                      DiffStatus.State.UNCHANGED);

    EXPORT _diff := DiffStatus.Convert (_change);
    // Get update information for all scalars
      _meta :=   IF (updated_Line1, DATASET ([{'Line1', R.Line1}], layouts.DiffMetaRow))
         +  IF (updated_Line2, DATASET ([{'Line2', R.Line2}], layouts.DiffMetaRow))
         +  IF (updated_City, DATASET ([{'City', R.City}], layouts.DiffMetaRow))
         +  IF (updated_State, DATASET ([{'State', R.State}], layouts.DiffMetaRow))
         +  IF (updated_Zip, DATASET ([{'Zip', R.Zip}], layouts.DiffMetaRow))
         +  IF (updated_Zip4, DATASET ([{'Zip4', R.Zip4}], layouts.DiffMetaRow));

    EXPORT _diffmeta := IF (~is_deleted AND ~is_added AND is_updated, _meta);
  END;

  EXPORT layouts._lt_AddressInfo ProcessTx(layouts._lt_AddressInfo L, layouts._lt_AddressInfo R, boolean is_deleted, boolean is_added) :=TRANSFORM
      m := DiffScalars(L, R, is_deleted, is_added);

      SELF._diff := IF(is_active, m._diff, '');
      SELF._diffmeta := IF(is_active, m._diffmeta);


      SELF := IF (is_deleted, R, L);

    END;


  EXPORT layouts._lt_row_AddressInfo ProcessTxRow(layouts._lt_row_AddressInfo L, layouts._lt_row_AddressInfo R, integer1 joinRowType) :=TRANSFORM
    boolean is_deleted := joinRowType = DiffStatus.JoinRowType.OuterRight;
    boolean is_added := joinRowType = DiffStatus.JoinRowType.OuterLeft;
    m := DiffScalars(L, R, is_deleted, is_added);

    SELF._diff := IF(is_active, m._diff, '');
    SELF._diffmeta := IF(is_active, m._diffmeta);

    SELF._diff_ord := IF (is_deleted, R._diff_ord, L._diff_ord);
    SELF := IF (is_deleted, R, L);

  END;


  EXPORT AsRecord (layouts._lt_AddressInfo _new, layouts._lt_AddressInfo _old) := FUNCTION
    RETURN ROW (ProcessTx(_new, _old, false, false));
  END;

  EXPORT  integer1 CheckOuter_cityline1state(layouts._lt_AddressInfo L, layouts._lt_AddressInfo R) := FUNCTION
    boolean IsInner :=  (L.State = R.State AND L.City = R.City AND L.Line1 = R.Line1);

    boolean IsOuterRight :=   (L.State = '' AND L.City = '' AND L.Line1 = '');
    return IF (IsInner, DiffStatus.JoinRowType.IsInner, IF (IsOuterRight, DiffStatus.JoinRowType.OuterRight, DiffStatus.JoinRowType.OuterLeft));
  END;
  EXPORT  AsDataset_cityline1state (dataset(layouts._lt_AddressInfo) _n, dataset(layouts._lt_AddressInfo) _o) := FUNCTION

    _new := PROJECT (_n, TRANSFORM (layouts._lt_row_AddressInfo, SELF._diff_ord := COUNTER, SELF := LEFT));
    _old := PROJECT (_o, TRANSFORM (layouts._lt_row_AddressInfo, SELF._diff_ord := 10000 + COUNTER, SELF := LEFT));
    ActiveJoin := JOIN (_new, _old,
                  LEFT.State = RIGHT.State AND LEFT.City = RIGHT.City AND LEFT.Line1 = RIGHT.Line1,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter_cityline1state(LEFT, RIGHT)),
                  FULL OUTER,
                  LIMIT (0));
    PassiveJoin := JOIN (_new, _old,
                  LEFT.State = RIGHT.State AND LEFT.City = RIGHT.City AND LEFT.Line1 = RIGHT.Line1,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter_cityline1state(LEFT, RIGHT)),
                  LEFT OUTER,
                  LIMIT (0));
    RETURN PROJECT(SORT(IF (is_active, ActiveJoin, PassiveJoin), _diff_ord), layouts._lt_AddressInfo);
  END;

END;

EXPORT _df_NameInfo(boolean is_active, string path) := MODULE

  EXPORT DiffScalars (layouts._lt_NameInfo L, layouts._lt_NameInfo R, boolean is_deleted, boolean is_added) := MODULE
    shared boolean updated_First := (L.First != R.First);
    shared boolean updated_Middle := (L.Middle != R.Middle);
    shared boolean updated_Last := (L.Last != R.Last);

    shared is_updated := false
      OR updated_First
      OR updated_Middle
      OR updated_Last;

    shared integer _change := MAP (is_deleted  => DiffStatus.State.DELETED,
                      is_added    => DiffStatus.State.ADDED,
                      is_updated  => DiffStatus.State.UPDATED,
                      DiffStatus.State.UNCHANGED);

    EXPORT _diff := DiffStatus.Convert (_change);
    // Get update information for all scalars
      _meta :=   IF (updated_First, DATASET ([{'First', R.First}], layouts.DiffMetaRow))
         +  IF (updated_Middle, DATASET ([{'Middle', R.Middle}], layouts.DiffMetaRow))
         +  IF (updated_Last, DATASET ([{'Last', R.Last}], layouts.DiffMetaRow));

    EXPORT _diffmeta := IF (~is_deleted AND ~is_added AND is_updated, _meta);
  END;

  EXPORT layouts._lt_NameInfo ProcessTx(layouts._lt_NameInfo L, layouts._lt_NameInfo R, boolean is_deleted, boolean is_added) :=TRANSFORM
      m := DiffScalars(L, R, is_deleted, is_added);

      SELF._diff := IF(is_active, m._diff, '');
      SELF._diffmeta := IF(is_active, m._diffmeta);

      updated_Aliases := _df_DiffString(is_active, path + '/Aliases/Alias').AsDataset(L.Aliases, R.Aliases);
      checked_Aliases := MAP (is_deleted => R.Aliases,
                              is_added => L.Aliases,
                              updated_Aliases);
      SELF.Aliases := checked_Aliases;


      SELF := IF (is_deleted, R, L);

    END;


  EXPORT AsRecord (layouts._lt_NameInfo _new, layouts._lt_NameInfo _old) := FUNCTION
    RETURN ROW (ProcessTx(_new, _old, false, false));
  END;

END;

EXPORT _df_PersonInfo(boolean is_active, string path) := MODULE

  EXPORT DiffScalars (layouts._lt_PersonInfo L, layouts._lt_PersonInfo R, boolean is_deleted, boolean is_added) := MODULE

    shared is_updated := false;

    shared integer _change := MAP (is_deleted  => DiffStatus.State.DELETED,
                      is_added    => DiffStatus.State.ADDED,
                      is_updated  => DiffStatus.State.UPDATED,
                      DiffStatus.State.UNCHANGED);

    EXPORT _diff := DiffStatus.Convert (_change);
    // Get update information for all scalars
      _meta :=  DATASET ([], layouts.DiffMetaRow);

    EXPORT _diffmeta := IF (~is_deleted AND ~is_added AND is_updated, _meta);
  END;

  EXPORT layouts._lt_PersonInfo ProcessTx(layouts._lt_PersonInfo L, layouts._lt_PersonInfo R, boolean is_deleted, boolean is_added) :=TRANSFORM
      m := DiffScalars(L, R, is_deleted, is_added);

      SELF._diff := IF(is_active, m._diff, '');
      SELF._diffmeta := IF(is_active, m._diffmeta);

      path_Name := path + '/Name';

      updated_Name := _df_NameInfo(CASE(path_Name, '/Guests/Guest/Name' => (false), is_active), path_Name).AsRecord(L.Name, R.Name);

      checked_Name := MAP (is_deleted => R.Name,
                              is_added => L.Name,
                              updated_Name);
      SELF.Name := checked_Name;

      updated_Addresses := _df_AddressInfo(CASE(path + '/Addresses', '/Members/Member/Addresses' => (MonitorAddresses OR MonitorMembers), '/Guests/Guest/Addresses' => (MonitorAddresses OR MonitorGuests), is_active), path + '/Addresses/Address').AsDataset_cityline1state(L.Addresses, R.Addresses);
      checked_Addresses := MAP (is_deleted => R.Addresses,
                              is_added => L.Addresses,
                              updated_Addresses);
      SELF.Addresses  := checked_Addresses;


      SELF := IF (is_deleted, R, L);

    END;


  EXPORT layouts._lt_row_PersonInfo ProcessTxRow(layouts._lt_row_PersonInfo L, layouts._lt_row_PersonInfo R, integer1 joinRowType) :=TRANSFORM
    boolean is_deleted := joinRowType = DiffStatus.JoinRowType.OuterRight;
    boolean is_added := joinRowType = DiffStatus.JoinRowType.OuterLeft;
    m := DiffScalars(L, R, is_deleted, is_added);

    SELF._diff := IF(is_active, m._diff, '');
    SELF._diffmeta := IF(is_active, m._diffmeta);

      path_Name := path + '/Name';

      updated_Name := _df_NameInfo(CASE(path_Name, '/Guests/Guest/Name' => (false), is_active), path_Name).AsRecord(L.Name, R.Name);

      checked_Name := MAP (is_deleted => R.Name,
                              is_added => L.Name,
                              updated_Name);
      SELF.Name := checked_Name;

      updated_Addresses := _df_AddressInfo(CASE(path + '/Addresses', '/Members/Member/Addresses' => (MonitorAddresses OR MonitorMembers), '/Guests/Guest/Addresses' => (MonitorAddresses OR MonitorGuests), is_active), path + '/Addresses/Address').AsDataset_cityline1state(L.Addresses, R.Addresses);
      checked_Addresses := MAP (is_deleted => R.Addresses,
                              is_added => L.Addresses,
                              updated_Addresses);
      SELF.Addresses  := checked_Addresses;

    SELF._diff_ord := IF (is_deleted, R._diff_ord, L._diff_ord);
    SELF := IF (is_deleted, R, L);

  END;


  EXPORT AsRecord (layouts._lt_PersonInfo _new, layouts._lt_PersonInfo _old) := FUNCTION
    RETURN ROW (ProcessTx(_new, _old, false, false));
  END;

  EXPORT  integer1 CheckOuter_name_firstname_last(layouts._lt_PersonInfo L, layouts._lt_PersonInfo R) := FUNCTION
    boolean IsInner :=  (L.Name.Last = R.Name.Last AND L.Name.First = R.Name.First);

    boolean IsOuterRight :=   (L.Name.Last = '' AND L.Name.First = '');
    return IF (IsInner, DiffStatus.JoinRowType.IsInner, IF (IsOuterRight, DiffStatus.JoinRowType.OuterRight, DiffStatus.JoinRowType.OuterLeft));
  END;
  EXPORT  AsDataset_name_firstname_last (dataset(layouts._lt_PersonInfo) _n, dataset(layouts._lt_PersonInfo) _o) := FUNCTION

    _new := PROJECT (_n, TRANSFORM (layouts._lt_row_PersonInfo, SELF._diff_ord := COUNTER, SELF := LEFT));
    _old := PROJECT (_o, TRANSFORM (layouts._lt_row_PersonInfo, SELF._diff_ord := 10000 + COUNTER, SELF := LEFT));
    ActiveJoin := JOIN (_new, _old,
                  LEFT.Name.Last = RIGHT.Name.Last AND LEFT.Name.First = RIGHT.Name.First,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter_name_firstname_last(LEFT, RIGHT)),
                  FULL OUTER,
                  LIMIT (0));
    PassiveJoin := JOIN (_new, _old,
                  LEFT.Name.Last = RIGHT.Name.Last AND LEFT.Name.First = RIGHT.Name.First,
                  ProcessTxRow (LEFT, RIGHT,
                  CheckOuter_name_firstname_last(LEFT, RIGHT)),
                  LEFT OUTER,
                  LIMIT (0));
    RETURN PROJECT(SORT(IF (is_active, ActiveJoin, PassiveJoin), _diff_ord), layouts._lt_PersonInfo);
  END;

END;

EXPORT _df_OrganizationInfoResponse(boolean is_active, string path) := MODULE
  EXPORT layouts._lt_OrganizationInfoResponse ProcessTx(layouts._lt_OrganizationInfoResponse L, layouts._lt_OrganizationInfoResponse R) :=TRANSFORM


      SELF.OrgName := L.OrgName;

      SELF.Address := L.Address;

      SELF.Members  := _df_PersonInfo(CASE(path + '/Members', '/Members' => (MonitorMembers OR MonitorPeople), is_active), path + '/Members/Member').AsDataset_name_firstname_last(L.Members, R.Members);

      SELF.Guests  := _df_PersonInfo(CASE(path + '/Guests', '/Guests' => (MonitorGuests OR MonitorPeople), is_active), path + '/Guests/Guest').AsDataset_name_firstname_last(L.Guests, R.Guests);
END;

    EXPORT AsRecord (layouts._lt_OrganizationInfoResponse _new, layouts._lt_OrganizationInfoResponse _old) := FUNCTION
      RETURN ROW (ProcessTx(_new, _old));
    END;


END;
END;


  //Defines
  the_differenceModule := difference._df_OrganizationInfoResponse;
  the_requestLayout := request._lt_OrganizationInfoRequest;
  the_responseLayout := layouts._lt_OrganizationInfoResponse;


  //Inputs
  string csndServer := '127.0.0.1' : stored('cassandraServer', FORMAT(SEQUENCE(1)));
  string csndUser := '' : stored('cassandraUser', FORMAT(SEQUENCE(2)));
  string csndPassword := '' : stored('cassandraPassword', FORMAT(PASSWORD, SEQUENCE(3)));

  string csndKeySpaceFrom := 'monitors_a' : stored('fromKeyspace', FORMAT(SEQUENCE(4)));
  string csndKeySpaceTo := 'monitors_a' : stored('toKeyspace', FORMAT(SEQUENCE(4)));

  string monAction := 'Create' : STORED('MonAction', FORMAT(SELECT('Create,Run'), SEQUENCE(5)));
  string userId := '' : stored('UserId', FORMAT(SEQUENCE(6)));
  string serviceURL := '' : stored('QueryURL', FORMAT(SEQUENCE(7)));
  string serviceName := '' : stored('QueryName', FORMAT(SEQUENCE(8)));
  unsigned2 serviceTimeout := 1000 : stored('QueryTimeoutSecs', FORMAT(SEQUENCE(9)));
  unsigned1 serviceRetries := 3 : stored('QueryRetries', FORMAT(SEQUENCE(10)));


  string monitorIdIn := '' : stored('MonitorId', FORMAT(SEQUENCE(9)));

  requestIn := DATASET([], the_requestLayout) : STORED ('OrganizationInfoRequest', FEW, FORMAT(FIELDWIDTH(100),FIELDHEIGHT(30), sequence(100)));

  exceptionRec := RECORD
    string10 Source {xpath('Source')};
    integer2 Code {xpath('Code')};
    string100 Message {xpath('Message')};
  END;

  soapoutRec := record
    dataset (the_responseLayout) ds {xpath('Dataset/Row')};

    exceptionRec Exception {xpath('Exception')};
  end;

MonSoapcall(DATASET(the_requestLayout) req) := FUNCTION


  // When calling roxie the actual request parameters are placed inside a dataset that is named the same as the request
  // so it looks like:
  //   ........

  in_rec := record
    DATASET (the_requestLayout) OrganizationInfoRequest {xpath('OrganizationInfoRequest/Row'), maxcount(1)};
  end;
  in_rec Format () := transform
    Self.OrganizationInfoRequest := req;
  end;

  ds_request := DATASET ([Format()]);



  // execute soapcall
  ar_results := SOAPCALL (ds_request,
                          serviceURL,
                          serviceName,
                          {ds_request},
                          DATASET (soapoutRec),
                          TIMEOUT(serviceTimeout), RETRY(serviceRetries), LITERAL, XPATH('*/Results/Result'));
  RETURN ar_results;
END;

  monitorStoreRec := RECORD
    string MonitorId,
    string result
  END;

// Initialize the Cassandra table, passing in the ECL dataset to provide the rows
// When not using batch mode, maxFutures controls how many simultaenous writes to Cassandra are allowed before
// we start to throttle, and maxRetries controls how many times inserts that fail because Cassandra is too busy
// will be retried.

monitorStoreRec getStoredMonitor(string id) := EMBED(cassandra : server(csndServer), user(csndUser), password(csndPassword), keyspace(csndKeySpaceFrom))
  SELECT monitorId, result from monitor WHERE monitorId=? LIMIT 1;
ENDEMBED;

updateMonitor(dataset(monitorStoreRec) values) := EMBED(cassandra : server(csndServer), user(csndUser), password(csndPassword), keyspace(csndKeySpaceTo), maxFutures(100), maxRetries(10))
  INSERT INTO monitor (monitorId, result) values (?,?);
ENDEMBED;

  MonitorResultRec := RECORD
    string id;

    string responseXML;

    dataset(the_responseLayout) report;
  END;



RunMonitor (string id, dataset(the_requestLayout) req) := MODULE
  SHARED monitorId := id;
  SHARED monitorStore := getStoredMonitor(id);
  SHARED soapOut := MonSoapCall(req)[1];

  SHARED responseRow := soapOut.ds[1];

  SHARED responseXML := '<Row>' + TOXML(responseRow) + '</Row>';

  SHARED oldResponse := FROMXML (the_responseLayout, monitorStore.result);

  SHARED diff_result := the_differenceModule(false, '').AsRecord(responseRow, oldResponse);

  EXPORT MonitorResultRec BuildMonitor() :=TRANSFORM
    SELF.id := IF (soapOut.Exception.Code=0, monitorId, ERROR(soapOut.Exception.Code, soapOut.Exception.Message));
    SELF.responseXML := (string) responseXML;
    SELF.report := diff_result;
  END;
  EXPORT Result () := FUNCTION
    RETURN ROW(BuildMonitor());
  END;
END;

  executedAction := RunMonitor(monitorIdIn, requestIn).Result();
  updateMonitor(DATASET([{executedAction.id, executedAction.responseXML}], monitorStoreRec));


  output(executedAction.id, NAMED('MonitorId'));
  output(executedAction.report, NAMED('Result'));


  SelectorRec := RECORD
    string monitor {xpath('@monitor')};
    boolean active {xpath('@active')};
  END;

    OUTPUT(DATASET([{'Members', difference.MonitorMembers},{'People', difference.MonitorPeople},{'Addresses', difference.MonitorAddresses},{'Guests', difference.MonitorGuests}], SelectorRec), NAMED('Selected'));
  /*** Generated Code do not hand edit ***/
