/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

//nothor
//nohthor
import lib_logging; // only needed if lib_logging.Logging.getTraceSpanHeader used below
string TargetIP := '.' : stored('TargetIP');

NameRec := RECORD
  string10 First;
  string15 Last;
END;

AddressRec := RECORD
  string10 City;
  string2 State;
  integer4 ZipCode;
END;

PersonRec := RECORD
  NameRec Name;
  AddressRec Address;
END;

pipe_send := DATASET([{{'Joe', 'Doe'}, {'Fresno', 'CA', 11111}},
{{'Jason', 'Jones'}, {'Tempe', 'AZ', 22222}},
{{'Becky', 'Lopez'}, {'New York', 'NY', 33333}},
{{'Roderic', 'Lykke'}, {'Lubbock', 'TX', 44444}},
{{'Rina', 'Yonkers'}, {'Denver', 'CO', 55555}},
{{'Laverna', 'Campa'}, {'Detroit', 'MI', 66666}},
{{'Shantell', 'Mattera'}, {'Toledo', 'OH', 77777}},
{{'John', 'Smith'}, {'Boise', 'ID', 88888}},
{{'Jane','Smith'}, {'Smallville', 'KY', 99999}}], PersonRec);

/*  NOTES:
	roxiepipe = the executable we're calling
	iw = Input Width
	vip = Virtual IP
	t = # threads
	ow = output width
	b = # records in a batch
	mr = max retries
	h = ??? -- the URL to the Roxie batch service
	r = name of the results dataset (case sensitive!)
  tp = traceparent (trace context) propagatated to roxie
       can be hardcoded or current trace (if available) can be fetched dynamically
       via lib_logging.Logging.getTraceSpanHeader()
	q = query
*/


// This is the remote call to the 'roxie_echo' service.
pipe_recv := PIPE(pipe_send,
    'roxiepipe' +
    ' -iw ' + SIZEOF(PersonRec) +
    ' -t 1' +
    ' -ow ' + SIZEOF(PersonRec) +
    ' -b 3' +
    ' -mr 2' +
    ' -h ' + TargetIP + ':9876 ' +
    ' -r Peeps' +
    ' -tp ' + lib_logging.Logging.getTraceSpanHeader() +  //Fetches current active trace/span context
                                                          //Not currently supported on thor - run on hthor
                                                          //can provide hardcoded traceparent as below
    //' -tp 00-0c483675c6c887d9fb49885c0f2ba116-379563f3fcd56bc9-01 ' + 
    ' -q "<roxie_echo format=\'raw\'><peeps id=\'id\' format=\'raw\'></peeps></roxie_echo>"'
, PersonRec);

OUTPUT(pipe_recv);

