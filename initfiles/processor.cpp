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
#include <string> 
#include <vector>
#include <iostream>
#include <fstream>
#include <cstdio>

using namespace std;

string getFileVector(const char* fileName){
    vector<string> text;
    string line;
    ifstream textstream(fileName);
    while ( getline(textstream, line) ){
        text.push_back(line + "\n");
    } 
    textstream.close();

    string alltext;
    for (unsigned int i=0; i < text.size(); i++)
        alltext += text[i];

    return alltext;
}


int main(int argc, char* argv[])
{

    if (argc != 4) {
        fprintf(stderr, "Usage: %s <bash-vars> <input> <output>\n", argv[0]);
        return 1;
    }

    string bashVars = getFileVector(argv[1]);
    string inFileContents = getFileVector(argv[2]);
    
    string searchString( "###<REPLACE>###" );
    string replaceString( bashVars );
    string::size_type pos = 0;
    while ( (pos = inFileContents.find(searchString, pos)) != string::npos ) 
    {
        inFileContents.replace( pos, searchString.size(), replaceString );
        pos++;
    }
    ofstream outstream(argv[3]);
    outstream<<inFileContents;
    outstream.close();
    return 0;
}
