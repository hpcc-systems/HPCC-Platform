/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    for (int i=0; i < text.size(); i++)
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
