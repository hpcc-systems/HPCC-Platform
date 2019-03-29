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


pointRec := { real8 x, real8 y };

shapeRecord :=
            RECORD
unsigned        id;
pointRec        corners[4];
unsigned        extra := 10;
            END;


mkSquare(real8 x, real8 y, real8 w) := dataset([{x,y},{x+w,y},{x+w,y+w},{x,y+w}], pointRec);
mkRectangle(real8 x, real8 y, real8 w, real8 h) := dataset([{x,y},{x+w,y},{x+w,y+h},{x,y+h}], pointRec);

shapesDs := DATASET([
                {1, mkSquare(-1,-1,2)},
                {2, mkSquare(0,4,2)},
                {3, mkSquare(1,3,12)},
                {4, mkRectangle(0,0,1000,0.001)},
                {5, mkRectangle(1,2,3,4)},
                {0, mkSquare(0,0,0)}
                ], shapeRecord)(id != 0);

spilledDs := global(nofold(shapesDs));

projDs := PROJECT(spilledDs, shapeRecord - [extra]);

sequential(
    output(shapesDs,          { name := 'Area of ' + (string)id + ' = ', area := (corners[3].x-corners[1].x) * (corners[3].y-corners[1].y) });
    output(nofold(spilledDs), { name := 'Area of ' + (string)id + ' = ', area := (corners[3].x-corners[1].x) * (corners[3].y-corners[1].y) });
    output(nofold(projDs),    { name := 'Area of ' + (string)id + ' = ', area := (corners[3].x-corners[1].x) * (corners[3].y-corners[1].y) });
);