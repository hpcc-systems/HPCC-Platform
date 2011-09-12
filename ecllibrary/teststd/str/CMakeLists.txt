################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    This program is free software: you can redistribute it and/or All rights
#    reserved. This program is NOT PRESENTLY free software: you can NOT
#    redistribute
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################
FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/TestCleanSpaces.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestCompareIgnoreCase.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestContains.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestCountWords.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestEditDistance.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestEditDistanceWithinRadius.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestEqualIgnoreCase.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestExtract.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestFilter.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestFilterOut.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestFind.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestFindCount.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestFindReplace.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestFromHexPairs.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestGetNthWord.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestReverse.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestSplitWords.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestSubstituteExcluded.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestSubstituteIncluded.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestToHexPairs.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestToLowerCase.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestToCapitalCase.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestToTitleCase.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestToUpperCase.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestWildMatch.ecl
    ${CMAKE_CURRENT_SOURCE_DIR}/TestWordCount.ecl
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/share/ecllibrary/teststd/str COMPONENT Runtime )
ENDFOREACH ( iFILES )
