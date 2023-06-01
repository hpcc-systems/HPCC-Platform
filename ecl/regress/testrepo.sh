# Script file to testing different variants of submitting queries
ROOTPATH=/opt/HPCCSystems/bin
ECLCC=$ROOTPATH/eclcc
ECL=$ROOTPATH/ecl

echo ---------- Standard eclcc compiles ----------
${ECLCC} --main demo.main@ghalliday/gch-ecldemo-d#version3
./a.out

${ECLCC} demomain.ecl --main @ghalliday/gch-ecldemo-d#version3
./a.out

${ECLCC} demomain.ecl --main demo.main@ghalliday/gch-ecldemo-d#version3
./a.out

cat demomain.ecl | ${ECLCC} - --main demo.main@ghalliday/gch-ecldemo-d#version3
./a.out

echo ---------- eclcc using --defaultrepo and --defaultrepoversion ----------

${ECLCC} --main demo.main@ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out
${ECLCC} --main demo.main#version3 --defaultrepo=ghalliday/gch-ecldemo-d
./a.out
${ECLCC} --main demo.main --defaultrepo=ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out

${ECLCC} demomain.ecl --main @ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out
${ECLCC} demomain.ecl --main "#version3" --defaultrepo=ghalliday/gch-ecldemo-d
./a.out
${ECLCC} demomain.ecl --defaultrepo=ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out

${ECLCC} demomain.ecl --main demo.main@ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out
${ECLCC} demomain.ecl --main demo.main#version3 --defaultrepo=ghalliday/gch-ecldemo-d
./a.out
${ECLCC} demomain.ecl --main demo.main --defaultrepo=ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out

cat demomain.ecl | ${ECLCC} - --main demo.main@ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out
cat demomain.ecl | ${ECLCC} - --main demo.main#version3 --defaultrepo=ghalliday/gch-ecldemo-d
./a.out
cat demomain.ecl | ${ECLCC} - --main demo.main --defaultrepo=ghalliday/gch-ecldemo-d --defaultrepoversion=version3
./a.out

echo ---------- check precedence ----------

${ECLCC} demomain.ecl --main demo.main --defaultrepo=ghalliday/gch-ecldemo-d#versionXXX --defaultrepoversion=version3
./a.out

${ECLCC} demomain.ecl --main demo.main@ghalliday/gch-ecldemo-d#version3 --defaultrepoversion=versionXXX
./a.out

${ECLCC} demomain.ecl --main demo.main@ghalliday/gch-ecldemo-d#versionXXX --mainrepoversion=version3
./a.out

echo ---------- eclcc using --mainrepo and --mainrepoversion ----------

${ECLCC} --main demo.main@ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out
${ECLCC} --main demo.main#version3 --mainrepo=ghalliday/gch-ecldemo-d
./a.out
${ECLCC} --main demo.main --mainrepo=ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out

${ECLCC} demomain.ecl --main @ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out
${ECLCC} demomain.ecl --main "#version3" --mainrepo=ghalliday/gch-ecldemo-d
./a.out

${ECLCC} demomain.ecl --main demo.main@ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out
${ECLCC} demomain.ecl --main demo.main#version3 --mainrepo=ghalliday/gch-ecldemo-d
./a.out
${ECLCC} demomain.ecl --main demo.main --mainrepo=ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out

cat demomain.ecl | ${ECLCC} - --main demo.main@ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out
cat demomain.ecl | ${ECLCC} - --main demo.main#version3 --mainrepo=ghalliday/gch-ecldemo-d
./a.out
cat demomain.ecl | ${ECLCC} - --main demo.main --mainrepo=ghalliday/gch-ecldemo-d --mainrepoversion=version3
./a.out

echo ---------- Run using ecl ----------

${ECL} run hthor --server=. --ecl-only --main demo.main@ghalliday/gch-ecldemo-d -feclcc--defaultrepoversion=version3
${ECL} run hthor --server=. --ecl-only --main demo.main#version3 -feclcc--defaultrepo=ghalliday/gch-ecldemo-d
${ECL} run hthor --server=. --ecl-only --main demo.main -feclcc--defaultrepo=ghalliday/gch-ecldemo-d -feclcc--defaultrepoversion=version3
${ECL} run hthor --server=. --ecl-only demomain.ecl --main @ghalliday/gch-ecldemo-d -feclcc--defaultrepoversion=version3
${ECL} run hthor --server=. --ecl-only demomain.ecl --main "#version3" -feclcc--defaultrepo=ghalliday/gch-ecldemo-d
${ECL} run hthor --server=. --ecl-only demomain.ecl -feclcc--defaultrepo=ghalliday/gch-ecldemo-d -feclcc--defaultrepoversion=version3
${ECL} run hthor --server=. --ecl-only demomain.ecl --main demo.main@ghalliday/gch-ecldemo-d -feclcc--defaultrepoversion=version3
${ECL} run hthor --server=. --ecl-only demomain.ecl --main demo.main#version3 -feclcc--defaultrepo=ghalliday/gch-ecldemo-d
${ECL} run hthor --server=. --ecl-only demomain.ecl --main demo.main -feclcc--defaultrepo=ghalliday/gch-ecldemo-d -feclcc--defaultrepoversion=version3
