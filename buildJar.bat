REM compile to tmp
javac -g:none -d ./tmp *.java
cd tmp
REM build jar file
jar -cvfme ../nanodb.jar ../resources/manifest.mf nanodb.NanoDBServer nanodb/*.class  > ../log.txt
cd ..
REM remove tmp
rmdir /s /q tmp
REM start nanodb.jar
javaw -Xms2048m -jar nanodb.jar
