# sqlite3_odbc_vtabfunc
SQLite3 virtual table and custom function access to ODBC data source

Connect to the ODBC data source and execute the query to return result as the virtual table,execute update and delete with parameters.
Virtual table with hide columns "odbccondition" and "odbcparameter1..odbcparameterN",the meaning and usage are as follows.

Inspired by the *[Some troubles with ODBC (Win) virtual table](https://www.sqlite.org/forum/forumpost/b499e01e3aeccb89b8484d4ec399796cc8beea47695e54b20a679ccba73e5004)*

# Usage
```SQL
.load vtabodbc.dll sqlite3_odbc_init
CREATE VIRTUAL TABLE testodbc USING odbc('Driver={SQL Server};Server=127.0.0.1;Database=AdventureWorks;Uid=sa;Pwd=xxxx;',
'SELECT * FROM [AdventureWorks].[Person].[Person] where [PersonType] like ?  and [LastName] like ? and 1=?');
.header on
.mode table
select [Title],[FirstName],[MiddleName],[LastName] from testodbc where odbccondition = ' and Title like ''Ms%''' AND odbcparameter1='EM%' and odbcparameter2='G%' and odbcparameter3=1;
+-------+-----------+------------+----------+
| Title | FirstName | MiddleName | LastName |
+-------+-----------+------------+----------+
| Ms.   | Janice    | M          | Galvin   |
+-------+-----------+------------+----------+

select odbc_connect('Driver={SQL Server};Server=127.0.0.1;Database=AdventureWorks;Uid=sa;Pwd=xxxx;') connection_hash; 
+---------------------+
|   connection_hash   |
+---------------------+
| 6859994838057138823 |
+---------------------+

select odbc_execute(6859994838057138823,'update [AdventureWorks].[Person].[Person] SET Title=''Mr.'' WHERE rowguid=''92C4279F-1207-48A3-8448-4636514EB7E2''');
+--------------------------------------------------------------+
| odbc_execute(6859994838057138823,'update [AdventureWorks].[P |
+--------------------------------------------------------------+
| 1                                                            |
+--------------------------------------------------------------+

select odbc_execute(6859994838057138823,'update [AdventureWorks].[Person].[Person] SET Title=? WHERE rowguid=''92C4279F-1207-48A3-8448-4636514EB7E2''','Mr.');
+--------------------------------------------------------------+
| odbc_execute(6859994838057138823,'update [AdventureWorks].[P |
+--------------------------------------------------------------+
| 1                                                            |
+--------------------------------------------------------------+

select odbc_execute(6859994838057138823,'update [AdventureWorks].[Person].[Person] SET Title=? WHERE rowguid=''92C4279F-1207-48A3-8448-4636514EB7E2''',NULL);
+--------------------------------------------------------------+
| odbc_execute(6859994838057138823,'update [AdventureWorks].[P |
+--------------------------------------------------------------+
| 1                                                            |
+--------------------------------------------------------------+

select odbc_disconnect(6859994838057138823);
+--------------------------------------+
| odbc_disconnect(6859994838057138823) |
+--------------------------------------+
| 1                                    |
+--------------------------------------+
```
# dependencies
uthash<br>
xxHash

# build
```bat
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
cl /nologo /c /O2 /W3 /D_CRT_SECURE_NO_DEPRECATE /DSQLITE_API=__declspec(dllexport)  -I"sqlite3 folder"  odbcvirtualtab.c
link /nologo /DLL  ODBC32.LIB sqlite3.lib odbcvirtualtab.obj /out:vtabodbc.dll
```