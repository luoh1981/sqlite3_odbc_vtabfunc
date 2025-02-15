# sqlite3_odbc_vtabfunc
SQLite3 virtual table and custom function access to ODBC data source

# Usage
.load vtabodbc.dll sqlite3_odbc_init
CREATE VIRTUAL TABLE testodbc USING odbc('Driver={SQL Server};Server=127.0.0.1;Database=AdventureWorks;Uid=sa;Pwd=xxxx;',
'SELECT * FROM [AdventureWorks].[Person].[Person] where [PersonType] like ?  and [LastName] like ? and 1=?');
.header on
select [Title],[FirstName],[MiddleName],[LastName] from testodbc where odbccondition = ' and Title like ''Ms%''' AND odbcparameter1='EM%' and odbcparameter2='G%' and odbcparameter3=1;

.load vtabodbc.dll sqlite3_odbc_init
select odbc_connect('Driver={SQL Server};Server=127.0.0.1;Database=AdventureWorks;Uid=sa;Pwd=xxxx;');
select odbc_execute(6859994838057138823,'update [AdventureWorks].[Person].[Person] SET Title=''Mr.'' WHERE rowguid=''92C4279F-1207-48A3-8448-4636514EB7E2''');
select odbc_execute(6859994838057138823,'update [AdventureWorks].[Person].[Person] SET Title=? WHERE rowguid=''92C4279F-1207-48A3-8448-4636514EB7E2''','Mr.');
select odbc_execute(6859994838057138823,'update [AdventureWorks].[Person].[Person] SET Title=? WHERE rowguid=''92C4279F-1207-48A3-8448-4636514EB7E2''',NULL);
select odbc_disconnect(6859994838057138823);

# dependencies
uthash
xxHash

# build
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
cl /nologo /c /O2 /W3 /D_CRT_SECURE_NO_DEPRECATE /DSQLITE_API=__declspec(dllexport)  -I"sqlite3 folder"  odbcvirtualtab.c
link /nologo /DLL  ODBC32.LIB sqlite3.lib odbcvirtualtab.obj /out:vtabodbc.dll