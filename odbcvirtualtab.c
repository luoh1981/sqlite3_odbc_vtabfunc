#define UNICODE
#define _UNICODE

#define MAX_DATA_LENGTH 32000

#include <windows.h>
#include <stdio.h>
#include <tchar.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>

#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>
#include "..\uthash\src\uthash.h"
#define XXH_STATIC_LINKING_ONLY /* access advanced declarations */
#define XXH_IMPLEMENTATION      /* access definitions */
#include "..\xxHash\xxhash.h"
#undef XXH_STATIC_LINKING_ONLY
#undef XXH_IMPLEMENTATION
struct clientConnectionhandle {
	XXH64_hash_t DSNName;
	SQLHANDLE hConn;
	int refcnt;
	UT_hash_handle hh;
};
struct clientDataHandles {
	SQLHANDLE hEnv;
	int refcnt;
	struct clientConnectionhandle* cCh;
};
typedef struct odbc_vtab odbc_vtab;
struct odbc_vtab {
	sqlite3_vtab base;
	SQLHANDLE hEnv;
	SQLHANDLE hConn;
	TCHAR* query;
	char* coltypes;
	SQLSMALLINT colCount;
	SQLSMALLINT paramCount;
	struct clientDataHandles* pHandles;
	struct clientConnectionhandle* pconH;
};

typedef struct odbc_cursor odbc_cursor;
struct odbc_cursor {
	sqlite3_vtab_cursor base;
	SQLHANDLE hConn;
	TCHAR* query;
	char* coltypes;
	SQLSMALLINT colCount;
	SQLSMALLINT paramCount;
	sqlite3_int64 rowId;
	SQLHANDLE hStmt;
	BOOL isEof;
};

char* utf16to8(const TCHAR* in) {
	char* out;
	if (!in || _tcslen(in) == 0) {
		out = (char*)calloc(1, sizeof(char));
	}
	else {
		int len = WideCharToMultiByte(CP_UTF8, 0, in, -1, NULL, 0, 0, 0);
		out = (char*)calloc(len, sizeof(char));
		WideCharToMultiByte(CP_UTF8, 0, in, -1, out, len, 0, 0);
	}
	return out;
}

TCHAR* utf8to16(const char* in) {
	TCHAR* out;
	if (!in || strlen(in) == 0) {
		out = (TCHAR*)calloc(1, sizeof(TCHAR));
	}
	else {
		DWORD size = MultiByteToWideChar(CP_UTF8, 0, in, -1, NULL, 0);
		out = (TCHAR*)calloc(size, sizeof(TCHAR));
		MultiByteToWideChar(CP_UTF8, 0, in, -1, out, size);
	}
	return out;
}

static void* OdbcErrorMsg(SQLHANDLE      hHandle,
	SQLSMALLINT    hType,
	RETCODE        RetCode)
{
	SQLSMALLINT iRec = 0;
	SQLINTEGER  iError;
	WCHAR       wszMessage[1000];
	WCHAR       wszState[SQL_SQLSTATE_SIZE + 1];

	if (RetCode == SQL_INVALID_HANDLE)
	{
		return utf16to8(L"ODBC Invalid handle!\n");
	}

	while (SQLGetDiagRec(hType,
		hHandle,
		++iRec,
		wszState,
		&iError,
		wszMessage,
		(SQLSMALLINT)(sizeof(wszMessage) / sizeof(WCHAR)),
		(SQLSMALLINT*)NULL) == SQL_SUCCESS)
	{
		if (wcsncmp(wszState, L"01004", 5))
		{
			return utf16to8(wszMessage);
		}
	}
	return utf16to8("");
}
static void* sqlite_OdbcErrorMsg(SQLHANDLE      hHandle,
	SQLSMALLINT    hType,
	RETCODE        RetCode)
{
	const char* emsg = NULL;
	const char* pzErr = NULL;
	emsg = OdbcErrorMsg(hHandle, hType, RetCode);
	pzErr = sqlite3_mprintf(emsg);
	free(emsg);
	return pzErr;
}
static int odbcCreate(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** ppVtab, char** pzErr) {
	int rc = SQLITE_OK;
	SQLHANDLE hEnv = NULL;
	SQLHANDLE hConn = NULL;
	struct clientConnectionhandle* pconH = NULL;
	TCHAR* query = NULL;
	SQLHANDLE hStmt = 0;
	char* coltypes = NULL;
	odbc_vtab* pTab;
	TCHAR quotes[] = TEXT("\"'`[");
	if (argc != 5) {
		*pzErr = sqlite3_mprintf("odbc-module requires two argument: DSN and query to source");
		goto CREATE_ERROR;
	}
	struct clientDataHandles* pHandles = (struct clientDataHandles*)pAux;
	if (pHandles->hEnv == NULL) {
		rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &pHandles->hEnv);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			*pzErr = sqlite3_mprintf("Can't get access to ODBC");
			pHandles->hEnv = NULL;
			goto CREATE_ERROR;
		}
		rc = SQLSetEnvAttr(pHandles->hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			SQLFreeHandle(SQL_HANDLE_ENV, pHandles->hEnv);
			pHandles->hEnv = NULL;
			goto CREATE_ERROR;
		}
		pHandles->refcnt++;
	}
	else {
		pHandles->refcnt++;
	}
	hEnv = pHandles->hEnv;
	int dsnlen = strlen(argv[3]);
	char* s2 = &((argv[3])[1]);
	XXH64_hash_t DSNName = 0;
	if (strchr("\"'`[", *s2))
		DSNName = XXH64(s2, dsnlen - 2, 0);
	else
		DSNName = XXH64(argv[3], dsnlen, 0);
	HASH_FIND(hh, pHandles->cCh, &DSNName, sizeof(XXH64_hash_t), pconH);
	if (!pconH) {
		SQLHANDLE llhConn = NULL;
		rc = SQLAllocHandle(SQL_HANDLE_DBC, pHandles->hEnv, &llhConn);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			*pzErr = sqlite_OdbcErrorMsg(pHandles->hEnv, SQL_HANDLE_ENV, rc);
			goto CREATE_ERROR;
		}
#define ODBCCHECKCATTRRC(rc) if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) goto ENDSETATTR;
		rc = SQLSetConnectAttr(llhConn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)5, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_WRITE, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_COMMITTED, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)TRUE, 0);
#undef ODBCCHECKCATTRRC
		ENDSETATTR :
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			*pzErr = sqlite_OdbcErrorMsg(llhConn, SQL_HANDLE_DBC, rc);
			SQLFreeHandle(SQL_HANDLE_DBC, llhConn);
			llhConn = NULL;
			goto CREATE_ERROR;
		}
		TCHAR* dsn = utf8to16(argv[3]);
		if (!_tcschr(quotes, dsn)) {
			dsn[0] = TEXT(' ');
			dsn[_tcslen(dsn) - 1] = TEXT(' ');
		}
		rc = SQLDriverConnect(llhConn, NULL, dsn, _tcslen(dsn), SQL_NTS, 0, NULL, SQL_DRIVER_NOPROMPT);
		free(dsn);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			*pzErr = sqlite_OdbcErrorMsg(llhConn, SQL_HANDLE_DBC, rc);
			SQLFreeHandle(SQL_HANDLE_DBC, llhConn);
			goto CREATE_ERROR;
		}
		hConn = llhConn;
		pconH = (struct clientConnectionhandle*)sqlite3_malloc(sizeof(struct clientConnectionhandle));
		if (!pconH) {
			*pzErr = sqlite3_mprintf("sqlite3_malloc failure.");
			goto CREATE_ERROR;
		}
		memset(pconH, 0, sizeof(struct clientConnectionhandle));
		pconH->DSNName = DSNName;
		pconH->hConn = hConn;
		pconH->refcnt += 1;
		HASH_ADD(hh, pHandles->cCh, DSNName, sizeof(XXH64_hash_t), pconH);
	}
	else {
		hConn = pconH->hConn;
		pconH->refcnt += 1;
	}

	query = utf8to16(argv[4]);
	if (!_tcschr(quotes, query)) {
		query[0] = TEXT(' ');
		query[_tcslen(query) - 1] = TEXT(' ');
	}

	rc = SQLAllocHandle(SQL_HANDLE_STMT, hConn, &hStmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		*pzErr = sqlite_OdbcErrorMsg(hConn, SQL_HANDLE_DBC, rc);
		goto CREATE_ERROR;
	}
	rc = SQLPrepare(hStmt, query, SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		*pzErr = sqlite_OdbcErrorMsg(hStmt, SQL_HANDLE_STMT, rc);
		goto CREATE_ERROR;
	}
	SQLSMALLINT paramcnt = 0;
	rc = SQLNumParams(hStmt, &paramcnt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		*pzErr = sqlite_OdbcErrorMsg(hStmt, SQL_HANDLE_STMT, rc);
		goto CREATE_ERROR;
	}
	if (paramcnt > 63) {
		*pzErr = sqlite3_mprintf("SQL Number of Params lagre then 63");
		goto CREATE_ERROR;
	}
	if (paramcnt != 0) {
		for (int pidx = 1; pidx <= paramcnt; pidx++) {
			SQLSMALLINT paramDataType;
			SQLULEN paramColumnSize;
			SQLSMALLINT paramDecimalDigits;
			SQLSMALLINT paramNullable;
			rc = SQLDescribeParam(hStmt, pidx, &paramDataType, &paramColumnSize, &paramDecimalDigits, &paramNullable);
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
				*pzErr = sqlite_OdbcErrorMsg(hStmt, SQL_HANDLE_STMT, rc);
				goto CREATE_ERROR;
			}
			__int64 pbin = 0;double pdoub = 0;
			switch (paramDataType) {
			case SQL_CHAR:
			case SQL_VARCHAR:
			case SQL_LONGVARCHAR:
				rc = SQLBindParameter(hStmt, (unsigned short)pidx, SQL_PARAM_INPUT, SQL_C_TCHAR, SQL_VARCHAR, 4000, 0, TEXT(""), 0, NULL);
				break;
			case SQL_WCHAR:
			case SQL_WVARCHAR:
			case SQL_WLONGVARCHAR:
				rc = SQLBindParameter(hStmt, (unsigned short)pidx, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 4000, 0, TEXT(""), 0, NULL);
				break;
			case SQL_NUMERIC:
				rc = SQLBindParameter(hStmt, (unsigned short)pidx, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_NUMERIC, 10, 0, TEXT("0"), 0, NULL);
				break;
			case SQL_FLOAT:
			case SQL_DOUBLE:
				rc = SQLBindParameter(hStmt, (unsigned short)pidx, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &pdoub, 0, NULL);
				break;
			case SQL_SMALLINT:
			case SQL_INTEGER:
			case SQL_BIT:
			case SQL_TINYINT:
			case SQL_BIGINT:
				rc = SQLBindParameter(hStmt, (unsigned short)pidx, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &pbin, 0, NULL);
				break;
			}
			if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
				*pzErr = sqlite_OdbcErrorMsg(hStmt, SQL_HANDLE_STMT, rc);
				goto CREATE_ERROR;
			}
		}
	}
	rc = SQLExecute(hStmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		*pzErr = sqlite_OdbcErrorMsg(hStmt, SQL_HANDLE_STMT, rc);
		goto CREATE_ERROR;
	}
	TCHAR createQuery16[4000] = TEXT("create table x(\"");
	SQLSMALLINT colCount = 0;
	SQLNumResultCols(hStmt, &colCount);
	coltypes = sqlite3_malloc(sizeof(char) * (colCount + 1));
	memset(coltypes, 0, sizeof(char) * (colCount + 1));
	for (int colNo = 1; colNo <= colCount; colNo++) {
		TCHAR colName[512] = { 0 };
		SQLSMALLINT colType = 0;
		SQLDescribeCol(hStmt, colNo, colName, 511, 0, &colType, 0, 0, 0);
		_tcscat(createQuery16, colName);

		if (colType == SQL_REAL || colType == SQL_FLOAT || colType == SQL_DOUBLE)
		{
			_tcscat(createQuery16, TEXT("\" real"));
			coltypes[colNo] = 'R';
		}
		else if (colType == SQL_SMALLINT || colType == SQL_INTEGER || colType == SQL_BIT || colType == SQL_TINYINT || colType == SQL_BIGINT)
		{
			_tcscat(createQuery16, TEXT("\" integer"));
			coltypes[colNo] = 'I';
		}
		else if (colType == SQL_DECIMAL || colType == SQL_NUMERIC)
		{
			_tcscat(createQuery16, TEXT("\" text"));
			coltypes[colNo] = 'T';
		}
		else if (colType == SQL_CHAR || colType == SQL_VARCHAR || colType == SQL_LONGVARCHAR || colType == SQL_WCHAR || colType == SQL_WVARCHAR || colType == SQL_WLONGVARCHAR)
		{
			_tcscat(createQuery16, TEXT("\" text"));
			coltypes[colNo] = 'T';
		}
		else if (colType == SQL_BINARY || colType == SQL_VARBINARY)
		{
			_tcscat(createQuery16, TEXT("\" blob"));
			coltypes[colNo] = 'B';
		}
		else
		{
			_tcscat(createQuery16, TEXT("\""));
			coltypes[colNo] = 'T';
		}

		if (colNo != colCount)
			_tcscat(createQuery16, TEXT(",\""));
	}
	if (paramcnt != 0) {
		wchar_t vOut[12];
		for (int i = 1; i <= paramcnt; i += 1) {
			_tcscat(createQuery16, TEXT(",odbcparameter"));
			_itow_s(i, vOut, sizeof(vOut) / 2, 10);
			_tcscat(createQuery16, vOut);
			_tcscat(createQuery16, TEXT(" hidden"));
		}
	}
	_tcscat(createQuery16, TEXT(",odbccondition hidden "));
	_tcscat(createQuery16, TEXT(")"));
	SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
	hStmt = NULL;
	char* createQuery8 = utf16to8(createQuery16);
	rc = sqlite3_declare_vtab(db, createQuery8);
	free(createQuery8);
	if (rc == SQLITE_OK) {
		pTab = sqlite3_malloc(sizeof(*pTab));
		*ppVtab = (sqlite3_vtab*)pTab;
		if (pTab == 0) {
			rc = SQLITE_NOMEM;
			goto CREATE_ERROR;
		}
		memset(pTab, 0, sizeof(*pTab));
		pTab->query = query;
		pTab->hEnv = hEnv;
		pTab->hConn = hConn;
		pTab->coltypes = coltypes;
		pTab->colCount = colCount;
		pTab->paramCount = paramcnt;
		pTab->pHandles = pHandles;
		pTab->pconH = pconH;
		return rc;

	}
CREATE_ERROR:
	if (hEnv) {
		pHandles->refcnt -= 1;
		if (pHandles->refcnt == 0) {
			SQLFreeHandle(SQL_HANDLE_ENV, pHandles->hEnv);
			pHandles->hEnv = NULL;
		}
	}
	if (hConn) {
		if (!pconH) {
			SQLFreeHandle(SQL_HANDLE_DBC, hConn);
		}
		else {
			pconH->refcnt -= 1;
			if (pconH->refcnt == 0) {
				SQLFreeHandle(SQL_HANDLE_DBC, pconH->hConn);
				pconH->hConn = NULL;
				HASH_DEL(pHandles->cCh, pconH);
				sqlite3_free(pconH);
			}
		}
	}
	if (hStmt) {
		SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
	}
	free(query);
	sqlite3_free(coltypes);

	return SQLITE_ERROR;
}
static int odbcConnect(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** ppVtab, char** pzErr) {
	return odbcCreate(db, pAux, argc, argv, ppVtab, pzErr);
}

static int odbcDestroy(sqlite3_vtab* pVtab) {
	odbc_vtab* pTab = (odbc_vtab*)pVtab;
	pTab->pconH->refcnt -= 1;
	if (pTab->pconH->refcnt == 0) {
		SQLDisconnect(pTab->pconH->hConn);
		SQLFreeHandle(SQL_HANDLE_DBC, pTab->pconH->hConn);
		pTab->pconH->hConn = NULL;
		HASH_DEL(pTab->pHandles->cCh, pTab->pconH);
		sqlite3_free(pTab->pconH);
	}
	pTab->pHandles->refcnt--;
	if (pTab->pHandles->refcnt == 0) {
		SQLFreeHandle(SQL_HANDLE_ENV, pTab->pHandles->hEnv);
		pTab->pHandles->hEnv = NULL;
	}
	free(pTab->query);
	sqlite3_free(pTab->coltypes);
	sqlite3_free(pTab);

	return SQLITE_OK;
}

static int odbcBestIndex(sqlite3_vtab* tab, sqlite3_index_info* pIdxInfo) {
	int colCount = ((odbc_vtab*)tab)->colCount;
	pIdxInfo->estimatedCost = (double)10000;
	pIdxInfo->estimatedRows = 10000;
	sqlite3_index_info* info = pIdxInfo;
	sqlite_int64 colMask = 0;
	int found = 0;
	for (int i = 0; i != info->nConstraint; ++i)
	{
		if ((info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ) &&
			info->aConstraint[i].usable != 0 &&
			info->aConstraint[i].iColumn >= colCount)
		{
			sqlite_int64 col = 1;
			int coli = info->aConstraint[i].iColumn - colCount;
			col <<= coli;
			if (colMask & col) {
				return SQLITE_CONSTRAINT;
			}
			colMask |= col;
			found++;
			if (info->aConstraint[i].iColumn == colCount + ((odbc_vtab*)tab)->paramCount) {
				info->idxNum = 1;
			}
			info->aConstraintUsage[i].argvIndex = info->aConstraint[i].iColumn - colCount + 1;
			info->aConstraintUsage[i].omit = 1;
		}
	}
	if (((odbc_vtab*)tab)->paramCount + info->idxNum != found) {
		return SQLITE_CONSTRAINT;
	}
	if (found > 0) {
		pIdxInfo->estimatedCost = (double)100;
		pIdxInfo->estimatedRows = 100;
	}
	return SQLITE_OK;
}

static int odbcOpen(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor) {
	odbc_vtab* pTab = (odbc_vtab*)pVtab;
	odbc_cursor* pCur;

	pCur = sqlite3_malloc(sizeof(*pCur));
	if (pCur == 0)
		return SQLITE_NOMEM;
	memset(pCur, 0, sizeof(*pCur));
	*ppCursor = pCur;
	pCur->hConn = pTab->hConn;
	pCur->query = pTab->query;
	pCur->coltypes = pTab->coltypes;
	pCur->paramCount = pTab->paramCount;
	pCur->colCount = pTab->colCount;
	return SQLITE_OK;
}

static int odbcFilter(sqlite3_vtab_cursor* cur, int idxNum, const char* idxStr, int argc, sqlite3_value** argv) {
	odbc_cursor* pCur = (odbc_cursor*)cur;
	if (pCur->paramCount + idxNum != argc) { return SQLITE_ERROR; }
	if (pCur->hStmt) {
		SQLFreeHandle(SQL_HANDLE_STMT, pCur->hStmt);
		pCur->hStmt = NULL;
		pCur->rowId = 0;
		pCur->isEof = 0;
	}
	int rc = SQLAllocHandle(SQL_HANDLE_STMT, pCur->hConn, &(pCur->hStmt));
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		return SQLITE_ERROR;
	SQLSetStmtAttr(pCur->hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_KEYSET_DRIVEN, 0);
	if (argc == 0) {
		rc = SQLExecDirect(pCur->hStmt, (SQLWCHAR*)pCur->query, SQL_NTS);
	}
	else {
		if (idxNum == 1) {
			const char* wherestr8 = (const char*)sqlite3_value_text(argv[pCur->paramCount]);
			TCHAR* wcharTempwherestr = utf8to16(wherestr8);
			TCHAR wcharTempSQL[4000] = TEXT("");
			_tcscat(wcharTempSQL, (SQLWCHAR*)pCur->query);
			_tcscat(wcharTempSQL, wcharTempwherestr);
			free(wcharTempwherestr);
			rc = SQLPrepare(pCur->hStmt, wcharTempSQL, SQL_NTS);
		}
		else {
			rc = SQLPrepare(pCur->hStmt, (SQLWCHAR*)pCur->query, SQL_NTS);
		}
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			return SQLITE_ERROR;
		TCHAR* wcharPtr = NULL;
		if (pCur->paramCount >= 1) {
			int argcidx;
			TCHAR* wcharPtr = NULL;
			SQLINTEGER cbNumeric = SQL_NULL_DATA;
			for (argcidx = 0; argcidx < argc; argcidx += 1) {
				unsigned short col = argcidx + 1;
				switch (sqlite3_value_type(argv[argcidx])) {
				case SQLITE_INTEGER:
				{
					sqlite3_int64 lint64 = sqlite3_value_int64(argv[argcidx]);
					rc = SQLBindParameter(pCur->hStmt, col, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_NUMERIC, 8, 0, &lint64, sizeof(sqlite3_int64), NULL);
					break;
				}
				case SQLITE_FLOAT:
				{
					double dv = sqlite3_value_double(argv[argcidx]);
					rc = SQLBindParameter(pCur->hStmt, col, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 8, 0, &dv, sizeof(double), NULL);
					break;
				}
				case SQLITE_NULL: {
					rc = SQLBindParameter(pCur->hStmt, col, SQL_PARAM_INPUT, SQL_C_TCHAR, SQL_VARCHAR, 8, 0, NULL, 1, &cbNumeric);
					break;
				}
				case SQLITE_BLOB: {
					void* lbin = sqlite3_value_blob(argv[argcidx]);
					int llen = sqlite3_value_bytes(argv[argcidx]);
					rc = SQLBindParameter(pCur->hStmt, col, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, llen, 0, lbin, llen, &llen);
					break;
				}
				case SQLITE_TEXT: {
					wcharPtr = (const TCHAR*)sqlite3_value_text16(argv[argcidx]);
					rc = SQLBindParameter(pCur->hStmt, col, SQL_PARAM_INPUT, SQL_C_TCHAR, SQL_VARCHAR, 4000, 0, &wcharPtr[0], 0, NULL);
					break;
				}
				}
				if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
					return SQLITE_ERROR;
			}
		}
		rc = SQLExecute(pCur->hStmt);
		free(wcharPtr);
	}
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		if (cur->pVtab->zErrMsg) {
			sqlite3_free(cur->pVtab->zErrMsg);
			cur->pVtab->zErrMsg = NULL;
		}
		cur->pVtab->zErrMsg = sqlite_OdbcErrorMsg(pCur->hStmt, SQL_HANDLE_STMT, rc);
		return SQLITE_ERROR;
	}
	odbcNext(cur);
	return SQLITE_OK;
}

static int odbcColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int colNo) {
	odbc_cursor* pCur = (odbc_cursor*)cur;
	SQLWCHAR val16[MAX_DATA_LENGTH + 1] = { 0 };
	SQLLEN res = 0;
	sqlite_int64 val = 0;
	double valdouble = 0;
	colNo++;
	char ctype = pCur->coltypes[colNo];
	SQLSetPos(pCur->hStmt, 1, SQL_POSITION, SQL_LOCK_NO_CHANGE);
	switch (ctype)
	{
	case 'T':
		SQLGetData(pCur->hStmt, colNo, SQL_WCHAR, val16, MAX_DATA_LENGTH * sizeof(TCHAR), &res);
		char* val8 = utf16to8(val16);
		if (res != SQL_NULL_DATA)
			sqlite3_result_text(ctx, val8, strlen(val8), SQLITE_TRANSIENT);
		else
			sqlite3_result_null(ctx);
		free(val8);
		break;
	case 'I':
		SQLGetData(pCur->hStmt, colNo, SQL_C_SBIGINT, &val, sizeof(sqlite_int64), &res);//SQL_C_SLONG
		if (res != SQL_NULL_DATA)
			sqlite3_result_int64(ctx, val);
		else
			sqlite3_result_null(ctx);
		break;
	case 'R':
		SQLGetData(pCur->hStmt, colNo, SQL_C_DOUBLE, &valdouble, sizeof(double), &res);
		if (res != SQL_NULL_DATA)
			sqlite3_result_double(ctx, valdouble);
		else
			sqlite3_result_null(ctx);
		break;
	case 'B':
		SQLGetData(pCur->hStmt, colNo, SQL_C_BINARY, val16, MAX_DATA_LENGTH * sizeof(TCHAR), &res);
		if (res != SQL_NULL_DATA)
			sqlite3_result_blob(ctx, val16, res, SQLITE_TRANSIENT);
		else
			sqlite3_result_null(ctx);
		break;
	}
	return SQLITE_OK;
}

static int odbcRowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid) {
	odbc_cursor* pCur = (odbc_cursor*)cur;
	*pRowid = pCur->rowId;
	return SQLITE_OK;
}

static int odbcNext(sqlite3_vtab_cursor* cur) {
	odbc_cursor* pCur = (odbc_cursor*)cur;
	pCur->isEof = pCur->isEof || SQLFetch(pCur->hStmt) != SQL_SUCCESS;
	pCur->rowId++;
	return SQLITE_OK;
}

static int odbcEof(sqlite3_vtab_cursor* cur) {
	odbc_cursor* pCur = (odbc_cursor*)cur;
	return pCur->isEof;
}

static int odbcClose(sqlite3_vtab_cursor* cur) {
	odbc_cursor* pCur = (odbc_cursor*)cur;
	if (pCur->hStmt) {
		SQLFreeHandle(SQL_HANDLE_STMT, pCur->hStmt);
		pCur->hStmt = NULL;
	}
	sqlite3_free(pCur);
	return SQLITE_OK;
}

static sqlite3_module odbcModule = {
	/* iVersion    */ 1,
	/* xCreate     */ odbcCreate,
	/* xConnect    */ odbcConnect,
	/* xBestIndex  */ odbcBestIndex,
	/* xDisconnect */ odbcDestroy,
	/* xDestroy    */ odbcDestroy,
	/* xOpen       */ odbcOpen,
	/* xClose      */ odbcClose,
	/* xFilter     */ odbcFilter,
	/* xNext       */ odbcNext,
	/* xEof        */ odbcEof,
	/* xColumn     */ odbcColumn,
	/* xRowid      */ odbcRowid,
	/* xUpdate     */ 0,
	/* xBegin      */ 0,
	/* xSync       */ 0,
	/* xCommit     */ 0,
	/* xRollback   */ 0,
	/* xFindMethod */ 0,
	/* xRename     */ 0,
	/* xSavepoint  */ 0,
	/* xRelease    */ 0,
	/* xRollbackTo */ 0,
	/* xShadowName */ 0
};

static int GetSQLHandle(sqlite3_context* context, struct clientDataHandles* pHandles, const void* pConnStr) {
	int rc = SQLITE_OK;
	SQLHANDLE hEnv = NULL;
	SQLHANDLE hConn = NULL;
	struct clientConnectionhandle* pconH;
	if (pHandles->hEnv == NULL) {
		rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &pHandles->hEnv);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			const char* e = "Can't get access to ODBC";
			int elen = strlen(e);
			sqlite3_result_error(context, e, elen);
			pHandles->hEnv = NULL;
			goto CREATE_ERROR;
		}
		rc = SQLSetEnvAttr(pHandles->hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			const char* e = sqlite_OdbcErrorMsg(pHandles->hEnv, SQL_HANDLE_ENV, rc);
			sqlite3_result_error(context, e, strlen(e));
			SQLFreeHandle(SQL_HANDLE_ENV, pHandles->hEnv);
			pHandles->hEnv = NULL;
			goto CREATE_ERROR;
		}
		pHandles->refcnt++;
	}
	else {
		pHandles->refcnt++;
	}
	hEnv = pHandles->hEnv;
	XXH64_hash_t DSNName = XXH64(pConnStr, strlen(pConnStr), 0);
	HASH_FIND(hh, pHandles->cCh, &DSNName, sizeof(XXH64_hash_t), pconH);
	if (!pconH) {
		SQLHANDLE llhConn = NULL;
		rc = SQLAllocHandle(SQL_HANDLE_DBC, pHandles->hEnv, &llhConn);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			const char* e = sqlite_OdbcErrorMsg(pHandles->hEnv, SQL_HANDLE_ENV, rc);
			sqlite3_result_error(context, e, strlen(e));
			goto CREATE_ERROR;
		}
#define ODBCCHECKCATTRRC(rc) if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) goto ENDSETATTR;
		rc = SQLSetConnectAttr(llhConn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)5, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_WRITE, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_COMMITTED, 0);
		ODBCCHECKCATTRRC(rc)
			rc = SQLSetConnectAttr(llhConn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)TRUE, 0);
#undef ODBCCHECKCATTRRC
		ENDSETATTR :
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			const char* e = sqlite_OdbcErrorMsg(llhConn, SQL_HANDLE_DBC, rc);
			sqlite3_result_error(context, e, strlen(e));
			SQLFreeHandle(SQL_HANDLE_DBC, llhConn);
			llhConn = NULL;
			goto CREATE_ERROR;
		}
		TCHAR* dsn = utf8to16(pConnStr);
		rc = SQLDriverConnect(llhConn, NULL, dsn, _tcslen(dsn), SQL_NTS, 0, NULL, SQL_DRIVER_NOPROMPT);
		free(dsn);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			const char* e = sqlite_OdbcErrorMsg(llhConn, SQL_HANDLE_DBC, rc);
			sqlite3_result_error(context, e, strlen(e));
			SQLFreeHandle(SQL_HANDLE_DBC, llhConn);
			goto CREATE_ERROR;
		}
		hConn = llhConn;
		pconH = (struct clientConnectionhandle*)sqlite3_malloc(sizeof(struct clientConnectionhandle));
		if (!pconH) {
			sqlite3_result_error(context, "sqlite3_malloc failure.", sizeof("sqlite3_malloc failure."));
			goto CREATE_ERROR;
		}
		memset(pconH, 0, sizeof(struct clientConnectionhandle));
		pconH->DSNName = DSNName;
		pconH->hConn = hConn;
		pconH->refcnt += 1;
		HASH_ADD(hh, pHandles->cCh, DSNName, sizeof(XXH64_hash_t), pconH);
	}
	else {
		hConn = pconH->hConn;
		pconH->refcnt += 1;
	}
	sqlite3_result_int64(context, DSNName);
	return SQLITE_OK;
CREATE_ERROR:
	if (hEnv) {
		pHandles->refcnt -= 1;
		if (pHandles->refcnt == 0) {
			SQLFreeHandle(SQL_HANDLE_ENV, pHandles->hEnv);
			pHandles->hEnv = NULL;
		}
	}
	if (hConn) {
		if (!pconH) {
			SQLFreeHandle(SQL_HANDLE_DBC, hConn);
		}
		else {
			pconH->refcnt -= 1;
			if (pconH->refcnt == 0) {
				SQLFreeHandle(SQL_HANDLE_DBC, pconH->hConn);
				pconH->hConn = NULL;
				HASH_DEL(pHandles->cCh, pconH);
				sqlite3_free(pconH);
			}
		}
	}
	return SQLITE_ERROR;
}
static void sqlite3_OdbcConnect(sqlite3_context* context, int argc, sqlite3_value** argv) {
	assert(argc == 1);
	if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
		sqlite3_result_null(context);
		return;
	}
	const char* pConnStr = sqlite3_value_text(argv[0]);
	struct clientDataHandles* pHandles = (struct clientDataHandles*)sqlite3_user_data(context);
	return GetSQLHandle(context, pHandles, pConnStr);
}
static void sqlite3_OdbcDisconnect(sqlite3_context* context, int argc, sqlite3_value** argv) {
	assert(argc == 1);
	if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
		sqlite3_result_null(context);
		return;
	}
	XXH64_hash_t DSNName = sqlite3_value_int64(argv[0]);
	struct clientDataHandles* pHandles = sqlite3_user_data(context);
	if (pHandles) {
		struct clientConnectionhandle* pconH;
		HASH_FIND(hh, pHandles->cCh, &DSNName, sizeof(XXH64_hash_t), pconH);
		if (pconH) {
			pconH->refcnt -= 1;
			if (pconH->refcnt == 0) {
				SQLDisconnect(pconH);
				SQLFreeHandle(SQL_HANDLE_DBC, pconH);
				HASH_DEL(pHandles->cCh, pconH);
				sqlite3_free(pconH);
			}
			pHandles->refcnt--;
			if (pHandles->refcnt == 0) {
				SQLFreeHandle(SQL_HANDLE_ENV, pHandles->hEnv);
				pHandles->hEnv = NULL;
			}
			sqlite3_result_int(context, 1);
		}
	}
	return SQLITE_OK;
}
static void sqlite3_OdbcExec(sqlite3_context* context, int argc, sqlite3_value** argv) {
	int rc = SQLITE_OK;
	assert(argc >= 2);
	if ((sqlite3_value_type(argv[0]) == SQLITE_NULL) || (sqlite3_value_type(argv[1]) == SQLITE_NULL)) {
		sqlite3_result_null(context);
		return;
	}
	XXH64_hash_t DSNName = sqlite3_value_int64(argv[0]);
	TCHAR* wSQLstr = utf8to16(sqlite3_value_text(argv[1]));
	struct clientDataHandles* pHandles = sqlite3_user_data(context);
	struct clientConnectionhandle* pconH;
	SQLHANDLE hStmt = NULL;
	HASH_FIND(hh, pHandles->cCh, &DSNName, sizeof(XXH64_hash_t), pconH);
	if (pconH) {
		rc = SQLAllocHandle(SQL_HANDLE_STMT, pconH->hConn, &hStmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			goto EERROR;
		SQLSetStmtAttr(hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_KEYSET_DRIVEN, 0);
		rc = SQLPrepare(hStmt, wSQLstr, SQL_NTS);
		free(wSQLstr);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
			goto EERROR;
		}
		TCHAR* wcharPtr = NULL;
		SQLINTEGER cbNumeric = SQL_NULL_DATA;
		if (argc > 2) {
			int argcidx;
			for (argcidx = 2; argcidx < argc; argcidx += 1) {
				unsigned short col = argcidx - 1;
				switch (sqlite3_value_type(argv[argcidx])) {
				case SQLITE_INTEGER:
				{
					sqlite3_int64 lint64 = sqlite3_value_int64(argv[argcidx]);
					rc = SQLBindParameter(hStmt, col, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_NUMERIC, 8, 0, &lint64, sizeof(sqlite3_int64), NULL);
					break;
				}
				case SQLITE_FLOAT:
				{
					double dv = sqlite3_value_double(argv[argcidx]);
					rc = SQLBindParameter(hStmt, col, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 8, 0, &dv, sizeof(double), NULL);
					break;
				}
				case SQLITE_NULL: {
					rc = SQLBindParameter(hStmt, col, SQL_PARAM_INPUT, SQL_C_TCHAR, SQL_VARCHAR, 8, 0, NULL, 1, &cbNumeric);
					break;
				}
				case SQLITE_BLOB: {
					void* lbin = sqlite3_value_blob(argv[argcidx]);
					int llen = sqlite3_value_bytes(argv[argcidx]);
					rc = SQLBindParameter(hStmt, col, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, llen, 0, lbin, llen, &llen);
					break;
				}
				case SQLITE_TEXT: {
					wcharPtr = (const TCHAR*)sqlite3_value_text16(argv[argcidx]);
					rc = SQLBindParameter(hStmt, col, SQL_PARAM_INPUT, SQL_C_TCHAR, SQL_VARCHAR, 4000, 0, &wcharPtr[0], 0, NULL);
					break;
				}
				}
				if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
					goto EERROR;
			}
		}
		rc = SQLExecute(hStmt);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA) {
			goto EERROR;
		}
		SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
		sqlite3_result_int(context, 1);
		return;
	}
EERROR:
	if (hStmt) {
		if (rc == SQL_NEED_DATA)
		{
			SQLCancel(hStmt);
			const char* pe = "SQL_NEED_DATA";
			sqlite3_result_error(context, pe, strlen(pe));
		}
		else {
			const char* e = sqlite_OdbcErrorMsg(hStmt, SQL_HANDLE_STMT, rc);
			sqlite3_result_error(context, e, strlen(e));
		}
		SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
	}
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_odbc_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
	struct clientDataHandles* pH = sqlite3_malloc(sizeof(struct clientDataHandles));
	memset(pH, 0, sizeof(struct clientDataHandles));
	rc = sqlite3_create_module_v2(db, "odbc", &odbcModule, pH, sqlite3_free);
	rc = sqlite3_create_function_v2(db, "odbc_connect", -1, SQLITE_UTF8, pH, sqlite3_OdbcConnect, NULL, NULL, NULL);
	rc = sqlite3_create_function_v2(db, "odbc_disconnect", -1, SQLITE_UTF8, pH, sqlite3_OdbcDisconnect, NULL, NULL, NULL);
	rc = sqlite3_create_function_v2(db, "odbc_execute", -1, SQLITE_UTF8, pH, sqlite3_OdbcExec, NULL, NULL, NULL);
	return rc;
}