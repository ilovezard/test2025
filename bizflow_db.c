/*
 *
 *
 * Biz-flow Processor
 *
 *
 * FileName: bizflow.c
 *
 *  <Date>        <Author>       <Auditor>     <Desc>
 */
/*--------------------------- Include files -----------------------------*/
#include	"dle_bizflow_db.h"
#include	"bizflow_private.h"

#include	<stdio.h>
#include	<stdlib.h>
#include	<string.h>

#include	"poc_helper.h"
#include	"top_dbs.h"
#include	"util_str.h"
#include    "poc_itf.h"

/*--------------------------- Macro define ------------------------------*/
#define	_TINY_BUF_LEN	32
#define	_SMALL_BUF_LEN	256
#define	_LARGE_BUF_LEN	1024
#define	_HUGE_BUF_LEN	10240

#define	setnull(x)		memset(&(x), 0, sizeof(x))
#define	setpnull(x)		memset(x, 0, sizeof(*(x)))
#define	max(x,y)		((x) > (y) ? (x) : (y))
#define	min(x,y)		((x) < (y) ? (x) : (y))

#define	bizReturn(r)	\
do {                                    \
	mySetInt32(ptEnv->sRetTag, (r));    \
	return (r);                         \
} while(0)

/*---------------------------- Type define ------------------------------*/

/*---------------------- Local function declaration ---------------------*/
static int bizSetError(T_BIZ_FLOW_UNIT *ptUnit, int iRet, char *psFlowId);

/*-------------------------  Global variable ----------------------------*/
static int f_iCurFlowQueue;
static char f_sError[_LARGE_BUF_LEN];

/*-------------------------  Global functions ---------------------------*/
T_BIZ_FLOW_ENV *
bizDbFlowInit(char *sPrefix, char *sSqlFlag, char *sRetTag)
{
	T_BIZ_FLOW_ENV *ptEnv = malloc(sizeof(*ptEnv));
	if (!ptEnv) {
		_(LOG_ERR, "malloc() error, iSize[%zu] sErrStr[%s]",
		  sizeof(*ptEnv), strErrno());
		return NULL;
	}

	setpnull(ptEnv);

	ptEnv->sGetFlow = strAprintf(
		"select exec_type, exec_func, exec_dll, "
		       "suc_return, nor_quit_flag, flow_case, flow_queue, flow_desc "
		  "from %sbizflow_cfg "
		 "where flow_id = :id and status = '1' "
		 "order by flow_queue ", sPrefix);
	if (!ptEnv->sGetFlow) {
		_(LOG_ERR, "strAprintf() error, sErrStr[%s]", strErrno());
		bizDbFlowFinal(ptEnv);
		return NULL;
	}

	if (!bizTestGetFlow(ptEnv)) {
		free(ptEnv->sGetFlow);
		ptEnv->sGetFlow = strAprintf(
			"select exec_type, exec_func, exec_dll, "
			       "suc_return, nor_quit_flag, flow_case, flow_queue, flow_desc "
			  "from %sbizflow_cfg "
			 "where flow_id = :id and status = '1' "
			 "order by flow_queue ", sPrefix);
		if (!ptEnv->sGetFlow) {
			_(LOG_ERR, "strAprintf() error, sErrStr[%s]", strErrno());
			bizDbFlowFinal(ptEnv);
			return NULL;
		}
	}

	ptEnv->sGetSql = strAprintf(
		"select sql_string, sql_encrypted "
		  "from %sbizsql_cfg "
		 "where sql_id = :id ", sPrefix);
	if (!ptEnv->sGetSql) {
		_(LOG_ERR, "strAprintf() error, sErrStr[%s]", strErrno());
		bizDbFlowFinal(ptEnv);
		return NULL;
	}

	ptEnv->sUpdSql = strAprintf(
		"update %sbizsql_cfg "
		   "set sql_encrypted = :sql "
		 "where sql_id = :id ", sPrefix);
	if (!ptEnv->sUpdSql) {
		_(LOG_ERR, "strAprintf() error, sErrStr[%s]", strErrno());
		bizDbFlowFinal(ptEnv);
		return NULL;
	}

	ptEnv->iSqlFlag = atoi(sSqlFlag);

	ptEnv->sRetTag = strdup(sRetTag);
	if (!ptEnv->sRetTag) {
		_(LOG_ERR, "strdup() error, sString[%s] sErrStr[%s]",
		  sRetTag, strErrno());
		bizDbFlowFinal(ptEnv);
		return NULL;
	}

	return ptEnv;
}

int
bizDbFlowFinal(T_BIZ_FLOW_ENV *ptEnv)
{
	if (ptEnv) {
		free(ptEnv->sGetFlow);
		free(ptEnv->sGetSql);
		free(ptEnv->sUpdSql);
		free(ptEnv->sRetTag);
		free(ptEnv);
	}
	return 0;
}

F_BIZ_FLOW_CALLBACK
bizDbFlowSetCallback(T_BIZ_FLOW_ENV *ptEnv, F_BIZ_FLOW_CALLBACK fCallback)
{
	F_BIZ_FLOW_CALLBACK	fRet;

	if (!ptEnv) return NULL;

	fRet = ptEnv->fCallback;
	ptEnv->fCallback = fCallback;

	return fRet;
}

int
bizDbFlowExecFlow(T_BIZ_FLOW_ENV *ptEnv, char *sFlowId)
{
    f_iCurFlowQueue = 0;
    PFN_TOP_DBS_ERR_CALL fErrFuncTmp=NULL;
	if (!ptEnv || !sFlowId) {
		_(LOG_ERR, "sErrStr[Invalid argument]");
		return ERR_BIZ_PARAM;
	}

	bizSetError(NULL, 0, NULL);
	ptEnv->iLastType = BIZ_TYPE_FLOW;
    
	char	sRealFlowId[_LARGE_BUF_LEN];
	int		iRet;

	iRet = bizReplaceVar(sFlowId, sRealFlowId, sizeof(sRealFlowId), -1);
	if (iRet < 0) {
		_(LOG_ERR, "bizReplaceVar() error on [%d], sCase[%s]", iRet, sFlowId);
		bizReturn(ERR_BIZ_SYNTAX_FLOW);
	}

	/* Get Node */
	char *psNode = strchr(sRealFlowId, '@');
	if ( NULL != psNode ) {
		*psNode = '\0';
		psNode += 1;

		POC_SetValueS(VAR_POC_DEFAULT_OBJECT_TAG, psNode, -1);
	}

	T_CTN *ptCtn = bizDbFlowLoad(ptEnv, sRealFlowId, &iRet);
	if (!ptCtn) {
		if (iRet == ERR_BIZ_FLOW_NOTFOUND) {
			_(LOG_WRN, "bizDbFlowLoad() sFlowId[%s] Is not found", sRealFlowId);
		}else{
			_(LOG_ERR, "bizDbFlowLoad() error on [%d], sFlowId[%s]", iRet, sRealFlowId);
		}
		bizReturn(iRet);
	}

	while (1) {
		if ( NULL != psNode ) {
			iRet = POC_ArrayFetch(psNode);	
			if( iRet == ERR_POC_NOTFOUND ) {
				break;
			}

			if( ERR_POC_OK != iRet ) {
				_(LOG_ERR, "POC_ArrayFetch error, iRet[%d]", iRet);
				bizReturn(iRet);
			}
		}

		T_BIZ_FLOW_UNIT	*ptUnit;
		T_BIZ_FLOW_UNIT	tUnit;
		int				iRet2, i = -1;

		if ( NULL == psNode ) {
			_(LOG_NOR, "flow [%s]", sRealFlowId);
		} else {
			_(LOG_NOR, "flow [%s] node[%s]", sRealFlowId, psNode);
		}
		while ((ptUnit = ctnGet(ptCtn, ++i))) {
			/* 如果多次执行，复制数据以方串改数据 */
			if ( NULL != psNode ) {
				memcpy(&tUnit, ptUnit, sizeof(tUnit));
				ptUnit = &tUnit;
			}
		    
		    f_iCurFlowQueue = atoi(ptUnit->sFlowQueue);
		    
			iRet = bizTestCase(ptUnit->sFlowCase);
			if (iRet < 0) {
				_(LOG_ERR, "bizTestCase() error on [%d] ==>", iRet);
				bizPrintUnit(ptUnit, LOG_ERROR);
				ctnFree(ptCtn);
				bizReturn(ERR_BIZ_SYNTAX_CASE);
			} else if (iRet == 0) {
				continue;
			}

			_(LOG_NOR, "[%s] [%s] [%s]", ptUnit->sFlowDesc, ptUnit->sExecFunc,ptUnit->sSucReturn);
			bizPrintUnit(ptUnit, LOG_TRACE);
    	    
			switch (ptUnit->sExecType[0]) {
				case BIZ_TYPE_SET: {
					iRet = bizDbFlowExecSet(ptEnv, ptUnit->sExecFunc);
					break;
				}
				case BIZ_TYPE_SQL: {
				    fErrFuncTmp=dbsSetErrCall(NULL);
					iRet = bizDbFlowExecSql(ptEnv,
					                      ptUnit->sExecFunc, ptUnit->sExecDll);
    	            dbsSetErrCall(fErrFuncTmp);
					break;
				}
				case BIZ_TYPE_FUNC: {
					iRet = bizDbFlowExecFunc(ptEnv,
					                       ptUnit->sExecFunc, ptUnit->sExecDll);
					break;
				}
				case BIZ_TYPE_LOCK: {
				    fErrFuncTmp=dbsSetErrCall(NULL);
					iRet = bizDbFlowExecSql4Lock(ptEnv,
					                      ptUnit->sExecFunc, ptUnit->sExecDll);
    	            dbsSetErrCall(fErrFuncTmp);
					break;
				}
				case BIZ_TYPE_FLOW: {
					iRet = bizDbFlowExecFlow(ptEnv, ptUnit->sExecFunc);
					break;
				}
				case BIZ_TYPE_RETURN: {
					iRet = bizDbFlowExecReturn(ptEnv, ptUnit->sExecFunc);
					ctnFree(ptCtn);
					bizReturn(iRet);
				}
				default: {
					_(LOG_ERR, "Unknown Type[%s] ==>", ptUnit->sExecType);
					bizPrintUnit(ptUnit, LOG_ERROR);
					ctnFree(ptCtn);
					bizReturn(ERR_BIZ_UNKNOWN_TYPE);
				}
			}

			if (iRet < ERR_BIZ_BASE) {
				_(LOG_ERR, "Execute Type[%s] System Error on [%d] ==>",
				  ptUnit->sExecType, iRet);
				bizPrintUnit(ptUnit, LOG_ERROR);
				ctnFree(ptCtn);
				bizReturn(iRet);
			}

			iRet2 = bizIsSuccess(ptUnit, iRet);
			if (iRet2 < 0) {
				_(LOG_ERR, "bizIsSuccess() error on [%d] ==>", iRet2);
				bizPrintUnit(ptUnit, LOG_ERROR);
				ctnFree(ptCtn);
				bizReturn(ERR_BIZ_SYNTAX_RET);

			} else if (iRet2 > 0) {
				if (iRet != 0 && ptUnit->sQuitFlag[0] == BIZ_CHAR_QUIT_TRUE) {
					break;
				}

			} else {
    	        if (BIZ_TYPE_SQL == ptUnit->sExecType[0] || BIZ_TYPE_LOCK == ptUnit->sExecType[0]) {
    	            dbsLog(LOG_ERR, "Execute Type[%s] User Error on [%d] ==>",
    	              ptUnit->sExecType, iRet);
    	        } else {
    	            _(LOG_ERR, "Execute Type[%s] User Error on [%d] ==>",
    	              ptUnit->sExecType, iRet);
    	        }
				bizSetError(ptUnit, iRet, sFlowId);
				bizPrintUnit(ptUnit, LOG_ERROR);
				ctnFree(ptCtn);
				bizReturn(iRet);
			}

			if (ptEnv->fCallback) {
				iRet = ptEnv->fCallback(iRet);
				if (iRet < 0) {
					_(LOG_ERR, "Execute Type[%s] Callback Error on [%d] ==>",
					  ptUnit->sExecType, iRet);
					bizPrintUnit(ptUnit, LOG_ERROR);
					ctnFree(ptCtn);
					bizReturn(iRet);
				}
			}
		}

		if ( NULL == psNode ) {
			break;
		}
	}

	ctnFree(ptCtn);

	bizReturn(0);
}

int
bizDbFlowExecSet(T_BIZ_FLOW_ENV *ptEnv, char *sSet)
{
	if (!ptEnv || !sSet) {
		_(LOG_ERR, "sErrStr[Invalid argument]");
		return ERR_BIZ_PARAM;
	}

	ptEnv->iLastType = BIZ_TYPE_SET;

	T_BIZ_ARGS	tName, tValue;
	char		*pValue;
	int			iRet, i;

	pValue = strchr(sSet, '=');
	if (!pValue) {
		_(LOG_ERR, "expect equal '='");
		bizReturn(ERR_BIZ_SYNTAX_SET);
	}
	*pValue++ = '\0';

	iRet = bizParseVars(&tName, sSet);
	if (iRet < 0) {
		_(LOG_ERR, "bizParseVars() error, sList[%s]", sSet);
		bizReturn(ERR_BIZ_SYNTAX_VAR);
	}

	iRet = bizParseVars(&tValue, pValue);
	if (iRet < 0) {
		_(LOG_ERR, "bizParseVars() error, sList[%s]", pValue);
		bizReturn(ERR_BIZ_SYNTAX_VAR);
	}

	for (i = 0; i < min(tName.iArgc, tValue.iArgc); i++) {
		mySetStr(tName.sArgv[i], tValue.sArgv[i], tValue.iArgl[i]);
	}
	for (/* noop */; i < tName.iArgc; i++) {
		mySetStr(tName.sArgv[i], "", 0);
	}

	bizReturn(0);
}

int
bizDbFlowExecSql(T_BIZ_FLOW_ENV *ptEnv, char *sSqlId, char *sDefObj)
{
	if (!ptEnv || !sSqlId) {
		_(LOG_ERR, "sErrStr[Invalid argument]");
		return ERR_BIZ_PARAM;
	}

	ptEnv->iLastType = BIZ_TYPE_SQL;
	char *psNode = strchr(sSqlId, '@');
	if ( NULL != psNode ) {
		*psNode = '\0';
		psNode += 1;
	}

	char	sSql[DLEN_BIZ_SQL+1], sSqlEncry[DLEN_BIZ_SQL+1];

	int iRet = dbsExecuteV(ptEnv->sGetSql, sSqlId, sSql, sSqlEncry);
	if (iRet < 0) {
		_(LOG_ERR,
		  "dbsExecuteV() error on [%d], sSql[%s] sSqlId[%s] sErrStr[%s]",
		  iRet, ptEnv->sGetSql, sSqlId, dbsErrStr());
		bizReturn(ERR_BIZ_GET_SQL);
	}

	strRightTrim(sSql);

	if (ptEnv->iSqlFlag) {
		if (sSqlEncry[0] == '\0') {
			bizShiftEncrypt(sSqlEncry, sSql);

			iRet = dbsExecuteV(ptEnv->sUpdSql, sSqlEncry, sSqlId);
			if (iRet < 0) {
				_(LOG_ERR, "dbsExecuteV() error on [%d], "
				  "sSql[%s] sSqlEncry[%s] sSqlId[%s] sErrStr[%s]",
				  iRet, ptEnv->sUpdSql, sSqlEncry, sSqlId, dbsErrStr());
				bizReturn(ERR_BIZ_UPD_SQL);
			}

		} else {
			bizShiftDecrypt(sSql, sSqlEncry);
		}
	}

	if (sDefObj && sDefObj[0]) {
		iRet = POC_Helper_ExcSqlD(sSql, sDefObj);
		if (iRet < 0) {
			_(LOG_TRC, "POC_Helper_ExcSqlD() error on [%d], sDefObj[%s]",
			  iRet, sDefObj);
		}

	} else if ( NULL == psNode ) {
		iRet = POC_Helper_ExcSqlX(sSql);
		if (iRet < 0) {
			_(LOG_TRC, "POC_Helper_ExcSqlX() error on [%d]", iRet);
		}
	} else {
		iRet = POC_Helper_ExcSqlX_Node(sSql, psNode);
		if ( iRet < 0 ) {
			_(LOG_TRC, "POC_Helper_ExcSqlX_Node() error on [%d]", iRet);
		}
	}

	bizReturn(iRet);
}
/*ADD BY RYAN  20130411    新增数据库锁类型处理*/
int
bizDbFlowExecSql4Lock(T_BIZ_FLOW_ENV *ptEnv, char *sSqlId, char *sDefObj)
{
	if (!ptEnv || !sSqlId) {
		_(LOG_ERR, "sErrStr[Invalid argument]");
		return ERR_BIZ_PARAM;
	}

	char *psNode = strchr(sSqlId, '@');
	if ( NULL != psNode ) {
		*psNode = '\0';
		psNode += 1;
	}

	ptEnv->iLastType = BIZ_TYPE_SQL;

	char	sSql[DLEN_BIZ_SQL+1], sSqlEncry[DLEN_BIZ_SQL+1];

	int iRet = dbsExecuteV(ptEnv->sGetSql, sSqlId, sSql, sSqlEncry);
	if (iRet < 0) {
		_(LOG_ERR,
		  "dbsExecuteV() error on [%d], sSql[%s] sSqlId[%s] sErrStr[%s]",
		  iRet, ptEnv->sGetSql, sSqlId, dbsErrStr());
		bizReturn(ERR_BIZ_GET_SQL);
	}

	strRightTrim(sSql);

	if ( NULL == psNode ) {
		iRet = POC_Helper_ExcSqlX4Lock(sSql);
		if (iRet < 0) {
			_(LOG_TRC, "POC_Helper_ExcSqlX() error on [%d]", iRet);
		}
	} else {
		iRet = POC_Helper_ExcSqlX4Lock_Node(sSql, psNode);
		if (iRet < 0) {
			_(LOG_TRC, "POC_Helper_ExcSqlX_Node() error on [%d]", iRet);
		}
	}

	bizReturn(iRet);
}
/*ADD BY RYAN  20130411   END */

int
bizDbFlowExecFunc(T_BIZ_FLOW_ENV *ptEnv, char *sFunc, char *sDll)
{
	if (!ptEnv || !sFunc || !sDll) {
		_(LOG_ERR, "sErrStr[Invalid argument]");
		return ERR_BIZ_PARAM;
	}

	ptEnv->iLastType = BIZ_TYPE_FUNC;

	T_BIZ_ARGS	tArgs;
	char		*pArgs;
	int			iRet, iLen;

	pArgs = strchr(sFunc, '(');
	if (!pArgs) {
		_(LOG_ERR, "expect left bracket '('");
		bizReturn(ERR_BIZ_SYNTAX_FUNC);
	}
	*pArgs++ = '\0';

	iLen = strlen(pArgs) - 1;
	if (pArgs[iLen] != ')') {
		_(LOG_ERR, "expect right bracket ')'");
		bizReturn(ERR_BIZ_SYNTAX_FUNC);
	}
	pArgs[iLen] = '\0';

	strTrim(sFunc);
    strRightTrim(pArgs);
	iRet = bizParseVars(&tArgs, pArgs);
	if (iRet < 0) {
		_(LOG_ERR, "bizParseVars() error, sList[%s]", pArgs);
		bizReturn(ERR_BIZ_SYNTAX_VAR);
	}

	iRet = bizRunFunc(sDll, sFunc, &tArgs);
	if (iRet < ERR_BIZ_BASE) {
		_(LOG_ERR, "bizRunFunc() error, sDll[%s] sFunc[%s]", sDll, sFunc);
	}

	bizReturn(iRet);
}

int
bizDbFlowExecReturn(T_BIZ_FLOW_ENV *ptEnv, char *sRet)
{
	if (!ptEnv || !sRet) {
		_(LOG_ERR, "sErrStr[Invalid argument]");
		return ERR_BIZ_PARAM;
	}

	ptEnv->iLastType = BIZ_TYPE_RETURN;

	char	*pRet;

	pRet = bizParseVar(sRet, NULL, NULL);
	if (!pRet) {
		_(LOG_ERR, "bizParseVar() error, sVar[%s]", sRet);
		bizReturn(ERR_BIZ_SYNTAX_VAR);
	}

	bizReturn(atoi(pRet));
}

int
bizDbFlowLastType(T_BIZ_FLOW_ENV *ptEnv)
{
	return ptEnv ? ptEnv->iLastType : -1;
}

int 
bizDbFlowLastQueue(void)
{
    return (f_iCurFlowQueue); 
}

char *
bizDbFlowError(void)
{
	return f_sError;
}

/*-------------------------  Local functions ----------------------------*/
static int bizSetError(T_BIZ_FLOW_UNIT *ptUnit, int iRet, char *psFlowId)
{
	if ( NULL == ptUnit || NULL == psFlowId ) {
		f_sError[0] = '\0';
		return 0;
	}

	if ( '\0' != f_sError[0] ) {
		return 0;
	}

	snprintf(f_sError, sizeof(f_sError), "Flow[%s] Desc[%s] Func[%s] , error[%d]",
			psFlowId, ptUnit->sFlowDesc, ptUnit->sExecFunc, iRet);

	return 0;
}

/*-----------------------------  End ------------------------------------*/
