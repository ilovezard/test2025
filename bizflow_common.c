/*
 *
 *
 * Biz-flow Processor
 *
 *
 * FileName: bizflow_common.c
 *
 *  <Date>        <Author>       <Auditor>     <Desc>
 */
/*--------------------------- Include files -----------------------------*/
#include	"bizflow_private.h"

#include	<stdio.h>
#include	<stdlib.h>
#include	<string.h>

#include	"dllmgr_itf.h"
#include	"os_dll.h"
#include	"top_dbs.h"
#include	"util_str.h"
#include	"util_expr.h"

/*--------------------------- Macro define ------------------------------*/
#define	_TINY_BUF_LEN	32
#define	_SMALL_BUF_LEN	256
#define	_LARGE_BUF_LEN	1024
#define	_HUGE_BUF_LEN	10240

#define	setnull(x)		memset(&(x), 0, sizeof(x))
#define	setpnull(x)		memset(x, 0, sizeof(*(x)))
#define	max(x,y)		((x) > (y) ? (x) : (y))
#define	min(x,y)		((x) < (y) ? (x) : (y))

/*---------------------------- Type define ------------------------------*/

/*---------------------- Local function declaration ---------------------*/

/*-------------------------  Global variable ----------------------------*/

/*-------------------------  Global functions ---------------------------*/
int
bizTestGetFlow(T_BIZ_FLOW_ENV *ptEnv)
{
	T_BIZ_FLOW_UNIT	tUnit;
	int				iRet;

	iRet = dbsExecuteV(ptEnv->sGetFlow, "flow_id", tUnit.sExecType,
	                   tUnit.sExecFunc, tUnit.sExecDll, tUnit.sSucReturn,
	                   tUnit.sQuitFlag, tUnit.sFlowCase, tUnit.sFlowDesc);
	if (iRet != ERR_TOP_DBS_OK && iRet != ERR_TOP_DBS_NOTFOUND) {
		_(LOG_WRN, "dbsExecuteV() error on [%d], sSql[%s] sErrStr[%s]",
		  iRet, ptEnv->sGetFlow, dbsErrStr());
		return 0;
	}

	return 1;
}

T_CTN *
bizDbFlowLoad(T_BIZ_FLOW_ENV *ptEnv, char *sFlowId, int *piRet)
{
	T_CTN *ptCtn = ctnNew(sizeof(T_BIZ_FLOW_UNIT), NULL);
	if (!ptCtn) {
		_(LOG_ERR, "ctnNew() error, iSize[%zu]", sizeof(T_BIZ_FLOW_UNIT));
		  *piRet = ERR_BIZ_FLOW_LOAD;
		return NULL;
	}

	T_TOP_DBS_STMT hStmt;

	int iRet = dbsOpenV(&hStmt, ptEnv->sGetFlow, sFlowId);
	if (iRet < 0) {
		_(LOG_ERR, "dbsOpenV() error on [%d], sSql[%s] sFlowId[%s] sErrStr[%s]",
		  iRet, ptEnv->sGetFlow, sFlowId, dbsErrStr());
		ctnFree(ptCtn);
		*piRet = ERR_BIZ_FLOW_LOAD;
		return NULL;
	}

	T_BIZ_FLOW_UNIT	tUnit;
	int				iFirst = 1;

	setnull(tUnit);
	while ((iRet = dbsFetchV(hStmt, tUnit.sExecType, tUnit.sExecFunc,
	                         tUnit.sExecDll, tUnit.sSucReturn, tUnit.sQuitFlag,
	                         tUnit.sFlowCase, tUnit.sFlowQueue, tUnit.sFlowDesc)) == 0) {
		strRightTrim(tUnit.sExecFunc );
		strRightTrim(tUnit.sExecDll  );
		strRightTrim(tUnit.sSucReturn);
		strRightTrim(tUnit.sFlowCase );

		iRet = ctnAdd(ptCtn, &tUnit);
		if (iRet < 0) {
			_(LOG_ERR, "ctnAdd() error on [%d]", iRet);
			dbsCloseV(hStmt);
			ctnFree(ptCtn);
			*piRet = ERR_BIZ_FLOW_LOAD;
			return NULL;
		}

		if (iFirst) {
			iFirst = 0;
			_(LOG_TRC, "---- ==== All Units: sFlowId[%s] ==== ----", sFlowId);
		}
		bizPrintUnit(&tUnit, LOG_TRACE);

		setnull(tUnit);
	}
	if (iRet != ERR_TOP_DBS_NOTFOUND || ctnCount(ptCtn) == 0) {
		dbsCloseV(hStmt);
		ctnFree(ptCtn);
		if (iRet == ERR_TOP_DBS_NOTFOUND) {
			_(LOG_WRN, "sFlowId[%s] Is NULL", sFlowId);
			*piRet = ERR_BIZ_FLOW_NOTFOUND;

		} else {
			_(LOG_ERR,
			  "dbsFetchV() error on [%d], sSql[%s] sFlowId[%s] sErrStr[%s]",
			  iRet, ptEnv->sGetFlow, sFlowId, dbsErrStr());
			*piRet = ERR_BIZ_FLOW_LOAD;
		}
		return NULL;
	}

	dbsCloseV(hStmt);
	*piRet = 0;
	return ptCtn;
}

int
bizPrintUnit(T_BIZ_FLOW_UNIT *ptUnit, int iLevel)
{
	_(F_LOG_INFO(iLevel), "sExecType[%s] sExecFunc[%s] sExecDll[%s] "
	  "sQuitFlag[%s] sSucReturn[%s] sFlowCase[%s] sFlowQueue[%s]",
	  ptUnit->sExecType, ptUnit->sExecFunc, ptUnit->sExecDll,
	  ptUnit->sQuitFlag, ptUnit->sSucReturn, ptUnit->sFlowCase,ptUnit->sFlowQueue);

	return 0;
}

int
bizIsSuccess(T_BIZ_FLOW_UNIT *ptUnit, int iFlag)
{
	T_SPLIT_STR	tSucReturn;
	int			iRet, i;

	if (iFlag == 0) return 1;

	if (ptUnit->sSucReturn[0] == '\0') {
		tSucReturn.iCnt = 1;
		tSucReturn.psCol[0] = ptUnit->sSucReturn;

	} else {
		iRet = csvSplitStr(ptUnit->sSucReturn, BIZ_CHAR_SEP_RET, &tSucReturn);
		if (iRet < 0) {
			_(LOG_ERR, "csvSplitStr() error on [%d], sString[%s] cSplit[%c]",
			  iRet, ptUnit->sSucReturn, BIZ_CHAR_SEP_RET);
			return -1;
		}
	}

	for (i = 0; i < tSucReturn.iCnt; i++) {
		if (iFlag == atoi(tSucReturn.psCol[i])) {
			return 1;
		}
	}

	return 0;
}

int
bizShiftEncrypt(char *sOut, char *sIn)
{
	int	i, iOffset, iLen = strlen(sIn);

	if (iLen % 3 == 2)
		iOffset = 2;
	else if (iLen % 3 == 1)
		iOffset = -1;
	else
		iOffset = 1;

	for (i = 0; i < iLen; i++) {
		if ((sIn[i] + iOffset == ' '  && i == iLen - 1) ||
			(sIn[i] + iOffset == '\t' && i == iLen - 1) ||
			(sIn[i] + iOffset == '\r' && i == iLen - 1) ||
			(sIn[i] + iOffset == '\n' && i == iLen - 1)) {
			sOut[i] = sIn[i];
		}
		else {
			sOut[i] = sIn[i] + iOffset;
		}
	}
	sOut[i] = '\0';

	return 0;
}

int
bizShiftDecrypt(char *sOut, char *sIn)
{
	int	i, iOffset, iLen = strlen(sIn);

	if (iLen % 3 == 2)
		iOffset = 2;
	else if (iLen % 3 == 1)
		iOffset = -1;
	else
		iOffset = 1;

	for (i = 0; i < iLen; i++) {
		if ((sIn[i] - iOffset == ' '  && i == iLen-1) ||
			(sIn[i] - iOffset == '\t' && i == iLen-1) ||
			(sIn[i] - iOffset == '\r' && i == iLen-1) ||
			(sIn[i] - iOffset == '\n' && i == iLen-1)) {
			sOut[i] = sIn[i];
		}
		else {
			sOut[i] = sIn[i] - iOffset;;
		}
	}
	sOut[i] = '\0';

	return 0;
}

int
bizRunFunc(char *sDll, char *sFunc, T_BIZ_ARGS *ptArgs)
{
	void	*pfnFun;
	int		iRet, i;

	pfnFun = DLM_GetFunW(sDll, sFunc);
	if (!pfnFun) {
		_(LOG_ERR, "DLM_GetFunW() error, sLibName[%s] sFunName[%s]",
		  sDll, sFunc);
		return ERR_BIZ_FUNC_LOAD;
	}
	_(LOG_TRC, "sDll[%s] sFunc[%s] = [%p]", sDll, sFunc, pfnFun);

	T_OS_DLL_FUN_ARG	tFunArg;

	setnull(tFunArg);
	tFunArg.iArgc = ptArgs->iArgc;
	for (i = 0; i < tFunArg.iArgc; i++) {
		tFunArg.psArgV[i] = ptArgs->sArgv[i];
	}

	iRet = dllExecFun(pfnFun, &tFunArg, 0);
	if (iRet < ERR_DLL_BASE) {
		_(LOG_ERR, "dllExecFun() error on [%d]", iRet);
		return ERR_BIZ_FUNC_EXEC;
	}

	return iRet;
}

char *
bizParseVar(char *sVar, int *piLen, int *piType)
{
	T_POC_VALUE	tValue;
	int			iLen;

	strTrim(sVar);
	iLen = strlen(sVar);
	if (piLen) *piLen = iLen;
	if (piType) *piType = 0;

	if (sVar[0] == '"') {
		if (sVar[iLen-1] != '"') {
			_(LOG_ERR, "Variable Syntax Error, sVar[%s]", sVar);
			return NULL;
		}
		sVar[iLen-1] = '\0';
		sVar++;
		if (piLen) *piLen -= 2;

	} else if (sVar[0] == ':') {
		if (sVar[1] == '"') {
			if (sVar[iLen-1] != '"') {
				_(LOG_ERR, "Variable Syntax Error, sVar[%s]", sVar);
				return NULL;
			}
			sVar[iLen-1] = '\0';
			sVar += 2;
		} else {
			sVar++;
		}

		setnull(tValue);
		POC_GetValue(sVar, &tValue);
		sVar = (char *)tValue.psValue;
		if (piLen) *piLen = tValue.iLen;
		if (piType) *piType = tValue.iType;
	}

	return sVar;
}

int
bizParseVars(T_BIZ_ARGS *ptArgs, char *sVarList)
{
	T_SPLIT_STR	tList;
	int			iType, iRet, i;

	if (sVarList[0] == '\0') {
		ptArgs->iArgc = 0;

	} else {
		iRet = csvSplitStr(sVarList, BIZ_CHAR_SEP_VAR, &tList);
		if (iRet < 0) {
			_(LOG_ERR, "csvSplitStr() error on [%d], sString[%s] cSplitChar[%c]",
			  iRet, sVarList, BIZ_CHAR_SEP_VAR);
			return -1;
		}

		if (tList.iCnt > BIZ_ARGS_MAX) {
			_(LOG_ERR, "too many args, iCnt[%d] iMax[%d]",
			  tList.iCnt, BIZ_ARGS_MAX);
			return -2;
		}

		ptArgs->iArgc = tList.iCnt;
	}

	_(LOG_TRC, "iArgc=[%d]", ptArgs->iArgc);

	for (i = 0; i < ptArgs->iArgc; i++) {
		ptArgs->sArgv[i] = bizParseVar(tList.psCol[i], ptArgs->iArgl + i,
		                               &iType);
		if (!ptArgs->sArgv[i]) {
			_(LOG_ERR, "bizParseVar() error, sVar[%s]", tList.psCol[i]);
			return -3;
		}
		_(LOG_TRC, "sArgv[%d]=[%d][%.*s]",
		  i, ptArgs->iArgl[i], iType ? 0 : min(ptArgs->iArgl[i], 60),
		  ptArgs->sArgv[i]);
	}

	return 0;
}

int
bizReplaceVar(char *sIn, char *sOut, int iSize, int iOpt)
{
	T_POC_VALUE	tValue;
	char		*pCur = sIn, *pLast, *pOut = sOut;
	char		sTag[_SMALL_BUF_LEN];
	int			iFlag;

	#define	bizCopy(p,l)                               \
	do {                                               \
		if (iSize < (l)) {                             \
			_(LOG_ERR, "Output Buffer is Not Enough"); \
			return -2;                                 \
		}                                              \
		memcpy(pOut, (p), (l));                        \
		pOut += (l);                                   \
		iSize -= (l);                                  \
	} while(0)

	iSize--;
	while (pCur) {
		pLast = pCur;

		pCur = strchr(pCur, ':');
		if (pCur) {
			if (pCur == sIn ||
			    *(pCur - 1) == ' '  || *(pCur - 1) == '\t' ||
			    *(pCur - 1) == '\r' || *(pCur - 1) == '\n') {
				iFlag = 1;
			} else {
				iFlag = 0;
			}

			bizCopy(pLast, pCur - pLast);

			pCur++;
			if (*pCur == '\"') {
				pCur++;
				pLast = pCur;
				pCur = strchr(pCur, '"');
				if (!pCur) {
					_(LOG_ERR, "Missing quote");
					return -1;
				}
				strMem2Str(sTag, sizeof(sTag), pLast, pCur - pLast);
				pCur++;

				if (*pCur == '\0' || *pCur == ' ' || *pCur == '\t' ||
				    *pCur == '\r' || *pCur == '\n') {
					iFlag = iFlag && 1;
				} else {
					iFlag = iFlag && 0;
				}
			} else {
				pLast = pCur;
				pCur = strpbrk(pCur, " \t\r\n");
				strMem2Str(sTag, sizeof(sTag), pLast,
				           pCur ? pCur - pLast : -1);

				if (!pCur || *pCur == ' '  || *pCur == '\t' ||
				             *pCur == '\r' || *pCur == '\n') {
					iFlag = iFlag && 1;
				} else {
					iFlag = iFlag && 0;
				}
			}

			     if (iOpt > 0) iFlag = 1;
			else if (iOpt < 0) iFlag = 0;

			POC_GetValue(sTag, &tValue);
			if (iFlag) bizCopy("\"", 1);
			bizCopy(tValue.psValue, strLength(tValue.psValue, tValue.iLen));
			if (iFlag) bizCopy("\"", 1);

		} else {
			bizCopy(pLast, strlen(pLast));
		}
	}
	*pOut = '\0';

	#undef	bizCopy
	return pOut - sOut;
}

int
bizTestCase(char *sCase)
{
	char	sBuf[_HUGE_BUF_LEN];
	int		iRet;

	iRet = bizReplaceVar(sCase, sBuf, sizeof(sBuf), 0);
	if (iRet < 0) {
		_(LOG_ERR, "bizReplaceVar() error on [%d], sCase[%s]", iRet, sCase);
		return -1;
	}

	if (sBuf[0] == '\0') {
		iRet = 1;
	} else {
		_(LOG_TRC, "sRealCase[%s]", sBuf);
		iRet = exprLogic(sBuf);
		if (iRet < 0) {
			_(LOG_ERR, "exprLogic() error on [%d], sBuf[%s] sErrStr[%s]",
			  iRet, sBuf, exprErrStr());
			return -2;
		}
	}

	return iRet;
}

/*-------------------------  Local functions ----------------------------*/

/*-----------------------------  End ------------------------------------*/
