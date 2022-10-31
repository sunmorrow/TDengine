/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "tsdb.h"

static int32_t tsdbWriteBlockDataEx(STsdbFileWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlockInfo,
                                    int8_t cmprAlg) {
  int32_t code = 0;
  int32_t lino = 0;

_exit:
  if (code) {
  } else {
  }
  return code;
#if 0
  int32_t code = 0;

  ASSERT(pBlockData->nRow > 0);

  if (toLast) {
    pBlkInfo->offset = pWriter->fStt[pWriter->wSet.nSttF - 1].size;
  } else {
    pBlkInfo->offset = pWriter->fData.size;
  }
  pBlkInfo->szBlock = 0;
  pBlkInfo->szKey = 0;

  int32_t aBufN[4] = {0};
  code = tCmprBlockData(pBlockData, cmprAlg, NULL, NULL, pWriter->aBuf, aBufN);
  if (code) goto _err;

  // write =================
  STsdbFD *pFD = toLast ? pWriter->pSttFD : pWriter->pDataFD;

  pBlkInfo->szKey = aBufN[3] + aBufN[2];
  pBlkInfo->szBlock = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];

  int64_t offset = pBlkInfo->offset;
  code = tsdbWriteFile(pFD, offset, pWriter->aBuf[3], aBufN[3]);
  if (code) goto _err;
  offset += aBufN[3];

  code = tsdbWriteFile(pFD, offset, pWriter->aBuf[2], aBufN[2]);
  if (code) goto _err;
  offset += aBufN[2];

  if (aBufN[1]) {
    code = tsdbWriteFile(pFD, offset, pWriter->aBuf[1], aBufN[1]);
    if (code) goto _err;
    offset += aBufN[1];
  }

  if (aBufN[0]) {
    code = tsdbWriteFile(pFD, offset, pWriter->aBuf[0], aBufN[0]);
    if (code) goto _err;
  }

  // update info
  if (toLast) {
    pWriter->fStt[pWriter->wSet.nSttF - 1].size += pBlkInfo->szBlock;
  } else {
    pWriter->fData.size += pBlkInfo->szBlock;
  }

  // ================= SMA ====================
  if (pSmaInfo) {
    code = tsdbWriteBlockSma(pWriter, pBlockData, pSmaInfo);
    if (code) goto _err;
  }

_exit:
  tsdbTrace("vgId:%d, tsdb write block data, suid:%" PRId64 " uid:%" PRId64 " nRow:%d, offset:%" PRId64 " size:%d",
            TD_VID(pWriter->pTsdb->pVnode), pBlockData->suid, pBlockData->uid, pBlockData->nRow, pBlkInfo->offset,
            pBlkInfo->szBlock);
  return code;

_err:
  tsdbError("vgId:%d, tsdb write block data failed since %s", TD_VID(pWriter->pTsdb->pVnode), tstrerror(code));
  return code;
#endif
}

// FLUSH MEMTABLE TO FILE SYSTEM ===================================
typedef struct {
  // configs
  STsdb  *pTsdb;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int8_t  sttTrigger;

  SArray *aTbDataP;
  SArray *aFileOpP;  // SArray<STsdbFileOp *>

  // time-series data
  int32_t          fid;
  TSKEY            minKey;
  TSKEY            maxKey;
  STsdbFileWriter *pWriter;
  SArray          *aSttBlk;
  SBlockData       bData;
  int32_t          iTbData;
  SSkmInfo         skmTable;
  SSkmInfo         skmRow;
  // tomestone data
} STsdbFlusher;

static int32_t tsdbFlusherInit(STsdb *pTsdb, STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = pTsdb->pVnode;

  pFlusher->pTsdb = pTsdb;
  pFlusher->minutes = pTsdb->keepCfg.days;
  pFlusher->precision = pTsdb->keepCfg.precision;
  pFlusher->minRow = pVnode->config.tsdbCfg.minRows;
  pFlusher->maxRow = pVnode->config.tsdbCfg.maxRows;
  pFlusher->cmprAlg = pVnode->config.tsdbCfg.compression;
  pFlusher->sttTrigger = pVnode->config.sttTrigger;

  pFlusher->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (NULL == pFlusher->aTbDataP) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFlusher->aFileOpP = taosArrayInit(0, sizeof(STsdbFileOp *));
  if (NULL == pFlusher->aFileOpP) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pFlusher->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static void tsdbFlusherClear(STsdbFlusher *pFlusher) {
  tBlockDataDestroy(&pFlusher->bData, 1);

  if (pFlusher->aSttBlk) {
    taosArrayDestroy(pFlusher->aSttBlk);
    pFlusher->aSttBlk = NULL;
  }
  if (pFlusher->aFileOpP) {
    taosArrayDestroyEx(pFlusher->aFileOpP, (FDelete)tsdbFileOpDestroy);
    pFlusher->aFileOpP = NULL;
  }

  if (pFlusher->aTbDataP) {
    taosArrayDestroy(pFlusher->aTbDataP);
    pFlusher->aTbDataP = NULL;
  }
}

static int32_t tsdbUpdateSkmInfo(SMeta *pMeta, int64_t suid, int64_t uid, int32_t sver, SSkmInfo *pSkmInfo) {
  int32_t code = 0;

  if (!TABLE_SAME_SCHEMA(suid, uid, pSkmInfo->suid, pSkmInfo->uid)   // not same schema
      || (pSkmInfo->pTSchema == NULL)                                // schema not created
      || (sver > 0 && pSkmInfo->pTSchema->version != sver /*todo*/)  // not same version
  ) {
    pSkmInfo->suid = suid;
    pSkmInfo->uid = uid;
    if (pSkmInfo->pTSchema) {
      tTSchemaDestroy(pSkmInfo->pTSchema);
    }
    code = metaGetTbTSchemaEx(pMeta, suid, uid, sver, &pSkmInfo->pTSchema);
  }

_exit:
  return code;
}

static int32_t tsdbFlushBlockData(STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;

  if (0 == pFlusher->bData.nRow) return code;

  SBlockData *pBlockData = &pFlusher->bData;
  SSttBlk     sttBlk = {0};

  sttBlk.suid = pBlockData->suid;
  sttBlk.nRow = pBlockData->nRow;
  sttBlk.minKey = TSKEY_MAX;
  sttBlk.maxKey = TSKEY_MIN;
  sttBlk.minVer = VERSION_MAX;
  sttBlk.maxVer = VERSION_MIN;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    sttBlk.minKey = TMIN(sttBlk.minKey, pBlockData->aTSKEY[iRow]);
    sttBlk.maxKey = TMAX(sttBlk.maxKey, pBlockData->aTSKEY[iRow]);
    sttBlk.minVer = TMIN(sttBlk.minVer, pBlockData->aVersion[iRow]);
    sttBlk.maxVer = TMAX(sttBlk.maxVer, pBlockData->aVersion[iRow]);
  }
  sttBlk.minUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[0];
  sttBlk.maxUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[pBlockData->nRow - 1];

  // write
  code = tsdbWriteBlockDataEx(pFlusher->pWriter, pBlockData, &sttBlk.bInfo, pFlusher->cmprAlg);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (NULL == taosArrayPush(pFlusher->aSttBlk, &sttBlk)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataClear(&pFlusher->bData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pFlusher->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pFlusher->pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbFlushTableTimeSeriesData(STsdbFlusher *pFlusher, TSKEY *nextKey) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  STbData    *pTbData = (STbData *)taosArrayGetP(pFlusher->aTbDataP, pFlusher->iTbData);
  STbDataIter iter = {0};
  int64_t     nRows = 0;
  TABLEID     id = {.suid = pTbData->suid, .uid = pTbData->uid};
  TSDBKEY     fromKey = {.version = VERSION_MIN, .ts = pFlusher->minKey};
  SMeta      *pMeta = pTsdb->pVnode->pMeta;

  code = tsdbUpdateSkmInfo(pMeta, pTbData->suid, pTbData->uid, -1, &pFlusher->skmTable);
  TSDB_CHECK_CODE(code, lino, _exit);

  // check if need to flush the data (todo)
  if (!TABLE_SAME_SCHEMA(pFlusher->bData.suid, pFlusher->bData.uid, pTbData->suid, pTbData->uid)) {
    code = tsdbFlushBlockData(pFlusher);
    TSDB_CHECK_CODE(code, lino, _exit);

    tBlockDataReset(&pFlusher->bData);
  }

  // init the block data (todo)
  if (!TABLE_SAME_SCHEMA(pTbData->suid, pTbData->uid, pFlusher->bData.suid, pFlusher->bData.uid)) {
    code = tBlockDataInit(&pFlusher->bData, &id, pFlusher->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tsdbTbDataIterOpen(pTbData, &fromKey, 0, &iter);
  for (;;) {
    TSDBROW *pRow = tsdbTbDataIterGet(&iter);
    if (NULL == pRow) {
      *nextKey = TSKEY_MAX;
      break;
    } else if (TSDBROW_TS(pRow) > pFlusher->maxKey) {
      *nextKey = TSDBROW_TS(pRow);
      break;
    }

    nRows++;

    code = tsdbUpdateSkmInfo(pMeta, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow), &pFlusher->skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tBlockDataAppendRow(&pFlusher->bData, pRow, pFlusher->skmRow.pTSchema, pTbData->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    tsdbTbDataIterNext(&iter);

    if (pFlusher->bData.nRow >= pFlusher->maxRow) {
      code = tsdbFlushBlockData(pFlusher);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d suid:%" PRId64 " uid:%" PRId64, TD_VID(pTsdb->pVnode),
              __func__, lino, tstrerror(code), pFlusher->fid, pTbData->suid, pTbData->uid);
  } else {
    tsdbTrace("vgId:%d %s done, fid:%d suid:%" PRId64 " uid:%" PRId64 " nRows:%" PRId64, TD_VID(pTsdb->pVnode),
              __func__, pFlusher->fid, pTbData->suid, pTbData->uid, nRows);
  }
  return code;
}

static int32_t tsdbFlushFileTimeSeriesData(STsdbFlusher *pFlusher, TSKEY *nextKey) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  // prepare and set state (todo)
  pFlusher->fid = tsdbKeyFid(*nextKey, pFlusher->minutes, pFlusher->precision);
  tsdbFidKeyRange(pFlusher->fid, pFlusher->minutes, pFlusher->precision, &pFlusher->minKey, &pFlusher->maxKey);
  if ((NULL == pFlusher->aSttBlk) && ((pFlusher->aSttBlk = taosArrayInit(0, sizeof(SSttBlk))) == NULL)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    taosArrayClear(pFlusher->aSttBlk);
  }
  tBlockDataReset(&pFlusher->bData);

  // create/open file to write
  STsdbFileOp *pFileOp = NULL;
  STsdbFile    file = {
         .ftype = TSDB_FTYPE_STT,  //
         .did = {0},               // todo
         .fid = pFlusher->fid,     //
         .id = 0,                  // todo
  };
  code = tsdbFileOpCreate(TSDB_FOP_ADD, &file, &pFileOp);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (NULL == taosArrayPush(pFlusher->aFileOpP, &pFileOp)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFileWriterOpen(pTsdb, &pFileOp->file, &pFlusher->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop to flush table time-series data
  *nextKey = TSKEY_MAX;
  for (pFlusher->iTbData = 0; pFlusher->iTbData < taosArrayGetSize(pFlusher->aTbDataP); pFlusher->iTbData++) {
    TSKEY nextTbKey;

    code = tsdbFlushTableTimeSeriesData(pFlusher, &nextTbKey);
    TSDB_CHECK_CODE(code, lino, _exit);

    *nextKey = TMIN(*nextKey, nextTbKey);
  }

  // flush remain data
  if (pFlusher->bData.nRow > 0) {
    code = tsdbFlushBlockData(pFlusher);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // close filas
  code = tsdbFileWriterClose(&pFlusher->pWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              pFlusher->fid);
    {
      // TODO: clear/rollback
    }
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(pTsdb->pVnode), __func__, pFlusher->fid);
  }
  return code;
}

static int32_t tsdbFlushTimeSeriesData(STsdbFlusher *pFlusher) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STsdb     *pTsdb = pFlusher->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  TSKEY nextKey = pMemTable->minKey;
  while (nextKey < TSKEY_MAX) {
    code = tsdbFlushFileTimeSeriesData(pFlusher, &nextKey);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, nRows:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pMemTable->nRow);
  }
  return code;
}

static int32_t tsdbFlushDelData(STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  // TODO
  ASSERT(0);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbFlush(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  // check ----
  SMemTable *pMemTable = pTsdb->mem;
  if (0 == pMemTable->nRow && 0 == pMemTable->nDel) {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->mem = NULL;
    taosThreadRwlockUnlock(&pTsdb->rwLock);

    tsdbUnrefMemTable(pMemTable);
    return code;
  } else {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->mem = NULL;
    pTsdb->imem = pMemTable;
    taosThreadRwlockUnlock(&pTsdb->rwLock);
  }

  // flush ----
  STsdbFlusher flusher = {0};

  code = tsdbFlusherInit(pTsdb, &flusher);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(taosArrayGetSize(flusher.aTbDataP) > 0);

  if (pMemTable->nRow > 0) {
    code = tsdbFlushTimeSeriesData(&flusher);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pMemTable->nDel > 0) {
    code = tsdbFlushDelData(&flusher);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // apply change
  // code = tsdbFileSystemCommit1(pTsdb, flusher.aFileOpP);
  // TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    // todo (do some rollback)
  }
  tsdbFlusherClear(&flusher);
  return code;
}

// MERGE MULTIPLE STT ===================================
typedef struct {
  STsdb *pTsdb;
  // data
} STsdbMerger;

static int32_t tsdbMergerInit(STsdb *pTsdb, STsdbMerger **ppMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbMerger *pMerger = (STsdbMerger *)taosMemoryCalloc(1, sizeof(*pMerger));
  if (NULL == pMerger) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pMerger->pTsdb = pTsdb;
  // todo
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    *ppMerger = NULL;
  } else {
  }
  return code;
}

static void tsdbMergerClear(STsdbMerger *pMerger) {
  if (pMerger) {
    taosMemoryFree(pMerger);
  }
}

static int32_t tsdbMergeFileGroup(STsdbMerger *pMerger, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pMerger->pTsdb;
  // TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(pTsdb->pVnode), __func__, fid);
  }
  return code;
}

int32_t tsdbMerge(STsdb *pTsdb, int32_t *aFid, int32_t nFid) {
  int32_t code = 0;
  int32_t lino = 0;

  // init merger (todo)
  STsdbMerger *pMerger = NULL;
  code = tsdbMergerInit(pTsdb, &pMerger);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop to merge
  for (int32_t iFid = 0; iFid < nFid; iFid++) {
    code = tsdbMergeFileGroup(pMerger, aFid[iFid]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // commit file change (todo)
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  tsdbMergerClear(pMerger);
  return code;
}

// TRANSACTION CONTROL ===================================