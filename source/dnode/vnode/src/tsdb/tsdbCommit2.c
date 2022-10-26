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

// FLUSH MEMTABLE TO FILE SYSTEM ===================================
typedef struct {
  STsdb *pTsdb;

  SArray *aTbDataP;
  TSKEY   nextKey;

  int32_t    fid;
  TSKEY      minKey;
  TSKEY      maxKey;
  SArray    *aSttBlk;
  SBlockData aBData[2];
  int32_t    iTbData;
} STsdbFlusher;

static int32_t tsdbFlusherInit(STsdb *pTsdb, STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;

  pFlusher->pTsdb = pTsdb;
  // TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static void tsdbFlusherClear(STsdbFlusher *pFlusher) {
  // todo
}

static int32_t tsdbFlushTableTimeSeriesData(STsdbFlusher *pFlusher, TSKEY *nextKey) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  STbData    *pTbData = (STbData *)taosArrayGetP(pFlusher->aTbDataP, pFlusher->iTbData);
  STbDataIter iter = {0};
  TSDBKEY     fromKey = {.version = VERSION_MIN, .ts = pFlusher->minKey};

  // check if need to flush the data (todo)

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

    // code = tsdbUpdateRowSchema();
    // TSDB_CHECK_CODE(code, lino, _exit);

    // code = tBlockDataAppendRow();
    // TSDB_CHECK_CODE(code, lino, _exit);

    tsdbTbDataIterNext(&iter);

    // if (BLOCk_DATA_NROWS() >= pFlusher->maxRows) {
    // flush the block and clear the block data
    // }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d suid:%" PRId64 " uid:%" PRId64, TD_VID(pTsdb->pVnode),
              __func__, lino, tstrerror(code), pFlusher->fid, pTbData->suid, pTbData->uid);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d suid:%" PRId64 " uid:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pFlusher->fid,
              pTbData->suid, pTbData->uid);
  }
  return code;
}

static int32_t tsdbFlushFileTimeSeriesData(STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  // prepare by setting state
  // pFlusher->fid = tsdbKeyFid();
  // tsdbFidKeyRange(pFlusher->fid, );
  pFlusher->nextKey = TSKEY_MAX;

  // create/open file to write (todo)

  // loop to commit
  for (pFlusher->iTbData = 0; pFlusher->iTbData < taosArrayGetSize(pFlusher->aTbDataP); pFlusher->iTbData++) {
    TSKEY nextKey = TSKEY_MAX;

    code = tsdbFlushTableTimeSeriesData(pFlusher, &nextKey);
    TSDB_CHECK_CODE(code, lino, _exit);

    pFlusher->nextKey = TMIN(pFlusher->nextKey, nextKey);
  }

  // close file (todo)

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              pFlusher->fid);
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

  if (0 == pMemTable->nRow) {
    return code;
  }

  pFlusher->nextKey = pMemTable->minKey;
  while (pFlusher->nextKey < TSKEY_MAX) {
    code = tsdbFlushFileTimeSeriesData(pFlusher);
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
  int32_t    code = 0;
  int32_t    lino = 0;
  STsdb     *pTsdb = pFlusher->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  if (0 == pMemTable->nDel) {
    return code;
  }

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, nDel:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pMemTable->nDel);
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
  }

  // flush ----
  STsdbFlusher flusher = {0};

  code = tsdbFlusherInit(pTsdb, &flusher);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFlushTimeSeriesData(&flusher);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFlushDelData(&flusher);
  TSDB_CHECK_CODE(code, lino, _exit);

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