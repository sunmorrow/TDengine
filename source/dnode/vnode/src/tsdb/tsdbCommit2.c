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
  // data
} STsdbCommitter;

int32_t tsdbFlushMemTable(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
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