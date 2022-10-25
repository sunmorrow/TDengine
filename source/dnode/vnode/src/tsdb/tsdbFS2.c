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

typedef volatile int32_t vi32_t;

#define TSDB_FTYPE_HEAD 0  // .head
#define TSDB_FTYPE_DATA 1  // .data
#define TSDB_FTYPE_SMA  2  // .sma
#define TSDB_FTYPE_STT  3  // .stt
#define TSDB_FTYPE_DEL  4  // .del
#define TSDB_FTYPE_MAX  5

typedef struct {
  int32_t ftype;
  SDiskID did;
  int32_t fid;
  int64_t id;
  int64_t size;
  int64_t offset;
} STsdbFile;

typedef struct {
  vi32_t      nRef;
  SRBTreeNode rbtn;
  STsdbFile   file;
} STsdbFileObj;

typedef struct {
  int32_t       fid;
  STsdbFileObj *fHead;
  STsdbFileObj *fData;
  STsdbFileObj *fSma;
  SRBTree       fStt;
  SRBTreeNode   rbtn;
} STsdbFileGroup;

typedef struct {
  int64_t       id;
  STsdbFileObj *fDel;
  SRBTree       fGroup;  // SArray<STsdbFileGroup>
} STsdbFileSystem;

// STsdbFile ==========================================
static void tsdbFileName(STsdb *pTsdb, const STsdbFile *pFile, char fName[]) {
  char   *fSuffix = NULL;
  SVnode *pVnode = pTsdb->pVnode;

  switch (pFile->ftype) {
    case TSDB_FTYPE_HEAD:
      fSuffix = ".head";
      break;
    case TSDB_FTYPE_DATA:
      fSuffix = ".data";
      break;
    case TSDB_FTYPE_SMA:
      fSuffix = ".sma";
      break;
    case TSDB_FTYPE_STT:
      fSuffix = ".stt";
      break;
    case TSDB_FTYPE_DEL:
      fSuffix = ".del";
      break;
    default:
      assert(0);
  }

  if (pVnode->pTfs) {
    snprintf(fName, TSDB_FILENAME_LEN, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pVnode->pTfs, pFile->did),
             TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pFile->fid, pFile->id, fSuffix);
  } else {
    snprintf(fName, TSDB_FILENAME_LEN, "%s%sv%df%dver%" PRId64 "%s", pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode),
             pFile->fid, pFile->id, fSuffix);
  }
}

static int32_t tsdbFileToBinary(uint8_t *p, STsdbFile *pFile) {
  int32_t n = 0;

  n += tPutI32(p ? p + n : p, pFile->ftype);
  n += tPutI32(p ? p + n : p, pFile->did.level);
  n += tPutI32(p ? p + n : p, pFile->did.id);
  n += tPutI32(p ? p + n : p, pFile->fid);
  n += tPutI64(p ? p + n : p, pFile->id);
  n += tPutI64(p ? p + n : p, pFile->size);
  n += tPutI64(p ? p + n : p, pFile->offset);

  return n;
}

static int32_t tsdbBinaryToFile(uint8_t *p, STsdbFile *pFile) {
  int32_t n = 0;

  n += tGetI32(p + n, &pFile->ftype);
  n += tGetI32(p + n, &pFile->did.level);
  n += tGetI32(p + n, &pFile->did.id);
  n += tGetI32(p + n, &pFile->fid);
  n += tGetI64(p + n, &pFile->id);
  n += tGetI64(p + n, &pFile->size);
  n += tGetI64(p + n, &pFile->offset);

  return n;
}

static int32_t tsdbFileToJson(STsdbFile *pFile) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

static int32_t tsdbJsonToFile(STsdbFile *pFile) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

// STsdbFileObj ==========================================
static int32_t tsdbNewFileObj(STsdb *pTsdb, STsdbFile *pFile, STsdbFileObj **ppFileObj) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileObj *pFileObj = (STsdbFileObj *)taosMemoryCalloc(1, sizeof(*pFileObj));
  if (NULL == pFileObj) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFileObj->nRef = 1;
  pFileObj->file = *pFile;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    *ppFileObj = NULL;
  } else {
    *ppFileObj = pFileObj;
  }
  return code;
}

static void tsdbFreeFileObj(STsdbFileObj *pFileObj) {
  if (pFileObj) {
    taosMemoryFree(pFileObj);
  }
}

static int32_t tsdbRefFileObj(STsdb *pTsdb, STsdbFileObj *pFileObj) {
  int32_t nRef = atomic_fetch_add_32(&pFileObj->nRef, 1);
  ASSERT(nRef > 0);
  return nRef;
}

static int32_t tsdbUnrefFileObj(STsdb *pTsdb, STsdbFileObj *pFileObj) {
  int32_t nRef = atomic_sub_fetch_32(&pFileObj->nRef, 1);
  if (0 == nRef) {
    // remove file and obj
    char fName[TSDB_FILENAME_LEN] = {0};

    tsdbFileName(pTsdb, &pFileObj->file, fName);
    (void)taosRemoveFile(fName);
    tsdbFreeFileObj(pFileObj);

    tsdbDebug("vgId:%d %s, file:%s", TD_VID(pTsdb->pVnode), __func__, fName);
  }
  return nRef;
}

static int32_t tsdbSttFileCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  STsdbFileObj *pFileObj1 = (STsdbFileObj *)(((uint8_t *)p1) - offsetof(STsdbFileObj, rbtn));
  STsdbFileObj *pFileObj2 = (STsdbFileObj *)(((uint8_t *)p2) - offsetof(STsdbFileObj, rbtn));

  ASSERT(pFileObj1->file.ftype == TSDB_FTYPE_STT);
  ASSERT(pFileObj2->file.ftype == TSDB_FTYPE_STT);

  if (pFileObj1->file.id < pFileObj2->file.id) {
    return -1;
  } else if (pFileObj1->file.id > pFileObj2->file.id) {
    return 1;
  }

  return 0;
}

// STsdbFileGroup ==========================================
static int32_t tsdbFileGroupCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  STsdbFileGroup *pFileGroup1 = (STsdbFileGroup *)(((uint8_t *)p1) - offsetof(STsdbFileGroup, rbtn));
  STsdbFileGroup *pFileGroup2 = (STsdbFileGroup *)(((uint8_t *)p2) - offsetof(STsdbFileGroup, rbtn));

  if (pFileGroup1->fid < pFileGroup2->fid) {
    return -1;
  } else if (pFileGroup1->fid > pFileGroup2->fid) {
    return 1;
  }

  return 0;
}

// STsdbFileSystem ==========================================
static int32_t tsdbNewFileSystem(STsdbFileSystem **ppFileSystem) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileSystem *pFileSystem = (STsdbFileSystem *)taosMemoryCalloc(1, sizeof(*pFileSystem));
  if (NULL == pFileSystem) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tRBTreeCreate(&pFileSystem->fGroup, tsdbFileGroupCmprFn);

_exit:
  if (code) {
    *ppFileSystem = NULL;
  } else {
    *ppFileSystem = pFileSystem;
  }
  return code;
}

static void tsdbFreeFileSystem(STsdbFileSystem *pFileSystem) {
  if (pFileSystem) {
    taosMemoryFree(pFileSystem);
  }
}

static void tsdbCurrentFileName(STsdb *pTsdb, char current[], char current_t[]) {
  SVnode *pVnode = pTsdb->pVnode;
  if (pVnode->pTfs) {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
               pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT.t", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
               pTsdb->path, TD_DIRSEP);
    }
  } else {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%sCURRENT", pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%sCURRENT.t", pTsdb->path, TD_DIRSEP);
    }
  }
}

static int32_t tsdbSaveFileSystemToFile(STsdb *pTsdb, STsdbFileSystem *pFileSystem, const char *fName) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

static int32_t tsdbLoadFileSystemFromFile(STsdb *pTsdb, const char *fName, STsdbFileSystem *pFileSystem) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

static int32_t tsdbScanAndTryFixFileSystem(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

int32_t tsdbOpenFileSystem(STsdb *pTsdb, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;

  // create
  STsdbFileSystem **ppFileSystem = NULL;  // todo
  code = tsdbNewFileSystem(ppFileSystem);
  TSDB_CHECK_CODE(code, lino, _exit);

  // recover file system
  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbCurrentFileName(pTsdb, current, current_t);

  if (taosCheckExistFile(current)) {
    code = tsdbLoadFileSystemFromFile(pTsdb, current, *ppFileSystem);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(current_t)) {
      if (rollback) {
        // todo
      } else {
        // todo
      }
    }
  } else {
    ASSERT(!rollback);

    code = tsdbSaveFileSystemToFile(pTsdb, *ppFileSystem, current);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // scan and fix file system
  code = tsdbScanAndTryFixFileSystem(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbCloseFileSystem(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}