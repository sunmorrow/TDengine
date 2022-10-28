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

#define TSDB_FTYPE_HEAD 0  // .head
#define TSDB_FTYPE_DATA 1  // .data
#define TSDB_FTYPE_SMA  2  // .sma
#define TSDB_FTYPE_STT  3  // .stt
#define TSDB_FTYPE_DEL  4  // .del
#define TSDB_FTYPE_MAX  5

typedef struct TSDBFILE        TSDBFILE;
typedef struct STsdbFile       STsdbFile;
typedef struct STsdbFileOp     STsdbFileOp;
typedef struct STsdbFileWriter STsdbFileWriter;
typedef struct STsdbFileObj    STsdbFileObj;
typedef struct STsdbFileGroup  STsdbFileGroup;
typedef struct STsdbFileSystem STsdbFileSystem;

// TSDBFILE ======================================================
struct TSDBFILE {
  char     *path;
  int32_t   szPage;
  int32_t   flags;
  TdFilePtr pFD;
  int64_t   pgno;
  uint8_t  *pBuf;
  int64_t   szFile;
};

// STsdbFile ======================================================
struct STsdbFile {
  int32_t ftype;
  SDiskID did;
  int32_t fid;
  int64_t id;
  int64_t size;
  int64_t offset;
};

bool tsdbIsSameFile(const STsdbFile *pFile1, const STsdbFile *pFile2);

// STsdbFileOp ======================================================
typedef enum {
  TSDB_FILE_ADD = 0, /* ADD FILE */
  TSDB_FILE_REMOVE,  /* REMOVE FILE */
  TSDB_FILE_MOD      /* MODIFY FILE */
} ETsdbFileOpT;
struct STsdbFileOp {
  ETsdbFileOpT op;
  STsdbFile    file;
};

// STsdbFileWriter ======================================================
struct STsdbFileWriter {
  TSDBFILE *pFILE;
  STsdbFile file;
};

int32_t tsdbFileWriterOpen(STsdbFile *pFile, STsdbFileWriter **ppWriter);
int32_t tsdbFileWriterClose(STsdbFileWriter **ppWriter);

// STsdbFileObj ======================================================
struct STsdbFileObj {
  volatile int32_t nRef;
  SRBTreeNode      rbtn;
  STsdbFile        file;
};

#define RBTN_TO_FILE_OBJ(PNODE) ((STsdbFileObj *)(((uint8_t *)PNODE) - offsetof(STsdbFileObj, rbtn)))

// STsdbFileGroup ======================================================
struct STsdbFileGroup {
  int32_t       fid;
  STsdbFileObj *fHead;
  STsdbFileObj *fData;
  STsdbFileObj *fSma;
  SRBTree       fStt;
  SRBTreeNode   rbtn;
};

#define RBTN_TO_FILE_GROUP(PNODE) ((STsdbFileGroup *)(((uint8_t *)PNODE) - offsetof(STsdbFileGroup, rbtn)))

// STsdbFileSystem ======================================================
struct STsdbFileSystem {
  int64_t       id;
  STsdbFileObj *fDel;
  SRBTree       fGroup;  // SArray<STsdbFileGroup>
};

int32_t tsdbOpenFileSystem(STsdb *pTsdb, int8_t rollback);
int32_t tsdbCloseFileSystem(STsdb *pTsdb);