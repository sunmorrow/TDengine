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

typedef struct TSDBFILE        TSDBFILE;
typedef struct STsdbFile       STsdbFile;
typedef struct STsdbFileOp     STsdbFileOp;
typedef struct STsdbFileWriter STsdbFileWriter;
typedef struct STsdbFileObj    STsdbFileObj;
typedef struct STsdbFileGroup  STsdbFileGroup;
typedef struct STsdbFileSystem STsdbFileSystem;

/* TSDBFILE */
struct STsdbFile {
  int32_t ftype;
  SDiskID did;
  int32_t fid;
  int64_t id;
  int64_t size;
  int64_t offset;
};

/* STsdbFile */
bool tsdbIsSameFile(const STsdbFile *pFile1, const STsdbFile *pFile2);

/* STsdbFileOp */
struct STsdbFileOp {
  int32_t   op;
  STsdbFile file;
};

/* STsdbFileWriter */
int32_t tsdbFileWriterOpen(STsdbFile *pFile, STsdbFileWriter **ppWriter);
int32_t tsdbFileWriterClose(STsdbFileWriter **ppWriter);

/* STsdbFileObj */

/* STsdbFileGroup */

/* STsdbFileSystem */
int32_t tsdbOpenFileSystem(STsdb *pTsdb, int8_t rollback);
int32_t tsdbCloseFileSystem(STsdb *pTsdb);