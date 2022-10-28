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

int32_t tsdbFSOpen(STsdb *pTsdb, int8_t rollback);
int32_t tsdbFSClose(STsdb *pTsdb);
int32_t tsdbFSCopy(STsdb *pTsdb, STsdbFS *pFS);
void    tsdbFSDestroy(STsdbFS *pFS);
int32_t tDFileSetCmprFn(const void *p1, const void *p2);
int32_t tsdbFSCommit(STsdb *pTsdb);
int32_t tsdbFSRollback(STsdb *pTsdb);
int32_t tsdbFSPrepareCommit(STsdb *pTsdb, STsdbFS *pFS);
int32_t tsdbFSRef(STsdb *pTsdb, STsdbFS *pFS);
void    tsdbFSUnref(STsdb *pTsdb, STsdbFS *pFS);

int32_t tsdbFSUpsertFSet(STsdbFS *pFS, SDFileSet *pSet);
int32_t tsdbFSUpsertDelFile(STsdbFS *pFS, SDelFile *pDelFile);