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

// SDataFWriter
int32_t tsdbDataFWriterOpen(SDataFWriter **ppWriter, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFWriterClose(SDataFWriter **ppWriter, int8_t sync);
int32_t tsdbUpdateDFileSetHeader(SDataFWriter *pWriter);
int32_t tsdbWriteBlockIdx(SDataFWriter *pWriter, SArray *aBlockIdx);
int32_t tsdbWriteDataBlk(SDataFWriter *pWriter, SMapData *mDataBlk, SBlockIdx *pBlockIdx);
int32_t tsdbWriteSttBlk(SDataFWriter *pWriter, SArray *aSttBlk);
int32_t tsdbWriteBlockData(SDataFWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlkInfo, SSmaInfo *pSmaInfo,
                           int8_t cmprAlg, int8_t toLast);
int32_t tsdbWriteDiskData(SDataFWriter *pWriter, const SDiskData *pDiskData, SBlockInfo *pBlkInfo, SSmaInfo *pSmaInfo);

int32_t tsdbDFileSetCopy(STsdb *pTsdb, SDFileSet *pSetFrom, SDFileSet *pSetTo);
// SDataFReader
int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet);
int32_t tsdbDataFReaderClose(SDataFReader **ppReader);
int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx);
int32_t tsdbReadDataBlk(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mDataBlk);
int32_t tsdbReadSttBlk(SDataFReader *pReader, int32_t iStt, SArray *aSttBlk);
int32_t tsdbReadBlockSma(SDataFReader *pReader, SDataBlk *pBlock, SArray *aColumnDataAgg);
int32_t tsdbReadDataBlock(SDataFReader *pReader, SDataBlk *pBlock, SBlockData *pBlockData);
int32_t tsdbReadSttBlock(SDataFReader *pReader, int32_t iStt, SSttBlk *pSttBlk, SBlockData *pBlockData);
int32_t tsdbReadSttBlockEx(SDataFReader *pReader, int32_t iStt, SSttBlk *pSttBlk, SBlockData *pBlockData);
// SDelFWriter
int32_t tsdbDelFWriterOpen(SDelFWriter **ppWriter, SDelFile *pFile, STsdb *pTsdb);
int32_t tsdbDelFWriterClose(SDelFWriter **ppWriter, int8_t sync);
int32_t tsdbWriteDelData(SDelFWriter *pWriter, SArray *aDelData, SDelIdx *pDelIdx);
int32_t tsdbWriteDelIdx(SDelFWriter *pWriter, SArray *aDelIdx);
int32_t tsdbUpdateDelFileHdr(SDelFWriter *pWriter);
// SDelFReader
int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb);
int32_t tsdbDelFReaderClose(SDelFReader **ppReader);
int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData);
int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx);