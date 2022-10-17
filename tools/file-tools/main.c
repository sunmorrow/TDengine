#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "tsdb.h"

const int32_t fid = 1927;
const char*   fDel = NULL;
const char*   fHead = "/root/workspace/Download/vnode350/tsdb/v350f1927ver200.head";
const char*   fData = "/root/workspace/Download/vnode350/tsdb/v350f1927ver1.data";
const char*   fSma = "/root/workspace/Download/vnode350/tsdb/v350f1927ver1.sma";
const char*   fStt[] = {"/root/workspace/Download/vnode350/tsdb/v350f1927ver193.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver194.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver195.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver196.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver197.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver198.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver199.stt",
                        "/root/workspace/Download/vnode350/tsdb/v350f1927ver200.stt"};

int main(int argc, char const* argv[]) {
  int     nStt = sizeof(fStt) / sizeof(fStt[0]);
  uint8_t buf[512] = {0};

  STsdbFS fs = {0};

  fs.aDFileSet = taosArrayInit(0, sizeof(SDFileSet));

  // SDelFile
  if (fDel) {
    fs.pDelFile = (SDelFile*)taosMemoryCalloc(1, sizeof(SDelFile));

    TdFilePtr pFD = taosOpenFile(fDel, O_RDONLY);

    taosReadFile(pFD, buf, 512);

    tGetDelFile(buf, fs.pDelFile);

    taosCloseFile(&pFD);
  } else {
    fs.pDelFile = NULL;
  }

  SDFileSet fSet = {.fid = fid};

  // SHeadFile
  {
    fSet.pHeadF = (SHeadFile*)taosMemoryCalloc(1, sizeof(SHeadFile));

    TdFilePtr pFD = taosOpenFile(fHead, O_RDONLY);
    taosReadFile(pFD, buf, 512);
    tGetHeadFile(buf, fSet.pHeadF);
    taosCloseFile(&pFD);
  }

  // SDataFile
  {
    fSet.pDataF = (SDataFile*)taosMemoryCalloc(1, sizeof(SDataFile));

    TdFilePtr pFD = taosOpenFile(fData, O_RDONLY);
    taosReadFile(pFD, buf, 512);
    tGetDataFile(buf, fSet.pDataF);
    taosCloseFile(&pFD);
  }

  // SSmaFile
  {
    fSet.pSmaF = (SSmaFile*)taosMemoryCalloc(1, sizeof(SSmaFile));

    TdFilePtr pFD = taosOpenFile(fSma, O_RDONLY);
    taosReadFile(pFD, buf, 512);
    tGetSmaFile(buf, fSet.pSmaF);
    taosCloseFile(&pFD);
  }

  for (int32_t iStt = 0; iStt < nStt; iStt++) {
    fSet.nSttF++;
    fSet.aSttF[iStt] = (SSttFile*)taosMemoryCalloc(1, sizeof(SSttFile));

    TdFilePtr pFD = taosOpenFile(fStt[iStt], O_RDONLY);
    taosReadFile(pFD, buf, 512);
    tGetSttFile(buf, fSet.aSttF[iStt]);
    taosCloseFile(&pFD);
  }

  taosArrayPush(fs.aDFileSet, &fSet);

  {
    int32_t   flags = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
    TdFilePtr pFD = taosOpenFile("CURRENT", flags);

    int32_t n = tsdbEncodeFS(buf, &fs);
    taosCalcChecksumAppend(0, buf, n + sizeof(TSCKSUM));

    taosWriteFile(pFD, buf, n + sizeof(TSCKSUM));

    taosFsyncFile(pFD);

    taosCloseFile(&pFD);
  }

  return 0;
}
