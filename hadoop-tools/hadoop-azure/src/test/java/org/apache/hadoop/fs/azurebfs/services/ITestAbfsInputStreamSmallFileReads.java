/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class ITestAbfsInputStreamSmallFileReads extends ITestAbfsInputStream {
  private static final int ONE_KB = 1024;
  private static final int ONE_MB = ONE_KB * ONE_KB;

  public ITestAbfsInputStreamSmallFileReads() throws Exception {
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    testNumBackendCalls(true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    testNumBackendCalls(false);
  }

  private void testNumBackendCalls(boolean readSmallFilesCompletely)
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        byte[] buffer = new byte[length];

        AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
            .getWrappedStream();
        abfsInputStream.setIsReadSmallFilesCompletelyEnabled(readSmallFilesCompletely);
        abfsInputStream.setIsOptimizeFooterReadEnabled(false);

        iStream.seek(seekPos(SeekTo.END, fileSize, length));
        iStream.read(buffer, 0, length);

        iStream.seek(seekPos(SeekTo.MIDDLE, fileSize, length));
        iStream.read(buffer, 0, length);

        iStream.seek(seekPos(SeekTo.BEGIN, fileSize, length));
        iStream.read(buffer, 0, length);

        if (readSmallFilesCompletely) {
          System.out.println("=================================== No. of Conn: 1 ============================================");
        } else {
          System.out.println("=================================== No. of Conn: 3 ============================================");
        }
      }
    }
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfTrue()
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, true);
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfFalse()
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, false);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, true);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, false);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, true);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, false);
  }

  @Test
  public void testSeekToEndAndReadBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, true);
  }

  @Test
  public void testSeekToEndAndReaBigFiledWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, false);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, true);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, false);
  }

  @Test
  public void testSeekToMiddleAndReaBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, true);
  }

  @Test
  public void testSeekToMiddleAndReadBigFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, false);
  }

  private void testSeekAndReadWithConf(SeekTo seekTo, int startFileSizeInMB,
      int endFileSizeInMB, boolean readSmallFilesCompletely) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    for (int i = startFileSizeInMB; i <= endFileSizeInMB; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      int seekPos = seekPos(seekTo, fileSize, length);
      seekReadAndTest(fs, testFilePath, seekPos, length, fileContent, readSmallFilesCompletely);
    }
  }

  private int seekPos(SeekTo seekTo, int fileSize, int length) {
    if (seekTo == SeekTo.BEGIN) {
      return 0;
    }
    if (seekTo == SeekTo.END) {
      return fileSize - length;
    }
    return fileSize / 2;
  }

  private void seekReadAndTest(FileSystem fs, Path testFilePath, int seekPos,
      int length, byte[] fileContent, boolean readSmallFilesCompletely)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AbfsConfiguration conf = getAbfsStore(fs).getAbfsConfiguration();
    try {
      FSDataInputStream iStream = fs.open(testFilePath);
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream.setIsReadSmallFilesCompletelyEnabled(readSmallFilesCompletely);
      abfsInputStream.setIsOptimizeFooterReadEnabled(false);
      iStream = new FSDataInputStream(abfsInputStream);

      seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      assertContentReadCorrectly(fileContent, seekPos, length, buffer);

      final int readBufferSize = conf.getReadBufferSize();
      final int fileContentLength = fileContent.length;
      final boolean smallFile = fileContentLength <= readBufferSize;
      int expectedLimit, expectedFCursor;
      int expectedBCursor;
      if (readSmallFilesCompletely && smallFile) {
        assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer(), conf);
        expectedFCursor = fileContentLength;
        expectedLimit = fileContentLength;
        expectedBCursor = seekPos + length;
      } else {
        if ((seekPos == 0)) {
          assertBuffersAreEqual(fileContent, abfsInputStream.getBuffer(), conf);
        } else {
          assertBuffersAreNotEqual(fileContent, abfsInputStream.getBuffer(),
              conf);
        }
        expectedBCursor = length;
        expectedFCursor = (fileContentLength < (seekPos + readBufferSize))
            ? fileContentLength
            : (seekPos + readBufferSize);
        expectedLimit = (fileContentLength < (seekPos + readBufferSize))
            ? (fileContentLength - seekPos)
            : readBufferSize;
      }
      assertEquals(expectedFCursor, abfsInputStream.getFCursor());
      assertEquals(expectedFCursor, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(expectedBCursor, abfsInputStream.getBCursor());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void testPartialReadWithNoData() throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem();
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithNoData(fs, testFilePath, fileSize / 2, fileSize / 4,
          fileContent);
    }
  }

  private void partialReadWithNoData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException {

    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream.setIsReadSmallFilesCompletelyEnabled(true);
      abfsInputStream = spy(abfsInputStream);
      doReturn(10)
          .doReturn(10)
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt());

      iStream = new FSDataInputStream(abfsInputStream);
      seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      assertContentReadCorrectly(fileContent, seekPos, length, buffer);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length,
          abfsInputStream.getFCursorAfterLastRead());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeData() throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem();
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      partialReadWithSomeData(fs, testFilePath, fileSize / 2,
          fileSize / 4, fileContent);
    }
  }

  private void partialReadWithSomeData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream.setIsReadSmallFilesCompletelyEnabled(true);
      abfsInputStream = spy(abfsInputStream);
      //  first readRemote, will return first 10 bytes
      //  second readRemote, seekPos - someDataLength(10) will reach the
      //  seekPos as 10 bytes are already read in the first call. Plus
      //  someDataLength(10)
      int someDataLength = 10;
      int secondReturnSize = seekPos - 10 + someDataLength;
      doReturn(10)
          .doReturn(secondReturnSize)
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt());

      iStream = new FSDataInputStream(abfsInputStream);
      seek(iStream, seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertTrue(abfsInputStream.getFCursor() > seekPos + length);
      assertTrue(abfsInputStream.getFCursorAfterLastRead() > seekPos + length);
      //  Optimized read was no complete but it got some user requested data
      //  from server. So obviously the buffer will contain data more than
      //  seekPos + len
      assertEquals(length - someDataLength, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() > length - someDataLength);
    } finally {
      iStream.close();
    }
  }

  private enum SeekTo {BEGIN, MIDDLE, END}

}