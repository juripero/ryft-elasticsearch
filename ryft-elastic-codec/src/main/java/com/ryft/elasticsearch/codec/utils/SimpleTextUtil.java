/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.ryft.elasticsearch.codec.utils;

import java.io.IOException;
import java.util.Locale;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

public class SimpleTextUtil {
  public final static byte NEWLINE = 10;
  public final static byte ESCAPE = 92;
  final static BytesRef CHECKSUM = new BytesRef("checksum ");
  
  public static void write(DataOutput out, String s, BytesRefBuilder scratch) throws IOException {
    scratch.copyChars(s, 0, s.length());
    write(out, scratch.get());
  }

  public static void write(DataOutput out, BytesRef b) throws IOException {
    for(int i=0;i<b.length;i++) {
      final byte bx = b.bytes[b.offset+i];
      if (bx == NEWLINE || bx == ESCAPE) {
        out.writeByte(ESCAPE);
      }
      out.writeByte(bx);
    }
  }

  public static void writeNewline(DataOutput out) throws IOException {
    out.writeByte(NEWLINE);
  }
  
  public static void readLine(DataInput in, BytesRefBuilder scratch) throws IOException {
    int upto = 0;
    while(true) {
      byte b = in.readByte();
      scratch.grow(1+upto);
      if (b == ESCAPE) {
        scratch.setByteAt(upto++, in.readByte());
      } else {
        if (b == NEWLINE) {
          break;
        } else {
          scratch.setByteAt(upto++, b);
        }
      }
    }
    scratch.setLength(upto);
  }

  public static void writeChecksum(IndexOutput out, BytesRefBuilder scratch) throws IOException {
    // Pad with zeros so different checksum values use the
    // same number of bytes
    // (BaseIndexFileFormatTestCase.testMergeStability cares):
    String checksum = String.format(Locale.ROOT, "%020d", out.getChecksum());
    SimpleTextUtil.write(out, CHECKSUM);
    SimpleTextUtil.write(out, checksum, scratch);
    SimpleTextUtil.writeNewline(out);
  }
  
  public static void checkFooter(ChecksumIndexInput input) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    String expectedChecksum = String.format(Locale.ROOT, "%020d", input.getChecksum());
    SimpleTextUtil.readLine(input, scratch);
    if (StringHelper.startsWith(scratch.get(), CHECKSUM) == false) {
      throw new CorruptIndexException("SimpleText failure: expected checksum line but got " + scratch.get().utf8ToString(), input);
    }
    String actualChecksum = new BytesRef(scratch.bytes(), CHECKSUM.length, scratch.length() - CHECKSUM.length).utf8ToString();
    if (!expectedChecksum.equals(actualChecksum)) {
      throw new CorruptIndexException("SimpleText checksum failure: " + actualChecksum + " != " + expectedChecksum, input);
    }
    if (input.length() != input.getFilePointer()) {
      throw new CorruptIndexException("Unexpected stuff at the end of file, please be careful with your text editor!", input);
    }
  }
}