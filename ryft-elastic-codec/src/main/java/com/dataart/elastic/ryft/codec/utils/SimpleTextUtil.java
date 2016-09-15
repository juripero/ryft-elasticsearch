package com.dataart.elastic.ryft.codec.utils;

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