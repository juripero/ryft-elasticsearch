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
package com.ryft.elasticsearch.codec;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 *
 * @author imasternoy
 */
public class RyftStoredFieldsWriter extends StoredFieldsWriter {

    private static final Logger LOGGER = Logger.getLogger(RyftStoredFieldsWriter.class.getName());
    public final static String FIELDS_EXTENSION = "jsonfld";
    private IndexOutput out;
    StoredFieldsWriter writer;

    public RyftStoredFieldsWriter(StoredFieldsWriter delegate, Directory directory, SegmentInfo segment,
            IOContext context) throws IOException {
        this.writer = delegate;
        String dir = directory.toString();
        String sbstr = dir.substring(dir.indexOf("indices"));
        String indexNameStart = sbstr.substring(sbstr.indexOf("/") + 1);
        String indexName = indexNameStart.substring(0, indexNameStart.indexOf("/"));

        boolean success = false;
        try {
            TrackingDirectoryWrapper dirWrapper = (TrackingDirectoryWrapper) directory;
            int start = dir.indexOf("/");
            int end = dir.indexOf(")");
            String dirname = dir.substring(start, end);
            String fileName = segmentFileName(segment.name, "", indexName + FIELDS_EXTENSION);
//            out = new OutputStreamWriter(new FileOutputStream(dirname + "/" + fileName, true));
            out = directory.createOutput(IndexFileNames.segmentFileName(segment.name, "", indexName + FIELDS_EXTENSION), context);
            // Hooking directory to manage(delete when needed) our file too
            dirWrapper.getCreatedFiles().add(fileName);
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void startDocument() throws IOException {
        writer.startDocument();
    }

    @Override
    public int merge(MergeState mergeState) throws IOException {
        long time = System.currentTimeMillis();
        int merged = super.merge(mergeState);
        LOGGER.log(Level.FINE, "Merged finished elapsed: {0}", new Object[]{System.currentTimeMillis() - time});
        return merged;
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {
        writer.writeField(info, field);
        String name = field.name();

        switch (name) {
            case "_source":
                BytesRef bytes = field.binaryValue();
                if (bytes == null) {
                    LOGGER.log(Level.SEVERE, "No value for _source field");
                    break;
                }
                String fieldToWrite = field.binaryValue().utf8ToString();
                int closing = fieldToWrite.lastIndexOf("}");
                if (closing == -1) {
                    LOGGER.log(Level.SEVERE, "Failed to find closing bracket for JSON {0}", fieldToWrite);
                    write(fieldToWrite);
                } else {
                    write(fieldToWrite.substring(0, closing)); // removed last curly
                    // bracket
                }
                break;
            case "_uid":
                String fieldValue = field.stringValue();
                if (fieldValue == null) {
                    LOGGER.log(Level.SEVERE, "Failed to determine _uid field value");
                    break;
                }
                int pos = fieldValue.indexOf("#");
                if (pos != -1) {
                    String data = String.format(",\"%s\": \"%s\", \"type\": \"%s\"}",
                            field.name(), fieldValue.substring(pos + 1), fieldValue.substring(0, pos));
                    write(data);
                } else {
                    write("}");
                    LOGGER.log(Level.SEVERE, "Failed to parse _uid field value: {0}", fieldValue);
                    break;
                }
                break;
            default:
                break;
        }
    }

    @Override
    public void finishDocument() throws IOException {
        writer.finishDocument();
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
        writer.finish(fis, numDocs);
        CodecUtil.writeFooter(out);
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(writer, out);
        } finally {
            IOUtils.closeWhileHandlingException(writer);
            IOUtils.closeWhileHandlingException(out);
            writer = null;
            out = null;
        }
    }

    private void write(byte[] bytes) throws IOException {
        out.writeBytes(bytes, bytes.length);
    }

    private void write(String s) throws IOException {
        LOGGER.log(Level.FINEST, "Write: {0}", s);
        write(s.getBytes());
    }

    public static String segmentFileName(String segmentName, String segmentSuffix, String ext) {
        if (ext.length() > 0 || segmentSuffix.length() > 0) {
            StringBuilder sb = new StringBuilder(segmentName.length() + 2 + segmentSuffix.length() + ext.length());
            sb.append(segmentName);
            if (segmentSuffix.length() > 0) {
                sb.append('_').append(segmentSuffix);
            }
            if (ext.length() > 0) {
                sb.append('.').append(ext);
            }
            return sb.toString();
        } else {
            return segmentName;
        }
    }
}
