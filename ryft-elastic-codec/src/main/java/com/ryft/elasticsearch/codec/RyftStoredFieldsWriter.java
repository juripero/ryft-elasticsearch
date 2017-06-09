package com.ryft.elasticsearch.codec;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
    Logger log = Logger.getLogger(RyftStoredFieldsWriter.class.getName());
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
            String fileName = segmentFileName(segment.name, "",  indexName + FIELDS_EXTENSION);
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
        log.log(Level.FINE, "Merged finished elapsed: {0}", new Object[] { System.currentTimeMillis() - time });
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
                log.log(Level.SEVERE, "No value for _source field");
                break;
            }
            String fieldToWrite = new String(field.binaryValue().utf8ToString());
            int closing = fieldToWrite.lastIndexOf("}");
            if (closing == -1) {
                log.log(Level.SEVERE, "Failed to find closing bracket for JSON " + fieldToWrite);
                write(fieldToWrite);
            } else {
                write(fieldToWrite.substring(0, closing)); // removed last curly
                                                           // bracket
            }
            break;
        case "_uid":
            String fieldValue = field.stringValue();
            if (fieldValue == null) {
                log.log(Level.SEVERE, "Failed to determine _uid field value");
                break;
            }
            int pos = fieldValue.indexOf("#");
            if (pos != -1) {
                write(",");
                write("\"" + field.name() + "\"" + ":" + " \"");
                write(fieldValue.substring(pos + 1));
                write("\",");
                // newLine();
                write("\"type\": \"");
                write(fieldValue.substring(0, pos));
                write("\"}");
            } else {
                write("}");
                log.log(Level.SEVERE, "Failed to parse _uid field value: {0}", fieldValue);
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

    private void write(String s) throws IOException {
        out.writeBytes(s.getBytes(), s.length());
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
