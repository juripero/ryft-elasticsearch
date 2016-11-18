package com.dataart.elastic.ryft.codec;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

import com.dataart.elastic.ryft.codec.utils.SimpleTextUtil;

/**
 * 
 * @author imasternoy
 */
public class RyftStoredFieldsWriter extends StoredFieldsWriter {
    Logger log = Logger.getLogger(RyftStoredFieldsWriter.class.getName());
    public final static String FIELDS_EXTENSION = "jsonfld";
    private OutputStreamWriter out;
    private int numDocsWritten = 0;
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
            String fileName = IndexFileNames.segmentFileName(segment.name, "", indexName + FIELDS_EXTENSION);
            out = new OutputStreamWriter(new FileOutputStream(dirname + "/" + fileName, true));
            //Hooking directory to manage(delete when needed) our file too
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
        write("{\"doc\":");
        numDocsWritten++;
    }

    @Override
    public int merge(MergeState mergeState) throws IOException {
        long time = System.currentTimeMillis();
        int merged = super.merge(mergeState);
        log.log(Level.FINE,"Merged finished elapsed: {0}",new Object[]{System.currentTimeMillis()-time});
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
            write(fieldToWrite);
            write(",");
            break;
        case "_uid":
            String fieldValue = field.stringValue();
            if (fieldValue == null) {
                log.log(Level.SEVERE, "Failed to determine _uid field value");
                break;
            }
            int pos = fieldValue.indexOf("#");
            if (pos != -1) {
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
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
            out.close();
        } finally {
            IOUtils.closeWhileHandlingException(writer);
            IOUtils.closeWhileHandlingException(out);
            writer = null;
            out = null;
        }
    }

    private void write(String s) throws IOException {
        out.write(s);
    }
    
}
