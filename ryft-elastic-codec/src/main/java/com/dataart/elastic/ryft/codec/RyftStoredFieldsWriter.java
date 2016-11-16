package com.dataart.elastic.ryft.codec;

import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.codecs.StoredFieldsReader;
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
import org.apache.lucene.util.Bits;
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
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private IndexOutput out;
    FileWriter fileWriter;

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
            out = directory.createOutput(
                    IndexFileNames.segmentFileName(segment.name, "", indexName + FIELDS_EXTENSION), context);
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
        log.severe("Merging process has been started");
        int merged = writer.merge(mergeState);
//        writer.close();
        super.merge(mergeState);
        int docCount = 0;

        for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) {
            StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[i];
            storedFieldsReader.checkIntegrity();
            MergeVisitor visitor = new MergeVisitor(mergeState, i);
            int maxDoc = mergeState.maxDocs[i];
            Bits liveDocs = mergeState.liveDocs[i];
            // write("[");
            for (int docID = 0; docID < maxDoc; docID++) {
                if (liveDocs != null && !liveDocs.get(docID)) {
                    // skip deleted docs
                    continue;
                }
                // try {

                write("{\"doc\":");
                storedFieldsReader.visitDocument(docID, visitor);
                write("},");
                newLine();
                docCount++;
            }
        }
        // finish(mergeState.mergeFieldInfos, docCount);
        // // write("{\"end\":-1}]");
        log.severe("Merged finished");
        return merged/2;
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
            try {
                write(bytes);
                write(",");
                newLine();
            } catch (IOException e) {
                log.log(Level.WARNING, "Failed to write source field size {0}, {1}", new Object[] { bytes.length,
                        new String(bytes.bytes) });
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
                write("\"" + field.name() + "\"" + ":" + " \"");
                write(fieldValue.substring(pos + 1));
                write("\",");
                newLine();
                write("\"type\": \"");
                write(fieldValue.substring(0, pos));
                write("\"");
            } else {
                log.log(Level.SEVERE, "Failed to parse _uid field value: {0}", fieldValue);
                break;
            }
            newLine();
            break;
        default:
            break;
        }
    }

    @Override
    public void finishDocument() throws IOException {
        write("}");
        newLine();
        writer.finishDocument();
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
        if (writer != null) {
            writer.finish(fis, numDocs);
        }
        // SimpleTextUtil.writeChecksum(out, scratch);
    }

    @Override
    public void close() throws IOException {
        try {
            if (writer != null) {
                writer.close();
            }
            out.close();
        } finally {
            IOUtils.close(writer);
            writer = null;
        }
    }

    private void write(String s) throws IOException {
        SimpleTextUtil.write(out, s, scratch);
    }

    private void write(BytesRef bytes) throws IOException {
        SimpleTextUtil.write(out, bytes);
    }

    private void newLine() throws IOException {
        SimpleTextUtil.writeNewline(out);
    }
}
