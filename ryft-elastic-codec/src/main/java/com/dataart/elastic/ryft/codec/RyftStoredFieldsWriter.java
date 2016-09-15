package com.dataart.elastic.ryft.codec;

import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

import com.dataart.elastic.ryft.codec.utils.SimpleTextUtil;

public class RyftStoredFieldsWriter extends StoredFieldsWriter {
	public final static String FIELDS_EXTENSION = "jsonfld";
	private final BytesRefBuilder scratch = new BytesRefBuilder();
	private IndexOutput out;

	private int numDocsWritten = 0;

	StoredFieldsWriter writer;

	public RyftStoredFieldsWriter(StoredFieldsWriter delegate,
			Directory directory, SegmentInfo segment, IOContext context)
			throws IOException {
		this.writer = delegate;
		String dir = directory.toString();
		String sbstr = dir.substring(dir.indexOf("indices"));
		String indexNameStart = sbstr.substring(sbstr.indexOf("/")+1);
		String indexName = indexNameStart.substring(0, indexNameStart.indexOf("/"));
		
		boolean success = false;
		try {
			out = directory.createOutput(IndexFileNames.segmentFileName(
					segment.name, "",indexName+ FIELDS_EXTENSION), context);
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
		write("{");
		write("\"doc\":");
		numDocsWritten++;
	}

	@Override
	public void writeField(FieldInfo info, IndexableField field)
			throws IOException {
		writer.writeField(info, field);
		final Number n = field.numericValue();
		if (n != null) {
			// TODO: [imasternoy] place log here and skip this value
			System.out.println("Numeric field");
		} else {
			BytesRef bytes = field.binaryValue();
			if (bytes != null) {
				write(bytes);
				write(",");
				newLine();
			} else if (field.stringValue() == null) {
				throw new IllegalArgumentException(
						"field "
								+ field.name()
								+ " is stored but does not have binaryValue, stringValue nor numericValue");
			} else {
				// TODO: [imasternoy] Additional source info
				// Should be _uid check field.name() == "_uid"
				// Also check for _version field
				String fieldValue = field.stringValue();
				if (field.name() == "_uid") {
					write("\""+field.name()+"\"" + ":"+" \"");
					int pos = fieldValue.indexOf("#");
					if (pos != -1) {
						write(fieldValue.substring(pos + 1));
						write("\",");
						newLine();
						write("\"type\": \"");
						write(fieldValue.substring(0, pos));
						write("\"");
					} else {
						// Should never happen
						throw new RuntimeException("Failed to write index uid");
					}
				} else {
					// Should never happen
					System.out.println("Received field:"+field.name());
					write(field.stringValue());
				}
				newLine();
			}
		}
	}
	
	@Override
	public void finishDocument() throws IOException {
		write("},");
		newLine();
		writer.finishDocument();
	}

	@Override
	public void finish(FieldInfos fis, int numDocs) throws IOException {
		writer.finish(fis, numDocs);
		write("{\"end\":-1}");
		if (numDocsWritten != numDocs) {
			throw new RuntimeException(
					"mergeFields produced an invalid result: docCount is "
							+ numDocs
							+ " but only saw "
							+ numDocsWritten
							+ " file="
							+ out.toString()
							+ "; now aborting this merge to prevent index corruption");
		}
//		SimpleTextUtil.writeChecksum(out, scratch);
	}

	@Override
	public void close() throws IOException {
		try {
			writer.close();
			IOUtils.close(out);
		} finally {
			out = null;
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
