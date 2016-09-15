package com.dataart.elastic.ryft.codec;

import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.simpletext.SimpleTextStoredFieldsReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

public class RyftStoredFieldsFormat extends StoredFieldsFormat {
	
	StoredFieldsFormat delegate;
	StoredFieldsWriter fieldsWriter;
	
	public RyftStoredFieldsFormat(StoredFieldsFormat delegate) {
		this.delegate = delegate;
		
	}

	@Override
	public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si,
			FieldInfos fn, IOContext context) throws IOException {
		return delegate.fieldsReader(directory, si, fn, context);
	}

	@Override
	public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si,
			IOContext context) throws IOException {
		//TODO: [imasternoy] Ugly a little bit
		return new RyftStoredFieldsWriter(delegate.fieldsWriter(directory, si, context), directory, si, context);
	}
	
}
