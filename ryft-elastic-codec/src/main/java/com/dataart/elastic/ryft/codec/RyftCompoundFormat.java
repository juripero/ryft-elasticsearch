package com.dataart.elastic.ryft.codec;

import java.io.IOException;

import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

public class RyftCompoundFormat extends CompoundFormat {

	CompoundFormat delegate;

	public RyftCompoundFormat(CompoundFormat format) {
		this.delegate = format;
	}

	@Override
	public Directory getCompoundReader(Directory dir, SegmentInfo si,
			IOContext context) throws IOException {
		Directory readerDir = delegate.getCompoundReader(dir, si, context);
		return readerDir;
	}

	@Override
	public void write(Directory dir, SegmentInfo si, IOContext context)
			throws IOException {
		this.delegate.write(dir, si, context);
	}
}
