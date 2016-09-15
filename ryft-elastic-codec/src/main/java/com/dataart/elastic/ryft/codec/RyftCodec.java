package com.dataart.elastic.ryft.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
/**
 * 
 * @author imasternoy
 */
public class RyftCodec extends Codec {
	Codec delegate;
	SegmentInfoFormat segmentInfoFormat;
	StoredFieldsFormat storedFieldsFormat;
	CompoundFormat compoundFormat;

	public RyftCodec() {
		super("RyftCodec");
		//TODO: [imasternoy] Check possibility to do SPI lookup Codec.forName("")
		this.delegate = new Lucene54Codec();
		this.segmentInfoFormat = new RyftSegmentInfoFormat(delegate.segmentInfoFormat());
		this.compoundFormat = new RyftCompoundFormat(delegate.compoundFormat());
		this.storedFieldsFormat = new RyftStoredFieldsFormat(delegate.storedFieldsFormat());
	}

	@Override
	public PostingsFormat postingsFormat() {
		return delegate.postingsFormat();
	}

	@Override
	public DocValuesFormat docValuesFormat() {
		return delegate.docValuesFormat();
	}

	@Override
	public StoredFieldsFormat storedFieldsFormat() {
		return storedFieldsFormat;
	}

	@Override
	public TermVectorsFormat termVectorsFormat() {
		return delegate.termVectorsFormat();
	}

	@Override
	public FieldInfosFormat fieldInfosFormat() {
		return delegate.fieldInfosFormat();
	}

	@Override
	public SegmentInfoFormat segmentInfoFormat() {
		return this.segmentInfoFormat;
	}

	@Override
	public NormsFormat normsFormat() {
		return delegate.normsFormat();
	}

	@Override
	public LiveDocsFormat liveDocsFormat() {
		return delegate.liveDocsFormat();
	}

	@Override
	public CompoundFormat compoundFormat() {
		return compoundFormat;
	}

}
