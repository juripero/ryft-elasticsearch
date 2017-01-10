package com.ryft.elasticsearch.codec;

import java.io.IOException;

import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
/**
 * 
 * @author imasternoy
 */
public class RyftSegmentInfoFormat extends SegmentInfoFormat {
	
	SegmentInfoFormat delegate; 
	
	public RyftSegmentInfoFormat(SegmentInfoFormat delegate) {
		this.delegate = delegate;
	}
			

	@Override
	public SegmentInfo read(Directory directory, String segmentName,
			byte[] segmentID, IOContext context) throws IOException {
		SegmentInfo originalInfo = delegate.read(directory, segmentName, segmentID, context);
		//We need to add info about text segments
		return originalInfo;
	}

	@Override
	public void write(Directory dir, SegmentInfo info, IOContext ioContext)
			throws IOException {
		delegate.write(dir, info, ioContext);
	}

}
