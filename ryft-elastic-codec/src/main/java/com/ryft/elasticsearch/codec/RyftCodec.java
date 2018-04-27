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
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
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
//		this.delegate = new SimpleTextCodec();
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
