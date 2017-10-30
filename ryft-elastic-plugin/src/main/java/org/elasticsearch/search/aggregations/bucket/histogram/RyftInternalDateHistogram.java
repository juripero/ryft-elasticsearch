package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class RyftInternalDateHistogram {

    final static InternalAggregation.Type TYPE = new InternalAggregation.Type("date_histogram", "dhisto");

    public static class Bucket extends RyftInternalHistogram.Bucket {

        Bucket(boolean keyed, ValueFormatter formatter, RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> factory) {
            super(keyed, formatter, factory);
        }

        Bucket(long key, long docCount, InternalAggregations aggregations, boolean keyed, ValueFormatter formatter,
               RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> factory) {
            super(key, docCount, keyed, formatter, factory, aggregations);
        }

        @Override
        public String getKeyAsString() {
            return formatter != null ? formatter.format(key) : ValueFormatter.DateTime.DEFAULT.format(key);
        }

        @Override
        public DateTime getKey() {
            return new DateTime(key, DateTimeZone.UTC);
        }

        @Override
        public String toString() {
            return getKeyAsString();
        }
    }

    public static class Factory extends RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> {

        public Factory() {
        }

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public RyftInternalDateHistogram.Bucket createBucket(InternalAggregations aggregations, RyftInternalDateHistogram.Bucket prototype) {
            return new RyftInternalDateHistogram.Bucket(prototype.key, prototype.docCount, aggregations, prototype.getKeyed(), prototype.formatter, this);
        }

        @Override
        public RyftInternalDateHistogram.Bucket createBucket(Object key, long docCount, InternalAggregations aggregations, boolean keyed,
                                                         ValueFormatter formatter) {
            if (key instanceof Number) {
                return new RyftInternalDateHistogram.Bucket(((Number) key).longValue(), docCount, aggregations, keyed, formatter, this);
            } else if (key instanceof DateTime) {
                return new RyftInternalDateHistogram.Bucket(((DateTime) key).getMillis(), docCount, aggregations, keyed, formatter, this);
            } else {
                throw new AggregationExecutionException("Expected key of type Number or DateTime but got [" + key + "]");
            }
        }

        @Override
        protected RyftInternalDateHistogram.Bucket createEmptyBucket(boolean keyed, ValueFormatter formatter) {
            return new RyftInternalDateHistogram.Bucket(keyed, formatter, this);
        }
    }

    private RyftInternalDateHistogram() {}
}
