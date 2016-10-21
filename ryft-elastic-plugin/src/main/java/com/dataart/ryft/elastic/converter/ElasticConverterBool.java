package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex;
import static com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator.AND;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterBool implements ElasticConvertingElement {

    public static class ElasticConverterMust implements ElasticConvertingElement {

        private static final String NAME = "must";

        @Override
        public String[] names() {
            return new String[]{NAME};
        }

        @Override
        public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            try {
                XContentParser parser = convertingContext.getContentParser();
                XContentParser.Token token = parser.nextToken();
                String currentName = parser.currentName();
                if (XContentParser.Token.START_ARRAY.equals(token) && NAME.equals(currentName)) {
                    List<RyftQuery> ryftQueryParts = new ArrayList<>();
                    while (!XContentParser.Token.END_ARRAY.equals(token)) {
                        token = parser.currentToken();
                        currentName = parser.currentName();
                        ElasticConvertingElement elasticConverter;
                        if ((currentName != null) && (XContentParser.Token.FIELD_NAME.equals(token))) {
                            elasticConverter = convertingContext.getElasticConverter(parser.currentName());
                            ryftQueryParts.add(elasticConverter.convert(convertingContext));
                        }
                        parser.nextToken();
                    }
                    return new RyftQueryComplex(AND, ryftQueryParts);
                }
            } catch (IOException | ClassNotFoundException ex) {
                throw new ElasticConversionException(ex);
            }
            return null;
        }

    }

    private static final String NAME = "bool";

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            XContentParser parser = convertingContext.getContentParser();
            XContentParser.Token token = parser.nextToken();
            String currentName = parser.currentName();
            if (XContentParser.Token.START_OBJECT.equals(token) && NAME.equals(currentName)) {
                parser.nextToken();
                currentName = parser.currentName();
                return convertingContext.getElasticConverter(currentName).convert(convertingContext);
            }
        } catch (IOException | ClassNotFoundException ex) {
            throw new ElasticConversionException(ex);
        }
        return null;
    }

}
