package com.dataart.ryft.elastic.parser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.xml.ParserException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;

public class FuzzyQueryParser {

    public static RyftRequestEvent parseQuery(BytesReference searchContent) throws ElasticsearchParseException {
        RyftRequestEvent request = null;
        String currentName;
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(searchContent).createParser(searchContent);
            XContentParser.Token token = parser.nextToken(); // Should be
                                                             // startObject
                                                             // token
            token = parser.nextToken();
            currentName = parser.currentName();
            if (token != XContentParser.Token.FIELD_NAME && !currentName.equals("query")) {
                return null;
            }

            parser.nextToken(); // Start object
            token = parser.nextToken();
            currentName = parser.currentName();
            if (token != XContentParser.Token.FIELD_NAME
                    && (!parser.currentName().equals("match") || !currentName.equals("multi_match"))) {
                return null;
            }

            if (currentName.equals("multi_match")) {
                return parseMultiMatch(parser);
            }

            parser.nextToken(); // Start object
            token = parser.nextToken();
            currentName = parser.currentName();
            // Now it should be filedName
            Integer fuzziness = null;
            String fieldQuery = null;
            String fieldName = null;
            if (token != XContentParser.Token.FIELD_NAME) {
                // Parsing Exception here
                throw new ElasticsearchParseException(
                        "Failed to parse query after match expected record field name but " + token + " found");
            }
            fieldName = parser.currentName();
            // next token should be query
            parser.nextToken();
            Map<String, Object> fieldProperties = parser.map();
            if (fieldProperties.containsKey("query")) {
                fieldQuery = (String) fieldProperties.get("query");
            } else {
                throw new ElasticsearchParseException("Failed to parse query. Field entry should have 'query' field");
            }
            
            if (fieldProperties.containsKey("fuzziness")) {
                fuzziness = (Integer) fieldProperties.get("fuzziness");
            } else {
                throw new ElasticsearchParseException("Failed to parse query. Field entry should have 'fuzziness' field");
            }
            request = new RyftRequestEvent(fuzziness, fieldQuery, Arrays.asList(fieldName));

        } catch (Exception e) {
            throw new ElasticsearchParseException("Failed to parse query", e);
        }
        return request;
    }

    private static RyftRequestEvent parseMultiMatch(XContentParser parser) {
        //XXX: [imasternoy] Implement me
        return null;
    }

}
