package com.dataart.ryft.elastic.converter;

import static com.dataart.ryft.elastic.plugin.PropertiesProvider.RYFT_FILES_TO_SEARCH;
import com.dataart.ryft.utils.Try;
import java.util.List;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterRyftFiles implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRyftEnabled.class);
    final static String NAME = "ryft_files";

    @Override
    public Try<Void> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            List<String> ryftFiles = ElasticConversionUtil.getArray(convertingContext);
            convertingContext.getQueryProperties().put(RYFT_FILES_TO_SEARCH, ryftFiles);
            return null;
        });
    }

}
