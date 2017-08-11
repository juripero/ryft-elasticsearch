package com.ryft.elasticsearch.integration.test.util;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.fluttercode.datafactory.impl.DataFactory;

public abstract class DataGenerator<T> {

    protected static final Random RANDOM = new Random();
    protected static final DataFactory DATA_FACTORY = new DataFactory();

    public List<T> getDataSamples(Integer num) {
        return IntStream.range(0, num).mapToObj(this::getDataSample).collect(Collectors.toList());
    }

    public abstract T getDataSample(Integer id);
}
