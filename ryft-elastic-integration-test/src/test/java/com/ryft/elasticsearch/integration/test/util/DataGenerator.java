package com.ryft.elasticsearch.integration.test.util;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.fluttercode.datafactory.impl.DataFactory;
import org.fluttercode.datafactory.impl.DefaultContentDataValues;

public abstract class DataGenerator<T> {

    public static final Random RANDOM = new Random();
    public static final DataFactory DATA_FACTORY = new DataFactory();

    public DataGenerator() {
        this(new String[]{"laboris", "consequat", "cillum",
            "nisi", "esse", "pariatur", "tempor", "cupidatat", "irure", "officia",
            "quis", "veniam", "sint", "reprehenderit", "non", "lorem", "adipisicing",
            "dolore", "mollit", "ipsum", "et", "sunt", "excepteur", "minim", "est",
            "culpa", "anim", "occaecat", "perspiciatis", "fugiat", "ipum", "velit", "consectetur",
            "voluptate", "aliqua", "laboris", "labore", "amet", "qui", "aute",
            "aliquip", "sit", "in", "exercitation"});
    }

    public DataGenerator(String[] words) {
        DefaultContentDataValues.words = words;
    }

    public List<T> getDataSamples(Integer num) {
        return IntStream.range(0, num).mapToObj(this::getDataSample).collect(Collectors.toList());
    }

    public abstract T getDataSample(Integer id);

    public String getSentense(Integer words) {
        return IntStream.range(0, words)
                .mapToObj(i -> DATA_FACTORY.getRandomWord()).collect(Collectors.joining(" ")) + ".";
    }
}
