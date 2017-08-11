package com.ryft.elasticsearch.integration.test.util;

import com.ryft.elasticsearch.integration.test.TestData;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

public class TestDataGenerator extends DataGenerator<TestData> {

    private final DateFormat dateFormat;

    public TestDataGenerator(DateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    @Override
    public TestData getDataSample(Integer id) {
        TestData result = new TestData();
        result.setIndex(id);
        result.setId(id.toString());
        result.setAge(DATA_FACTORY.getNumberBetween(16, 65));
        result.setEyeColor(DATA_FACTORY.getItem(new String[]{"green", "blue", "brown"}));
        result.setFirstName(DATA_FACTORY.getFirstName());
        result.setLastName(DATA_FACTORY.getLastName());
        result.setAbout(DATA_FACTORY.getRandomText(50, 100));
        result.setRegistered(dateFormat.format(DATA_FACTORY.getDate(new Date(), -1000, 0)));
        result.setIpv4(String.format("192.168.%d.%d",
                DATA_FACTORY.getNumberUpTo(256), DATA_FACTORY.getNumberUpTo(256)));
        result.setIpv6(String.format("%s:%s:%s:%s:%s:%s:%s:%s",
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536)),
                Integer.toHexString(DATA_FACTORY.getNumberUpTo(65536))
        ));
        result.setCompany(DATA_FACTORY.getBusinessName());
        result.setIsActive(DATA_FACTORY.chance(70));
        Double balance = RANDOM.nextDouble() * 10000;
        result.setBalance(balance);
        result.setBalanceRaw(String.format("$%,.2f", balance));
        result.setLocation(String.format(Locale.ROOT, "%f,%f", 50 + RANDOM.nextDouble(), 30 + RANDOM.nextDouble()));
        return result;
    }

}
