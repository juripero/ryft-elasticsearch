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
package com.ryft.elasticsearch.integration.test.util;

import com.ryft.elasticsearch.integration.test.entity.TestData;
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
        result.setAbout(getSentense(DATA_FACTORY.getNumberBetween(5, 10)));
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
        Double balance = Integer.valueOf(RANDOM.nextInt(1000000)).doubleValue() / 100;
        result.setBalance(balance);
        result.setBalanceRaw(String.format("$%,.2f", balance));
        result.setLocation(String.format(Locale.ROOT, "%f,%f", 50 + RANDOM.nextDouble(), 30 + RANDOM.nextDouble()));
        return result;
    }

}
