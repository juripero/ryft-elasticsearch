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
package com.ryft.elasticsearch.integration.test.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TestData {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    @JsonProperty("registered")
    private String registered;
    @JsonProperty("ipv6")
    private String ipv6;
    @JsonProperty("ipv4")
    private String ipv4;
    @JsonProperty("about")
    private String about;
    @JsonProperty("company")
    private String company;
    @JsonProperty("lastName")
    private String lastName;
    @JsonProperty("firstName")
    private String firstName;
    @JsonProperty("eyeColor")
    private String eyeColor;
    @JsonProperty("age")
    private Integer age;
    @JsonProperty("balance_raw")
    private String balanceRaw;
    @JsonProperty("balance")
    private Double balance;
    @JsonProperty("isActive")
    private Boolean isActive;
    @JsonProperty("index")
    private Integer index;
    @JsonProperty("location")
    private String location;
    @JsonProperty("id")
    private String id;

    @JsonProperty("registered")
    public String getRegistered() {
        return registered;
    }

    @JsonProperty("registered")
    public void setRegistered(String registered) {
        this.registered = registered;
    }

    @JsonProperty("ipv6")
    public String getIpv6() {
        return ipv6;
    }

    @JsonProperty("ipv6")
    public void setIpv6(String ipv6) {
        this.ipv6 = ipv6;
    }

    @JsonProperty("ipv4")
    public String getIpv4() {
        return ipv4;
    }

    @JsonProperty("ipv4")
    public void setIpv4(String ipv4) {
        this.ipv4 = ipv4;
    }

    @JsonProperty("about")
    public String getAbout() {
        return about;
    }

    @JsonProperty("about")
    public void setAbout(String about) {
        this.about = about;
    }

    @JsonProperty("company")
    public String getCompany() {
        return company;
    }

    @JsonProperty("company")
    public void setCompany(String company) {
        this.company = company;
    }

    @JsonProperty("lastName")
    public String getLastName() {
        return lastName;
    }

    @JsonProperty("lastName")
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @JsonProperty("firstName")
    public String getFirstName() {
        return firstName;
    }

    @JsonProperty("firstName")
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @JsonProperty("eyeColor")
    public String getEyeColor() {
        return eyeColor;
    }

    @JsonProperty("eyeColor")
    public void setEyeColor(String eyeColor) {
        this.eyeColor = eyeColor;
    }

    @JsonProperty("age")
    public Integer getAge() {
        return age;
    }

    @JsonProperty("age")
    public void setAge(Integer age) {
        this.age = age;
    }

    @JsonProperty("balance_raw")
    public String getBalanceRaw() {
        return balanceRaw;
    }

    @JsonProperty("balance_raw")
    public void setBalanceRaw(String balanceRaw) {
        this.balanceRaw = balanceRaw;
    }

    @JsonProperty("balance")
    public Double getBalance() {
        return balance;
    }

    @JsonProperty("balance")
    public void setBalance(Double balance) {
        this.balance = balance;
    }

    @JsonProperty("isActive")
    public Boolean getIsActive() {
        return isActive;
    }

    @JsonProperty("isActive")
    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    @JsonProperty("index")
    public Integer getIndex() {
        return index;
    }

    @JsonProperty("index")
    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 31 * hash + Objects.hashCode(this.registered);
        hash = 31 * hash + Objects.hashCode(this.ipv6);
        hash = 31 * hash + Objects.hashCode(this.ipv4);
        hash = 31 * hash + Objects.hashCode(this.about);
        hash = 31 * hash + Objects.hashCode(this.company);
        hash = 31 * hash + Objects.hashCode(this.lastName);
        hash = 31 * hash + Objects.hashCode(this.firstName);
        hash = 31 * hash + Objects.hashCode(this.eyeColor);
        hash = 31 * hash + Objects.hashCode(this.age);
        hash = 31 * hash + Objects.hashCode(this.balanceRaw);
        hash = 31 * hash + Objects.hashCode(this.balance);
        hash = 31 * hash + Objects.hashCode(this.isActive);
        hash = 31 * hash + Objects.hashCode(this.index);
        hash = 31 * hash + Objects.hashCode(this.location);
        hash = 31 * hash + Objects.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TestData other = (TestData) obj;
        if (!Objects.equals(this.registered, other.registered)) {
            return false;
        }
        if (!Objects.equals(this.ipv6, other.ipv6)) {
            return false;
        }
        if (!Objects.equals(this.ipv4, other.ipv4)) {
            return false;
        }
        if (!Objects.equals(this.about, other.about)) {
            return false;
        }
        if (!Objects.equals(this.company, other.company)) {
            return false;
        }
        if (!Objects.equals(this.lastName, other.lastName)) {
            return false;
        }
        if (!Objects.equals(this.firstName, other.firstName)) {
            return false;
        }
        if (!Objects.equals(this.eyeColor, other.eyeColor)) {
            return false;
        }
        if (!Objects.equals(this.balanceRaw, other.balanceRaw)) {
            return false;
        }
        if (!Objects.equals(this.location, other.location)) {
            return false;
        }
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.age, other.age)) {
            return false;
        }
        if (!Objects.equals(this.balance, other.balance)) {
            return false;
        }
        if (!Objects.equals(this.isActive, other.isActive)) {
            return false;
        }
        return Objects.equals(this.index, other.index);
    }

    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }
    
    public String toCsv() {
        return '\"' + registered + "\",\"" + ipv6 + "\",\"" + ipv4 + "\",\"" 
                + about + "\",\"" + company + "\",\"" + lastName + "\",\"" 
                + firstName + "\",\"" + eyeColor + "\",\"" + age + "\",\"" 
                + balanceRaw + "\",\"" + balance + "\",\"" + isActive + "\",\""
                + index + "\",\"" + location + "\",\"" + id + '\"';
    }

    public static TestData fromJson(byte[] content) throws IOException {
        return MAPPER.readValue(content, TestData.class);
    }

}
