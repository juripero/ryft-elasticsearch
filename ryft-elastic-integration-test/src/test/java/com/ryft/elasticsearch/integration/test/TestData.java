package com.ryft.elasticsearch.integration.test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    @JsonProperty("balance")
    private String balance;
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

    @JsonProperty("balance")
    public String getBalance() {
        return balance;
    }

    @JsonProperty("balance")
    public void setBalance(String balance) {
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

    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }

}
