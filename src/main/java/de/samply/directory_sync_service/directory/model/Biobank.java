package de.samply.directory_sync_service.directory.model;

import de.samply.directory_sync_service.Util;

import java.util.List;
import java.util.Map;

public class Biobank {
    String acronym; // alias
    List<Map> capabilities;
    Map contact;
    Map country;
    String description;
    Map head;
    String id;
    String juridical_person;
    String latitude;
    String location;
    String longitude;
    String name;
    List<Map> network;
    String url;

    public Biobank() {
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public List<Map> getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(List<Map> capabilities) {
        this.capabilities = capabilities;
    }

    public Map getContact() {
        return contact;
    }

    public void setContact(Map contact) {
        this.contact = contact;
    }

    public Map getCountry() {
        return country;
    }

    public void setCountry(Map country) {
        this.country = country;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map getHead() {
        return head;
    }

    public void setHead(Map head) {
        this.head = head;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJuridicalPerson() {
        return juridical_person;
    }

    public void setJuridicalPerson(String juridical_person) {
        this.juridical_person = juridical_person;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Map> getNetwork() {
        return network;
    }

    public void setNetwork(List<Map> network) {
        this.network = network;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return Util.jsonStringFomObject(this);
    }
}
