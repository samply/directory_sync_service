package de.samply.directory_sync_service.directory.model;

//TODO Add other relevant attributes
public class Biobank {

    String id;

    String name;

    String acronym;

    String description;

    String url;

    String juridical_person;

    boolean it_support_available;

    int it_staff_site;

    boolean is_available;

    boolean partner_charter_signed;

    String head_firstname;

    String head_lastname;

    String head_role;

    String latitude;

    String longitude;

    String[] also_known;

    boolean collaboration_commercial;

    boolean collaboration_non_for_profit;

    public Biobank() {

    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public BbmriEricId getId() {
        return BbmriEricId.valueOf(id).get();
    }

    public void setId(BbmriEricId id) {
        this.id = id.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getJuridical_person() {
        return juridical_person;
    }

    public void setJuridical_person(String juridical_person) {
        this.juridical_person = juridical_person;
    }

    public boolean isIt_support_available() {
        return it_support_available;
    }

    public void setIt_support_available(boolean it_support_available) {
        this.it_support_available = it_support_available;
    }

    public int getIt_staff_site() {
        return it_staff_site;
    }

    public void setIt_staff_site(int it_staff_site) {
        this.it_staff_site = it_staff_site;
    }

    public boolean isIs_available() {
        return is_available;
    }

    public void setIs_available(boolean is_available) {
        this.is_available = is_available;
    }

    public boolean isPartner_charter_signed() {
        return partner_charter_signed;
    }

    public void setPartner_charter_signed(boolean partner_charter_signed) {
        this.partner_charter_signed = partner_charter_signed;
    }

    public String getHead_firstname() {
        return head_firstname;
    }

    public void setHead_firstname(String head_firstname) {
        this.head_firstname = head_firstname;
    }

    public String getHead_lastname() {
        return head_lastname;
    }

    public void setHead_lastname(String head_lastname) {
        this.head_lastname = head_lastname;
    }

    public String getHead_role() {
        return head_role;
    }

    public void setHead_role(String head_role) {
        this.head_role = head_role;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String[] getAlso_known() {
        return also_known;
    }

    public void setAlso_known(String[] also_known) {
        this.also_known = also_known;
    }

    public boolean isCollaboration_commercial() {
        return collaboration_commercial;
    }

    public void setCollaboration_commercial(boolean collaboration_commercial) {
        this.collaboration_commercial = collaboration_commercial;
    }

    public boolean isCollaboration_non_for_profit() {
        return collaboration_non_for_profit;
    }

    public void setCollaboration_non_for_profit(boolean collaboration_non_for_profit) {
        this.collaboration_non_for_profit = collaboration_non_for_profit;
    }

    @Override
    public String toString() {
        return "Biobank{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
