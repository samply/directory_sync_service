package de.samply.directory_sync_service.directory.model;

public class Biobank {
    String id;

    String name;

    public Biobank() {

    }

    public String getName() {
        return name;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Biobank{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
