package de.samply.directory_sync_service.directory.model;

//TODO Add other relevant attributes
public class Biobank {
    String id;

    String name;

    public Biobank() {

    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Biobank{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
