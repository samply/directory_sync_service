package de.samply.directory_sync_service.fhir.model;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DTO for carrying the data collected from a FHIR store relating to a single
 * collection of samples.
 */
public class FhirCollection {

  private String id;
  private Integer size;
  private Integer numberOfDonors;
  private List<String> sex;
  private Integer ageLow;
  private Integer ageHigh;
  private List<String> materials;
  private List<String> storageTemperatures;
  private List<String> diagnosisAvailable;

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public Integer getSize() {
    return size;
  }

  public void setNumberOfDonors(Integer numberOfDonors) {
    this.numberOfDonors = numberOfDonors;
  }

  public Integer getNumberOfDonors() {
    return numberOfDonors;
  }

  public void setSex(List<String> sex) {
    this.sex = sex;
  }

  public List<String> getSex() {
    return sex;
  }

  public void setAgeLow(Integer ageLow) {
    this.ageLow = ageLow;
  }

  public Integer getAgeLow() {
    return ageLow;
  }

  public void setAgeHigh(Integer ageHigh) {
    this.ageHigh = ageHigh;
  }

  public Integer getAgeHigh() {
    return ageHigh;
  }

  public void setMaterials(List<String> materials) {
    this.materials = materials;
  }

  public List<String> getMaterials() {
    return materials;
  }

  public void setStorageTemperatures(List<String> storageTemperatures) {
    this.storageTemperatures = storageTemperatures;
  }

  public List<String> getStorageTemperatures() {
    return storageTemperatures;
  }

  public void setDiagnosisAvailable(List<String> diagnosisAvailable) {
    this.diagnosisAvailable = diagnosisAvailable;
  }

  public List<String> getDiagnosisAvailable() {
    return diagnosisAvailable;
  }
}
