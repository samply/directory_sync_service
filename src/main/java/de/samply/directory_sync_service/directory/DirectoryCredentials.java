package de.samply.directory_sync_service.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryCredentials {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryCredentials.class);
  private String username;
  private String password;
  private String token;

  public DirectoryCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public Object generateLoginCredentials() {
    return new LoginCredentials(username, password);
  }

  public class LoginCredentials {
    String username, password;

    LoginCredentials(String username, String password) {
      this.username = username;
      this.password = password;
    }
  }

  public static class LoginResponse {
    String username, token;

    LoginResponse() {
    }
  }
}
