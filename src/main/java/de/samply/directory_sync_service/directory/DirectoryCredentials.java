package de.samply.directory_sync_service.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents credentials used for authenticating with the Directory service.
 * <p>
 * This class manages the username, password, and authentication token required
 * for interacting with the Directory service. It also provides methods for generating
 * login credentials and storing the token received upon successful authentication.
 * </p>
 */
public class DirectoryCredentials {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryCredentials.class);
  private String username;
  private String password;
  private String token;

  /**
   * Constructs a {@code DirectoryCredentials} object with the specified username and password.
   *
   * @param username the username for Directory authentication
   * @param password the password for Directory authentication
   */
  public DirectoryCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  /**
   * Retrieves the current authentication token.
   *
   * @return the authentication token, or {@code null} if not yet set
   */
  public String getToken() {
    return token;
  }

  /**
   * Sets the authentication token.
   *
   * @param token the token to be set
   */
  public void setToken(String token) {
    this.token = token;
  }

  /**
   * Generates a new {@link LoginCredentials} object using the stored username and password.
   * <p>
   * This method is used to create the credentials required for the login request.
   * </p>
   *
   * @return a new {@link LoginCredentials} object containing the username and password
   */
  public Object generateLoginCredentials() {
    return new LoginCredentials(username, password);
  }

  /**
   * Inner class representing the login credentials sent to the Directory service.
   * <p>
   * This class holds the username and password required for authentication. It is typically
   * used in conjunction with the {@link DirectoryCredentials#generateLoginCredentials()} method.
   * </p>
   */
  public class LoginCredentials {
    String username, password;

    LoginCredentials(String username, String password) {
      this.username = username;
      this.password = password;
    }
  }

  /**
   * Static inner class representing the response received after a successful login.
   * <p>
   * This class stores the username and the authentication token returned by the Directory service.
   * It is used to capture the information provided in the login response.
   * </p>
   */
  public static class LoginResponse {
    public String username, token;

    LoginResponse() {
    }
  }
}
