package de.samply.directory_sync_service.directory;

import io.vavr.control.Either;
import org.apache.http.impl.client.CloseableHttpClient;
import org.hl7.fhir.r4.model.OperationOutcome;

public class DirectoryCredentials {
  public static Either<OperationOutcome, LoginResponse> firstLogin(CloseableHttpClient httpClient,
                                                                                String baseUrl,
                                                                                String username,
                                                                                String password,
                                                                                boolean mockDirectory) {
    if (mockDirectory)
      // Don't try logging in if we are mocking
      return Either.right(new LoginResponse());

    LoginResponse loginResponse = login(httpClient, baseUrl, username, password);
    if (loginResponse != null)
      return Either.right(loginResponse);
    else
      return Either.left(DirectoryUtils.error("firstLogin", "Failed to login to Directory"));
  }

  public static LoginResponse login(CloseableHttpClient httpClient,
                                                 String baseUrl,
                                                 String username,
                                                 String password) {
    LoginResponse loginResponse = (LoginResponse) DirectoryRest.post(httpClient, baseUrl + "/api/v1/login", LoginResponse.class, new LoginCredentials(username, password));

    return loginResponse;
  }

  static class LoginCredentials {
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
