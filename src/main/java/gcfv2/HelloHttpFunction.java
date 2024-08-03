package gcfv2;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.net.HttpURLConnection;
import com.google.gson.Gson;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;

public class HelloHttpFunction implements HttpFunction {

  private static final Logger logger = Logger.getLogger(HelloHttpFunction.class.getName());

  public void service(final HttpRequest request, final HttpResponse response) throws Exception {
    final BufferedWriter writer = response.getWriter();
    Gson gson = new Gson(); 
    response.setContentType("application/json");
    String url = System.getenv("databaseUrl");
    String username = System.getenv("username");
    String password = System.getenv("password");
    String driver = System.getenv("driverName");
    String sequenceName = request.getFirstQueryParameter("sequence").orElse("");
    Map<String, String> responseBody = new HashMap<>();
    if (sequenceName.isBlank()) {
        responseBody.put("visit_count", String.valueOf(-1));
        responseBody.put("error", "missing query param 'sequence'");
        writer.write(gson.toJson(responseBody));
        return;
    }
    try {
        long count = -1;
        long sequenceId = -1;
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, username, password);
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM website_hit_sequence WHERE  sequence_name = ?");
        statement.setString(1, sequenceName);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getLong("sequence_count");
            sequenceId = resultSet.getLong("id");
        }
        statement = connection.prepareStatement("UPDATE website_hit_sequence SET sequence_count = ? WHERE  id = ?");
        statement.setLong(1, count + 1);
        statement.setLong(2, sequenceId);
        statement.executeUpdate();
        logger.info("API Call Success");
        connection.close();
        responseBody.put("visit_count", String.valueOf(count + 1));
        writer.write(gson.toJson(responseBody));
    } catch (ClassNotFoundException | SQLException e) {
        e.printStackTrace();
        logger.severe("API Call Failed : " + e.toString());
        responseBody.put("visit_count", String.valueOf(-1));
        responseBody.put("error", "Something went wrong!");
        response.setStatusCode(HttpURLConnection.HTTP_SERVER_ERROR);
        writer.write(gson.toJson(responseBody));
    }
  }
}
