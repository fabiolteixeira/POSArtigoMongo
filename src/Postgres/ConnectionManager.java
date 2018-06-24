package Postgres;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionManager {

	private static String username = "postgres";   
    private static String password = "123456";
    public static Connection conn;
    private static String url = "jdbc:postgresql://127.0.0.1:5432/postgres";
    public static Connection getConnection() {
    	
    	try {
    	    Class.forName("org.postgresql.Driver");
    	    System.out.println("Driver postgresql loaded!");
    	} catch (ClassNotFoundException e) {
    	    throw new IllegalStateException("Cannot find the driver postgresql in the classpath!", e);
    	}
    	
		try {
			conn = DriverManager.getConnection(url, username, password);
			System.out.println("Conexão realizada com sucesso.");
		} catch (SQLException ex) {
			 
			System.out.println("Erro na conexão.");
		    System.out.println(ex); 
		}
        return conn;
    }
}
