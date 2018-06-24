package Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class ConnectionManager {
	
	public static MongoClient mongoClient;
	
    public static MongoClient getConnection() {
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
		} catch (MongoException ex) {
			 
			System.out.println("Erro na conexão.");
		    System.out.println(ex); 
		}
        return mongoClient;
    }

}
