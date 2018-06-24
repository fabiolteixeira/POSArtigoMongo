package Mongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ImportCSVFamilia {
	public static void main(String[] args) throws Exception {
		
		MongoClient mongoClient = ConnectionManager.getConnection();
		MongoDatabase database = mongoClient.getDatabase("bfa");
		MongoCollection<Document> collection = database.getCollection("familia");
	    int count = 0;
	    String file = "D:/base_amostra_familia_201712-20180411-145946.csv";

	    try(BufferedReader br = new BufferedReader(new FileReader(file))) {
	        String line = "";
	       
	        long startTime = System.currentTimeMillis();
	        while ((line = br.readLine()) != null) {
	        	if (count > 0) {
	        		
			        String[] linha = line.split(";");
					Document doc = new Document("_id", linha[0])
							.append("cd_ibge", linha[1])
							.append("id_estrato", linha[2])
							.append("id_subdivisao_unid_fed", linha[3])
							.append("dat_cadastramento_fam", linha[4])
							.append("dat_alteracao_fam", linha[5])
							.append("vlr_renda_media_fam", linha[6])
							.append("dat_atualizacao_familia", linha[7])
							.append("id_local_domic_fam", linha[8])
							.append("id_especie_domic_fam", linha[9])
							.append("qtd_comodos_domic_fam", linha[10])
							.append("qtd_comodos_dormitorio_fam", linha[11])
							.append("id_material_piso_fam", linha[12])
							.append("id_material_domic_fam", linha[13])
							.append("ind_agua_canalizada_fam", linha[14])
							.append("id_abaste_agua_domic_fam", linha[15])
							.append("ind_banheiro_domic_fam", linha[16])
							.append("id_escoa_sanitario_domic_fam", linha[17])
							.append("id_destino_lixo_domic_fam", linha[18])
							.append("id_iluminacao_domic_fam", linha[19])
							.append("id_calcamento_domic_fam", linha[20])
							.append("ind_familia_indigena_fam", linha[21])
							.append("ind_familia_quilombola_fam", linha[22])
							.append("nom_estab_assist_saude_fam", linha[23])
							.append("cod_eas_fam", linha[24])
							.append("nom_centro_assist_fam", linha[25])
							.append("cod_centro_assist_fam", linha[26])
							.append("id_ind_parc_mds_fam", linha[27])
							.append("qtd_pessoas", linha[28])
							.append("peso_fam", linha[29])
							.append("marc_pbf", linha[30]);
					
					collection.insertOne(doc);
	        	}
	        	count++;
	        	
	        }
	        mongoClient.close();	        
	        long stopTime = System.currentTimeMillis();
	        long elapsedTime = stopTime - startTime;
	        
	        System.out.println("elapsedTime: " + elapsedTime + " hours: " + TimeUnit.MILLISECONDS.toHours(elapsedTime) + " minutes: " + TimeUnit.MILLISECONDS.toMinutes(elapsedTime) + " seconds: " + TimeUnit.MILLISECONDS.toSeconds(elapsedTime) );
	        
	    } catch (FileNotFoundException e) {
	    	System.out.println(e.getMessage());
	    }
	}
}
