package Postgres;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import java.sql.PreparedStatement;
import java.sql.SQLException;
//import java.sql.SQLException;
import java.sql.Types;

public class ImportCSVFamilia {
	public static void main(String[] args) throws Exception {
		
		Connection conn = ConnectionManager.getConnection();
		System.out.println(conn);
	    int count = 0;
	    String file = "D:/base_amostra_familia_201712-20180411-145946.csv";

	    try(BufferedReader br = new BufferedReader(new FileReader(file))) {
	        String line = "";
	        PreparedStatement stmt_erro = conn.prepareStatement("INSERT INTO bfa.familia_descarte (id, error) VALUES (?, ?)");
	        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO bfa.familia" +
	        		"(	id, cd_ibge, id_estrato, id_subdivisao_unid_fed, dat_cadastramento_fam, dat_alteracao_fam, vlr_renda_media_fam," +
						"dat_atualizacao_familia, id_local_domic_fam, id_especie_domic_fam, qtd_comodos_domic_fam, qtd_comodos_dormitorio_fam," +
						"id_material_piso_fam, id_material_domic_fam, ind_agua_canalizada_fam, id_abaste_agua_domic_fam, ind_banheiro_domic_fam," +
						"id_escoa_sanitario_domic_fam, id_destino_lixo_domic_fam, id_iluminacao_domic_fam, id_calcamento_domic_fam, ind_familia_indigena_fam," +
						"ind_familia_quilombola_fam, nom_estab_assist_saude_fam, cod_eas_fam, nom_centro_assist_fam, cod_centro_assist_fam, id_ind_parc_mds_fam," +
						"qtd_pessoas, peso_fam, marc_pbf)" +
	        		" VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
	        long startTime = System.currentTimeMillis();
	        while ((line = br.readLine()) != null) {
	        	if (count > 0) {
	        		
			        String[] linha = line.split(";");
		            try {
			            pstmt.setInt(1, Integer.parseInt(linha[0]));
			            pstmt.setString(2, linha[1].replace("\"", ""));
			            pstmt.setInt(3, Integer.parseInt(linha[2]));
			            pstmt.setInt(4,  Integer.parseInt(linha[3]));
			            pstmt.setDate(5, java.sql.Date.valueOf(linha[4].replace("\"", "")));
			            pstmt.setDate(6, java.sql.Date.valueOf(linha[4].replace("\"", "")));
			            pstmt.setInt(7, Integer.parseInt(linha[6]));
			            pstmt.setDate(8, java.sql.Date.valueOf(linha[4].replace("\"", "")));
			            
			            if (linha[8].trim().equals("NA")) {
			            	pstmt.setNull(9, Types.INTEGER);
			            }else {
			            	pstmt.setInt(9, Integer.parseInt(linha[8]));
			            }
			            
			            if (linha[9].trim().equals("NA")) {
			            	pstmt.setNull(10, Types.INTEGER);
			            }else {
			            	pstmt.setInt(10, Integer.parseInt(linha[9]));
			            }
	
			            if (linha[10].trim().equals("NA")) {
			            	pstmt.setNull(11, Types.INTEGER);
			            }else {
			            	pstmt.setInt(11, Integer.parseInt(linha[10]));
			            }
			            
			            if (linha[11].trim().equals("NA")) {
			            	pstmt.setNull(12, Types.INTEGER);
			            }else {
			            	pstmt.setInt(12, Integer.parseInt(linha[11]));
			            }
			            
			            if (linha[12].trim().equals("NA")) {
			            	pstmt.setNull(13, Types.INTEGER);
			            }else {
			            	pstmt.setInt(13, Integer.parseInt(linha[12]));
			            }
	
			            if (linha[13].trim().equals("NA")) {
			            	pstmt.setNull(14, Types.INTEGER);
			            }else {
			            	pstmt.setInt(14, Integer.parseInt(linha[13]));
			            }
	
			            if (linha[14].trim().equals("NA")) {
			            	pstmt.setNull(15, Types.INTEGER);
			            }else {
			            	pstmt.setInt(15, Integer.parseInt(linha[14]));
			            }
	
			            if (linha[15].trim().equals("NA")) {
			            	pstmt.setNull(16, Types.INTEGER);
			            }else {
			            	pstmt.setInt(16, Integer.parseInt(linha[15]));
			            }
	
			            if (linha[16].trim().equals("NA")) {
			            	pstmt.setNull(17, Types.INTEGER);
			            }else {
			            	pstmt.setInt(17, Integer.parseInt(linha[16]));
			            }
	
			            if (linha[17].trim().equals("NA")) {
			            	pstmt.setNull(18, Types.INTEGER);	            	
			            } else {
			            	pstmt.setInt(18, Integer.parseInt(linha[17]));
			            }
			            
			            if (linha[18].trim().equals("NA")) {
			            	pstmt.setNull(19, Types.INTEGER);	            	
			            } else {
			            	pstmt.setInt(19, Integer.parseInt(linha[18]));
			            }
	
			            if (linha[19].trim().equals("NA")) {
			            	pstmt.setNull(20, Types.INTEGER);	            	
			            } else {
			            	pstmt.setInt(20, Integer.parseInt(linha[19]));
			            }
			            
			            if (linha[20].trim().equals("NA")) {
			            	pstmt.setNull(21, Types.INTEGER);	            	
			            } else {
			            	pstmt.setInt(21, Integer.parseInt(linha[20]));
			            }
	
			            if (linha[21].trim().equals("NA")) {
			            	pstmt.setNull(22, Types.INTEGER);	            	
			            } else {
			            	pstmt.setInt(22, Integer.parseInt(linha[21]));
			            }
	
			            if (linha[22].trim().equals("NA")) {
			            	pstmt.setNull(23, Types.INTEGER);
			            } else {
			            	pstmt.setInt(23, Integer.parseInt(linha[22]));
			            }
			            pstmt.setString(24, linha[23].replace("\"", ""));
			            pstmt.setString(25, linha[24].replace("\"", ""));
			            pstmt.setString(26, linha[25].replace("\"", ""));
			            pstmt.setString(27, linha[26].replace("\"", ""));
	
			            if (linha[27].trim().equals("NA")) {          	
			            	pstmt.setNull(28, Types.INTEGER);	            	
			            } else {
			            	pstmt.setInt(28, Integer.parseInt(linha[27].replace("000", "0")));
			            }
			            
			            if (linha[28].trim().equals("NA")) {
			            	pstmt.setNull(29, Types.INTEGER);
			            } else {
			            	pstmt.setInt(29, Integer.parseInt(linha[28]));		            	
			            }
	
			            if (linha[29].trim().equals("NA")) {
			            	pstmt.setNull(30, Types.FLOAT);
			            } else {
			            	pstmt.setFloat(30, Float.parseFloat(linha[29].replace(",", ".")));
			            }
			            
			            pstmt.setString(31, linha[30].replace("\"", ""));
			            
			            pstmt.executeUpdate();
			            
	        		} catch (IllegalArgumentException | SQLException e) {
	        			stmt_erro.setInt(1, Integer.parseInt(linha[0]));
	        			stmt_erro.setString(2, e.toString() +": "+ e.getMessage());
	        			stmt_erro.executeUpdate();
	        		}
	        	}
	        	count++;
	        	
	        }
	        
	        long stopTime = System.currentTimeMillis();
	        long elapsedTime = stopTime - startTime;
	        
	        System.out.println("elapsedTime: " + elapsedTime + " hours: " + TimeUnit.MILLISECONDS.toHours(elapsedTime) + " minutes: " + TimeUnit.MILLISECONDS.toMinutes(elapsedTime) + " seconds: " + TimeUnit.MILLISECONDS.toSeconds(elapsedTime) );
	        
	    } catch (FileNotFoundException e) {
	    	System.out.println(e.getMessage());
	    }
		
		conn.close();
	}
	

}
