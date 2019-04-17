package utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Utilitaires {
	
	private final int NB_BATCH_BEFORE_INSERT = 1000;	// Nombbre de batch effectué avant d'executer la requête
	
	private Connection connexion;
	private HashMap<String, Integer> salles;	// Salle + batiment
	private HashMap<String, Integer> typesMesure;	// Libellé + unité
	private HashMap<String, Integer> batiments;		// libellé
	private HashMap<String, Integer> ilots;		// Libellé + nomSalle + nomBatiment
	
	/**
	 * Constructeur de la classe Utilitaire
	 * @param connexion
	 */
	public Utilitaires(Connection connexion) {
		this.connexion = connexion;
		salles = this.getSalles();
		typesMesure = this.getTypesMesure();
		ilots = this.getIlots();
		batiments = this.getBatiments();
	}
	
	/**
	 * Permet de formater les données, du format initial au nouveau format
	 * @param ligne : la ligne à formater
	 * @return une liste d'objet représentant les infos importante pour l'ajout
	 */
	private List<Object> dataParser(String ligne) {
		
		List<Object> listResult = new ArrayList<Object>();
		
		// Découpage de la ligne lue
		String[] ligneDecoupee = ligne.split(";");	// Exemple de ligne : co2;u4/302/co2/ilot3;275;2017-09-22T16:27:39.511862;ppm;ilot3
		String[] infoIlot = ligneDecoupee[1].split("/");	// Exemple de ligneDecoupee[1] : u4/302/co2/ilot3
		
		String typeMesure = ligneDecoupee[0];
		String batiment = infoIlot[0];
		String salle = infoIlot[1];
		String ilot = infoIlot[3];
		float value = Float.parseFloat(ligneDecoupee[2]);
		String date = ligneDecoupee[3];
		String uniteMesure = ligneDecoupee[4];
		
		// Récupérer l'identifiant du type mesure
		int idTypeMesure = -1;
		// Si présent dans le HashMap
		if (typesMesure.containsKey(typeMesure + uniteMesure)) {
			idTypeMesure = typesMesure.get(typeMesure + uniteMesure);
		} else {	// Sinon faire une requête SQL qui va chercher la valeur dans la base de donnée et l'ajouter dans la hashmap pour éviter de futures requêtes
			idTypeMesure = getTypeMesure(typeMesure, uniteMesure);
		}
		
		// Récupérer l'identifiant de l'ilot
		int idIlot = -1;
		if (ilot.contains(ilot + salle)) {		// Recherche dans la HashMap
			idIlot = ilots.get(ilot + salle);
		} else {
			idIlot = getIlot(ilot, salle, batiment);	// Recherche dans la base de données
		}
		
		// Formatage de la date et de l'heure
		date = date.replace("T", " ");
		
		Timestamp dateF = Timestamp.valueOf(date);
		
		listResult.add(value);
		listResult.add(dateF);
		listResult.add(idTypeMesure);
		listResult.add(idIlot);
		
		return listResult;
		
	}
	
	/**
	 * Prend une arrayList en argument et les insère dans la base de données par paquet de NB_BATCH_BEFORE_INSERT
	 * @param listeEquipement
	 * @return
	 * @throws SQLException 
	 */
	public void importBatch(List<String> listeEquipement) throws SQLException {
		
		// Désactivation de l'autoCommit
		//connexion.setAutoCommit(false);

		int compteurBatch = 0;
		
		List<Object> listResult = new ArrayList<Object>();
		
		PreparedStatement stmt = null;

		try {
			stmt = connexion.prepareStatement("INSERT INTO mesure (valeur, date, typemesure, ilot) VALUES (?, ?, ?, ?)");


			// Parcours de l'ensemble des lignes du fichier
			for (int i = 0 ; i < listeEquipement.size(); i++) {
				
				compteurBatch ++;
				
				// On récupère la ligne, on la parse pour récupérer les bonnes infos
				listResult = dataParser(listeEquipement.get(i));
				
				float value = (float) listResult.get(0);
				Timestamp dateF = (Timestamp) listResult.get(1);
				int idTypeMesure = (int) listResult.get(2);
				int idIlot = (int) listResult.get(3);
				
				if (compteurBatch <= NB_BATCH_BEFORE_INSERT) {
					stmt.setFloat(1, value);
					stmt.setObject(2, dateF);
					stmt.setInt(3, idTypeMesure);
					stmt.setInt(4, idIlot);
					stmt.addBatch();
				} else {
					stmt.executeBatch();	// On execéute le batch
					compteurBatch = 0;
					//connexion.commit();
				}
			}

		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'une mesure");
			//connexion.rollback();	// Erreur lors de l'insertion du batch, on annule le batch complet ?
			e.printStackTrace();
		}		
		
	}


	/**
	 * Prend en entrée une ligne de mesure, la découpe, récupère les différents identifiants nécessaires,
	 * identifiant de l'ilot, identifiant du type de la mesure, dans la hashmap correspondante si cette dernière est présente
	 * envoi une requête à la base sinon.
	 * @param ligne
	 * @return
	 */
	public int insertMesure(String ligne) {
		
		// Découpage de la ligne lue
		String[] ligneDecoupee = ligne.split(";");	// Exemple de ligne : co2;u4/302/co2/ilot3;275;2017-09-22T16:27:39.511862;ppm;ilot3
		String[] infoIlot = ligneDecoupee[1].split("/");	// Exemple de ligneDecoupee[1] : u4/302/co2/ilot3
		
		String typeMesure = ligneDecoupee[0];
		String batiment = infoIlot[0];
		String salle = infoIlot[1];
		String ilot = infoIlot[3];
		float value = Float.parseFloat(ligneDecoupee[2]);
		String date = ligneDecoupee[3];
		String uniteMesure = ligneDecoupee[4];
		
		// Récupérer l'identifiant du type mesure
		int idTypeMesure = -1;
		// Si présent dans le HashMap
		if (typesMesure.containsKey(typeMesure + uniteMesure)) {
			idTypeMesure = typesMesure.get(typeMesure + uniteMesure);
		} else {	// Sinon faire une requête SQL qui va chercher la valeur dans la base de donnée et l'ajouter dans la hashmap pour éviter de futures requêtes
			idTypeMesure = getTypeMesure(typeMesure, uniteMesure);
		}
		
		// Récupérer l'identifiant de l'ilot
		int idIlot = -1;
		if (ilot.contains(ilot + salle + batiment)) {		// Recherche dans la HashMap
			idIlot = ilots.get(ilot + salle + batiment);
		} else {
			idIlot = getIlot(ilot, salle, batiment);	// Recherche dans la base de données	
		}
		
		// Formatage de la date et de l'heure
		date = date.replace("T", " ");
		
		Timestamp dateF = Timestamp.valueOf(date);
		
		PreparedStatement stmt = null;
		
		// Requête
		try {
			stmt = connexion.prepareStatement("INSERT INTO mesure (valeur, date, typemesure, ilot) VALUES (?, ?, ?, ?)");
			stmt.setFloat(1, value);
			stmt.setObject(2, dateF);
			stmt.setInt(3, idTypeMesure);
			stmt.setInt(4, idIlot);
			stmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'une mesure");
			e.printStackTrace();
		}		
		
		System.out.println();
		// Tester si tout est ok avant d'insert sinon annuler l'insertion ?
		
		return 0;
	}
	
	/**
	 * 
	 * @param ilot
	 * @param salle
	 * @param batiment
	 * @return
	 */
	private int insertIlot(String ilot, String salle, String batiment) {
		
		int result = -1;
		int id = -1;
		
		PreparedStatement stmt = null;
		
		// Récupérer l'identifiant de la salle
		int idSalle = -1;
		// Chercher dans le hashmap
		if (salles.containsKey(salle + batiment)) {
			idSalle = salles.get(salle + batiment);
		} else {	// Chercher en base de données
			idSalle = getSalle(salle, batiment);
		} 
		
		if (idSalle == -1) {	// Si toujours pas trouvé, on insère dans la base de données et retourner l'identifiant de la salle
			// Insérer dans la base de données et retourner l'identifiant
			idSalle = insertSalle(salle, batiment);
		}
		
		
		try {
			stmt = connexion.prepareStatement("INSERT INTO ilot (libelle, salle) VALUES (?, ?)");
			stmt.setString(1, ilot);
			stmt.setInt(2, idSalle);
			result = stmt.executeUpdate();
			
			// On récupère l'identifiant généré
			id = getIlot(ilot, salle, batiment);	
			
			// Ajout dans la hashMap
			ilots.put(ilot + salle + batiment, result);
			
		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'un ilot");
			e.printStackTrace();
		}
		
		// Si l'ilot a été ajouté correctement dans la base de donnée
		if (result > 0) {
			// On recherche l'identifiant dans la base de données
			id = getIlot(ilot, salle, batiment);
		}
		return id;
	}
	
	/**
	 * 
	 * @param nomSalle
	 * @param batiment
	 * @return
	 */
	private int insertSalle(String nomSalle, String batiment) {
		
		PreparedStatement stmt = null;
		int id = -1;
		
		// Récupérer l'identifiant du batiment
		int idBatiment = getBatiment(batiment);
		
		// Insérer dans la base de données
		try {
			stmt = connexion.prepareStatement("INSERT INTO salle (nomSalle, batiment) VALUES (?, ?)");
			stmt.setString(1, nomSalle);
			stmt.setInt(2, idBatiment);
			stmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'une salle dans la base de données.");
			e.printStackTrace();
		}

		// Rretourner l'identifiant de la salle insérée
		id = getSalle(nomSalle, batiment);
		
		// Ajouter dans la hashMap pour éviter de future requêtes sur cette données
		salles.put(nomSalle + batiment, id);
		
		return id;
	}
	
	/**
	 * 
	 * @param libelle
	 * @return
	 */
	private int insertBatiment(String libelle) {
		
		PreparedStatement stmt = null;
		int id = -1;
		
		// insérer dans la base de données
		try {
			stmt = connexion.prepareStatement("INSERT INTO batiment (libelle) VALUES (?)");
			stmt.setString(1, libelle);
			stmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'un batiment");
			e.printStackTrace();
		}
		
		// Retourner l'identifiant du batiment
		id = getBatiment(libelle);
		
		// Ajout dans la HashMap
		batiments.put(libelle, id);
		
		return id;
	}
	
	/**
	 * 
	 * @param libelle
	 * @param unite
	 * @return
	 */
	private int insertTypeMesure(String libelle, String unite) {
		PreparedStatement stmt = null;
		int id = -1;
		
		// Insérer dans la base de données
		try {
			stmt = connexion.prepareStatement("INSERT INTO (libelle, unite) VALUES (?, ?)");
			stmt.setString(1, libelle);
			stmt.setString(2, unite);
			stmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'un type de mesure.");
			e.printStackTrace();
		}
		
		// Retourner l'identifiant du type de mesure
		id = getTypeMesure(libelle, unite);
		
		// Ajout dans la HashMap*
		typesMesure.put(libelle + unite, id);
		
		return id;
	}

	/**
	 * 
	 * @param ilot
	 * @param salle
	 * @param batiment
	 * @return
	 */
	private int getIlot(String ilot, String salle, String batiment) {
				
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		// Récupérer l'identifiant de la salle
		int idSalle = getSalle(salle, batiment);
		
		try {	// Utilisation de requête préparée
			String requete = "SELECT ilot.id FROM ilot JOIN salle ON ilot.salle = salle.id WHERE ilot.libelle = ? AND ilot.salle = ?";
			
			
			stmt = connexion.prepareStatement(requete);
			stmt.setString(1, ilot);
			stmt.setInt(2, idSalle);
			result = stmt.executeQuery();
			
			while (result.next()) {
				idResult = result.getInt("ilot.id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute dans la hashmap
			if (idResult != -1 && !ilots.containsKey(ilot + salle + batiment)) {
				ilots.put(ilot + salle + batiment, idResult);
			}
			
			// Si le résultat n'est pas dans la base de données, on l'ajoute 
			if (idResult == -1 ) {
				// Ajouter dans la base de données et récupérer l'id du batiment et de la salle
				idResult = insertIlot(ilot, salle, batiment);
			}
			
			
		} catch (SQLException e) {
			System.out.println("Erreur lors de la recherche d'un ilot dans la base.");
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du resultat de la requête getIlot()");
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du statement de la requête getIlot()");
					e.printStackTrace();
				}
			}
		}		
		return idResult;
	}

	/**
	 * Retourne l'ensemble des ilots présents en base de données dans une hashmap
	 * @return
	 */
	private HashMap<String, Integer> getIlots() {
		
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		ResultSet results = null;
		try {
			PreparedStatement stmt = connexion.prepareStatement("SELECT ilot.id, ilot.libelle, salle.nomSalle, batiment.libelle "
																+ "FROM ilot JOIN salle ON salle.id = ilot.salle "
																+ "JOIN batiment ON salle.batiment = batiment.id");
			results = stmt.executeQuery();
			
			while (results.next()) {
				int id = results.getInt("ilot.id");
				String ilotLibelle = results.getString("ilot.libelle");
				String nomSalle = results.getString("salle.nomSalle");
				String batiment = results.getString("batiment.libelle");
				resultats.put(ilotLibelle + nomSalle + batiment, id);
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			System.out.println("Utilitaires.getIlots() - Erreur lors de la récupération des ilots.");
			e.printStackTrace();
		}
		
		return resultats;
	}

	/**
	 * Récupère l'ensemble des salles présentent dans la base de données
	 * et retourne le résultat sous forme de HashMap<Key, Value>, avec la 
	 * value correspondant à l'identifiant de la salle et la Key à son nom 
	 * combiné à son batiment pour obtenir une valeur unique
	 * @param connexion
	 * @return HashMap<Key, Value>
	 */
	public HashMap<String, Integer> getSalles() {
		
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		ResultSet results = null;
		try {
			Statement stmt = connexion.createStatement();
			results = stmt.executeQuery("SELECT salle.id, salle.nomSalle, batiment.libelle FROM `salle` JOIN batiment ON salle.batiment = batiment.id;");
			
			while (results.next()) {
				int id = results.getInt("salle.id");
				String batimentLibelle = results.getString("batiment.libelle");
				String nomSalle = results.getString("salle.nomSalle");
				resultats.put(nomSalle + batimentLibelle, id);
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			System.out.println("Utilitaires.getSalles() - Erreur lors de la récupération des salles.");
			e.printStackTrace();
		}
		
		
		
		return resultats;
	}

	/**
	 * Récupère l'identifiant d'une salle en base de données à l'aide de son nom et de celui de son batiment
	 * @param nomBatiment
	 * @return
	 */
	public int getSalle(String nomSalle, String nomBatiment) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requête préparée
			stmt = connexion.prepareStatement("SELECT salle.id, salle.nomSalle, batiment.libelle FROM salle JOIN batiment ON batiment.id = salle.batiment WHERE salle.nomSalle = ? AND batiment.libelle = ?");
			stmt.setString(1, nomSalle);
			stmt.setString(2, nomBatiment);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("salle.id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute
			if (idResult != -1 && !ilots.containsKey(nomSalle + nomBatiment)) {
				typesMesure.put(nomSalle + nomBatiment, idResult);
			}
			
			// Si la valeur n'est pas présente dans la base de données
			if (idResult == -1) {
				// Ajouter dans la base de données et récupérer l'identifiant du batiment
				idResult = insertSalle(nomSalle, nomBatiment);
			}
			
			
		} catch (SQLException e) {
			System.out.println("Erreur lors de la requête de sélection d'une salle.");
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du résultat de la requête getSalle().");
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du statement de la requête getSalle().");
					e.printStackTrace();
				}
			}
		}
		
		return idResult;
		
	}
	
	/**
	 * 
	 * @return
	 */
	public HashMap<String, Integer> getBatiments() {
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		ResultSet results = null;
		
		try {
			Statement stmt = connexion.createStatement();
			results = stmt.executeQuery("SELECT * FROM batiment");
			
			while(results.next()) {
				resultats.put(results.getString("libelle"), results.getInt("id"));
			}
			
			stmt.close();
		} catch (SQLException e) {
			System.out.println("Erreur lors de la sélection des batiments.");
			e.printStackTrace();
		}	
		
		return resultats;
	}
	
	/**
	 * 
	 * @param nomBatiment
	 * @return
	 */
	public int getBatiment(String nomBatiment) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requête préparée
			stmt = connexion.prepareStatement("SELECT id FROM batiment WHERE batiment.libelle = ?");
			stmt.setString(1, nomBatiment);
			result = stmt.executeQuery();
			
			while (result.next()) {
				idResult = result.getInt("id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute
			if (idResult != -1 && !batiments.containsKey(nomBatiment)) {
				typesMesure.put(nomBatiment, idResult);
			}
			
			// Si le batiment n'est pas présent dans la base de données
			if (idResult == -1) {
				// Ajouter le batiment et retourne l'identifiant du batiment ajouté
				idResult = insertBatiment(nomBatiment);
			}
			
		} catch (SQLException e) {
			System.out.println("Erreur lors de la requête getBatiment().");
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du résultat de la requête getBatiment().");
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du statement de la requête getBatiment().");
					e.printStackTrace();
				}
			}
		}
		
		return idResult;
	}

	/**
	 * 
	 * @return
	 */
	public HashMap<String, Integer> getTypesMesure() {
		
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();

		ResultSet results = null;
		
		try {
			Statement stmt = connexion.createStatement();
			results = stmt.executeQuery("SELECT * FROM typemesure");
			
			while (results.next()) {
				int id = results.getInt("id");
				String libelle = results.getString("libelle");
				String unite = results.getString("unite");
				
				resultats.put(libelle + unite, id);	// Concaténation du nom et du type pour éviter les problèmes avec la luminosité.
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			System.out.println("Utilitaires.getTypesMesure() - Erreur lors de la récupération des types de mesure.");
			e.printStackTrace();
		}
		
		return resultats;
	}

	
	/**
	 * Récupère une seule valeur dans la base de données, retourne l'identifiant de cette dernière mais ajoute également cette valeur
	 * dans la hashMap typeMesure pour éviter de futures requêtes sur cette même valeur
	 * @param libelle
	 * @param unite
	 * @return
	 */
	public int getTypeMesure(String libelle, String unite) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requête préparée
			stmt = connexion.prepareStatement("SELECT id FROM typemesure WHERE libelle = ? AND unite = ?");
			stmt.setString(1, libelle);
			stmt.setString(2, unite);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute
			if (idResult != -1 && !typesMesure.containsKey(libelle+unite)) {
				typesMesure.put(libelle + unite, idResult);
			}
			
			// Si la valeur n'est pas présente dans la base de données
			if (idResult == -1) {
				// Ajouter le type de mesure dans la base de données et retourner l'identifiant du type de mesure ajouté dans la base de données
				idResult = insertTypeMesure(libelle, unite);
			}
			
			
		} catch (SQLException e) {
			System.out.println("Erreur lors de la requête getTypeMesure().");
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture des résultats de la requête getTypeMesure().");
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					System.out.println("Erreur lors de la fermeture du statement de la requête getTypeMesure().");
					e.printStackTrace();
				}
			}
		}
		return idResult;
	}
}
