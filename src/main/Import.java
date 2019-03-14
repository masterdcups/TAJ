package main;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import utils.Utilitaires;
import jdbc.Connexion;
import lecture.LectureCSV;

public class Import {
	
	final static String PATH = "res/bdTest.csv";
	//final static String PATH = "res/Equipement.csv";
	
	private static final String URL = "jdbc:mysql://localhost:3306/neocampus";

	public static void main(String args[]) throws IOException {

		// Lecture du fichier csv
		List<String> listeEquipement = LectureCSV.readFile(new File(PATH));
		
		// Chargement du driver JDBC pour MySql
		jdbc.Connexion.load();
		
		// Ouverture de la connexion à la base de données
		Connection connexion = Connexion.connexionBase(URL, "root", "");
		
		// Création de l'objet Utilitaire
		Utilitaires utilitaires = new Utilitaires(connexion);
		
		//utilitaires.insertMesure("co2;u4/302/co2/ilot3;275;2017-09-22T16:27:39.511862;ppm;ilot3");
		
		

		
		// Fermeture de la connexion à la base de données
		Connexion.fermetureBase(connexion);
		
	
		
	}

}
