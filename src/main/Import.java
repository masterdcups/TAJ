package main;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import utils.Utilitaires;
import jdbc.Connexion;
import lecture.LectureCSV;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime;
import org.json.*;

public class Import extends Thread {
	
	final static String PATH = "res/bdTest.csv";
	
	private static final String URL = "jdbc:mysql://localhost:3306/neocampus";
	
	  static Thread thread = new Thread();
	  
	public static void main(String args[]) throws IOException {

		// Lecture du fichier csv
		//List<String> listeDonnees = LectureCSV.readFile(new File(PATH));
		
		System.out.println("Fin de lecture");
		
		// Chargement du driver JDBC pour MySql
		jdbc.Connexion.load();
		
		// Ouverture de la connexion à la base de données
		Connection connexion = Connexion.connexionBase(URL, "root", "");
		
		// Création de l'objet Utilitaire
		Utilitaires utilitaires = new Utilitaires(connexion);
		
		
		//utilitaires.insertMesure("co2;u4/302/co2/ilot3;275;2017-09-22T16:27:39.511862;ppm;ilot3");
		//utilitaires.importBatch(listeDonnees);
		
		
		// Fermeture de la connexion à la base de données
		Connexion.fermetureBase(connexion);
		
	
		

		
		Import threadMeteo=new Import();  
		threadMeteo.start();  
	}
	
	
	
	public void run(){  
		while(true) {
			laMeteo();
			try {
				TimeUnit.MINUTES.sleep(30);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static void laMeteo() {
		
		System.out.println("----------------------------------------------------------------------------------");
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		System.out.println(dtf.format(now));
		
		String str = "";
		
		try {
			
			URL url = new URL("http://api.openweathermap.org/data/2.5/weather?q=Toulouse,fr&appid=8a9d70954f30547a3fee0c7dedaaa742");
			BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
			str = br.readLine();
			br.close();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println(str);
		
		//transformer la string en JSON
		JSONObject obj = new JSONObject(str);
		
		String nom = obj.getString("name");
		System.out.println(nom);
		System.out.println(" ");
		
		//Extraire une "sous-chaine"
		JSONObject lemain = obj.getJSONObject("main");
		//System.out.println(lemain);
		
		//Un élément de cette sous-chaine
		float temperatureKel = lemain.getFloat("temp");
		float temperatureCel = (float) (temperatureKel-273.15);
		System.out.println("temperature = "+temperatureCel);
		
		int humidite = lemain.getInt("humidity");
		System.out.println("humidité = "+humidite);
		
		int pression = lemain.getInt("pressure");
		System.out.println("pression = "+pression);
		
		//Objets imbriqués
		JSONArray temps = obj.getJSONArray("weather");
		//System.out.println(temps);
		JSONObject weather = temps.getJSONObject(0);
		//System.out.println(weather);
		String nuages = weather.getString("main");
		System.out.println("Nuages = "+nuages);
		String description = weather.getString("description");
		System.out.println("Temps général = "+description);
		
		

		JSONObject sys = obj.getJSONObject("sys");
		
		long sunrise = sys.getInt("sunrise");
		Date dateSunrise = new Date(sunrise*1000);
		System.out.println("Sunrise = "+dateSunrise);
		
		long sunset = sys.getInt("sunset");
		Date dateSunset = new Date(sunset*1000);
		System.out.println("Sunset = "+dateSunset);
		
		
		PreparedStatement stmt = null;
		Connection connexion = null;
		
		try {
			stmt = connexion.prepareStatement("INSERT INTO mesure (temperature, humidite, pression, nuage, tempsGeneral, sunrise, sunset) VALUES (?, ?, ?, ?, ?, ?, ?)");
			stmt.setFloat(1, temperatureCel);
			stmt.setInt(2, humidite);
			stmt.setInt(3, pression);
			stmt.setString(4, nuages);
			stmt.setString(5, description);
			stmt.setObject(6, dateSunrise);
			stmt.setObject(7, dateSunset);
			stmt.executeUpdate();
			
			
		} catch (SQLException e) {
			System.out.println("Erreur lors de l'insertion d'une météo");
			e.printStackTrace();
		}
	}
}
