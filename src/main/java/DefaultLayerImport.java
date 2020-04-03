/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class to import meta data into a GeoServer DB for newly imported files
 * in a file-based data source.
 * 
 * @author Ulrich Loup <ulrich.loup@dwd.de>
 * @version 2020-03-24
 */
public class DefaultLayerImport implements ILayerImport {

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");
    private static final String UTC_DEFAULT_PATTERN = "yyyyMMddHHmm";
    private static Pattern TIMEREGEX_DEFAULT_PATTERN = Pattern.compile("[0-9]{12}");
    private static String sNASPath = "/media/data/geoserver/imp/maps/jsonp/";	// alt
    
    private final static String FILEEXTENSION = ".tif";
    
    private Connection conn;
    private File importFile;
    private String layerName;
    private File dir;
    private long seconds;
    private String target_date;
    private boolean dateType;
    private String sName;
    private String proxyJsonpPath;
    private String localJsonpPath;
    private Date maxDate;
    private int maxFID;
    private long maxTime;
    private Pattern timeregexPattern;
    private String utc_pattern;
    
    // Fuer das Instantiieren dieser Klasse mittels reflect im Observer
    // wird ein Leer-Konstruktor benÃ¶tigt
    // 
    // DB-Connection dort mit der set-Methode setzen !
    public DefaultLayerImport() {
        this.dateType = false;
        this.maxDate = new Date(0);
        this.maxFID = 0;
        this.maxTime = -1l;
        this.timeregexPattern = TIMEREGEX_DEFAULT_PATTERN;
        this.utc_pattern = UTC_DEFAULT_PATTERN;
    }

//	public DefaultLayerImport(Connection dbconn) {
//
//		if (dbconn != null) {
//			setConn(dbconn);
//		}
//	}
//
    public void dbImport(String[] args) throws ClassNotFoundException, SQLException, ParseException, IOException {

        File[] list;

        if (args.length < 6) {
            System.out.println("Fehlende Eingabeparameter.");
            System.out.println("  -- Layername");
            System.out.println("  -- Layer-Verzeichnis");
            System.out.println("  -- Verfuegbarkeitsdauer in Stunden");
            System.out.println("  -- Zieldatei Datum (\"\", wenn nicht erwuenscht)");
            System.out.println("  -- Datumstyp verwenden");
            System.out.println("  -- Layernamen falls JSONP-Datei erzeugt werden soll - sonst Leerstring!");
            System.out.println("  -- local JSONP-Path falls JSONP-Datei erzeugt werden soll - sonst Leerstring!");
            System.out.println("  -- proxy JSONP-Path falls JSONP-Datei erzeugt werden soll - sonst Leerstring!");
            System.exit(0);
        } else {
            System.out.println("Starte Layer-Import mit " + args[0]);
        }

        setLayerName(args[0]);
        setDir(args[1] + layerName);  // dataPath aus ImportObserver / observer.conf
        loadTimeregex();
        setSeconds(args[2]);
        setTarget_date(args[3]);
        setDateType(args[4]);
        setsName(args[5]);
        if (args.length >= 7) {
            setLocalJsonpPath(args[6]);
        } else {
            setLocalJsonpPath("");
        }
        if (args.length >= 8) {
            setProxyJsonpPath(args[7]);
        } else {
            setProxyJsonpPath("");
        }

        // Ermittle maximale FID und maximales Datum in der Katalogtabelle
        String stmtString = "SELECT MAX(FID), MAX(INGESTION) FROM " + layerName;
        PreparedStatement stmt = conn.prepareStatement(stmtString);
        ResultSet rs = stmt.executeQuery();
        if (rs.next() && rs.getString(1) != null) {
            maxFID = rs.getInt(1);
            maxDate = rs.getTimestamp(2);
        }
        rs.close();
        stmt.close();

        // Bestimme alle neuen Dateien
        list = new_files();

        // Hinzufuegen von neuen Eintraegen in die Katalogtabelle des Layers
        add_new_elements(list);

        // Bestimme alle zu alten Dateien
        list = old_files();

        // Loesche Eintraege in der Katalogtabelle des Layers
        delete_old_elements(list);

        // Loesche die zu alten Dateien
        delete_old_files(list);

        // conn.close();
        // write date
        if ((!target_date.isEmpty()) && (maxTime >= 0)) {
            try {
                PrintWriter writer = new PrintWriter(target_date, "UTF-8");
                SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");
                sdf.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                writer.println(sdf.format(maxTime));
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // JSONP-Dateien erzeugen
        writeJSONP(sName);

        System.out.println("Fertig");
    }

    private File[] new_files() {
        // Erzeuge einen Filefilter fuer Dateien, deren Zeitstempel juenger als das Maximaldatum ist
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                final String filename = file.getName();
                if ((filename.endsWith(FILEEXTENSION)) && (!filename.startsWith("."))) {
                    final Matcher matcher = DefaultLayerImport.this.timeregexPattern.matcher(filename);
                    if (matcher.find()) {
                        final String match = matcher.group();
                        try {
                            Date timestamp = getTimeFormat(DefaultLayerImport.this.utc_pattern).parse(match);
                            if (dateType) {
                                long days = timestamp.getTime() / (24 * 3600 * 1000);
                                timestamp = new Date(days * 24 * 3600 * 1000);
                            }
                            if (timestamp.after(maxDate)) {
                                System.out.println(layerName + " timestamp " + timestamp + " der Datei " + filename + " ist aktueller als letzter Eintrag in der DB: " + maxDate);
                                return true;
                            } else {
                                System.out.println(layerName + " timestamp " + timestamp + " der Datei " + filename + " ist aelter als letzter Eintrag in der DB: " + maxDate);
                                return false;
                            }
                        } catch (ParseException e) {
                            System.out.println(e.getMessage());
                        }
                    }
                }
                return false;
            }
        };

        return dir.listFiles(fileFilter);
    }

    private File[] old_files() {

        // Erzeuge einen Filefilter fuer Dateien, deren Zeitstempel
        // aelter als das Maximaldatum - days ist
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                final String filename = file.getName();
                if (filename.endsWith(FILEEXTENSION)) {
                    final Matcher matcher = DefaultLayerImport.this.timeregexPattern.matcher(filename);
                    if (matcher.find()) {
                        final String match = matcher.group();
                        try {
                            final Date timestamp = getTimeFormat(DefaultLayerImport.this.utc_pattern).parse(match);
                            if (timestamp.before(new Date(maxDate.getTime() - seconds + 1))) {
                                return true;
                            }
                        } catch (ParseException e) {
                            System.out.println(e.getMessage());
                        }
                    }
                }
                return false;
            }
        };

        return dir.listFiles(fileFilter);
    }

    private void add_new_elements(File[] list) throws SQLException, ParseException {
        String filename;
        Date timestamp;
        Matcher matcher;

        if (list != null) {
            // Fuege die Eintraege zur Katalogtabelle hinzu
            // Geometrie-Wert wird dabei vom Eintrag mit FID = maxFID uebernommen
            // Falls dieser Eintrag nicht existiert, finden keine Einfuegungen statt.
            String stmtString = "INSERT INTO " + layerName + " SELECT ?,THE_GEOM,?,? FROM " + layerName + " WHERE FID = ?";
            PreparedStatement stmt = conn.prepareStatement(stmtString);
            
            for (int i = 0; i < list.length; i++) {
                filename = list[i].getName();
                System.out.println("DB-Import: " + filename);
                matcher = this.timeregexPattern.matcher(filename);
                if (matcher.find()) {
                    timestamp = getTimeFormat(this.utc_pattern).parse(matcher.group());
                    if (maxTime < timestamp.getTime()) {
                        maxTime = timestamp.getTime();
                    }
                    stmt.setInt(1, maxFID + i + 1); // set FID
                    stmt.setString(2, filename); // set LOCATION
                    // set INGESTION
                    if (dateType) {
                        stmt.setDate(3, new java.sql.Date(timestamp.getTime()));
                    } else {
                        stmt.setTimestamp(3, new java.sql.Timestamp(timestamp.getTime()));
                    }
                    stmt.setInt(4, maxFID);
                    if( stmt.executeUpdate() > 0 )
                        System.out.println("+ " + filename);
                }
            }

            stmt.close();
            conn.commit();
        }
    }

    private void delete_old_elements(File[] list) throws SQLException {
        String filename;

        if (list != null) {
            // Loesche die Eintraege aus der Katalogtabelle
            String stmtString = "DELETE FROM " + layerName + " WHERE LOCATION = ?";
            PreparedStatement stmt = conn.prepareStatement(stmtString);

            for (int i = 0; i < list.length; i++) {
                filename = list[i].getName();
                stmt.setString(1, filename); // set LOCATION
                stmt.executeUpdate();
                System.out.println("- " + filename);
            }

            stmt.close();
            conn.commit();
        }
    }

    private void delete_old_files(File[] list) {
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                list[i].delete();
            }
        }
    }

    private void writeJSONP(String sName) {
        if ((!sName.isEmpty()) && (!localJsonpPath.isEmpty()) && (maxTime >= 0)) {
            write_date(sName);
            write_time(sName);
        }
    }

    private void write_date(String sName) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
        String sFile = localJsonpPath + "date/" + sName + ".jsonp";
        String sContent = "set" + sName.replaceAll("-", "") + "Date({\"date\":\"" + sdf.format(maxTime) + "\"});";
        write_file(sFile, sContent, "date");
    }

    private void write_time(String sName) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(tz);

        try {
            String stmtString = "SELECT DISTINCT INGESTION FROM " + layerName + " ORDER BY INGESTION";
            PreparedStatement stmt = conn.prepareStatement(stmtString);
            ResultSet rs = stmt.executeQuery();
            StringBuilder sbTime = new StringBuilder();
            sbTime.append("set");
            sbTime.append(sName.replaceAll("-", ""));
            sbTime.append("Time");
            sbTime.append("({\"time\":[");
            int length = sbTime.length();
            String sTime;
            while (rs.next()) {
                sTime = df.format(new Date(rs.getTimestamp(1).getTime()));
                if (sbTime.length() > length) {
                    sbTime.append(",");
                }
                sbTime.append("\"");
                sbTime.append(sTime);
                sbTime.append("\"");
            }
            sbTime.append("]});");
            rs.close();
            stmt.close();
            String sFile = localJsonpPath + "time/" + sName + ".jsonp";
            write_file(sFile, sbTime.toString(), "time");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void write_file(String sFile, String sContent, String sType) {
        try {
            PrintWriter writer = new PrintWriter(sFile, "UTF-8");
            writer.println(sContent);
            writer.close();
            if (!proxyJsonpPath.isEmpty()) {
                String[] str = new String[]{"/bin/sh", "-c", "/usr/bin/scp " + sFile + " " + proxyJsonpPath + sType + "/."};
                Process p = Runtime.getRuntime().exec(str);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Redefines {@link #timeregexPattern} by loading the file timeregex.properties from {@link #dir}.
     */
    private void loadTimeregex() throws FileNotFoundException, IOException {
        File timeregexFile = new File(this.dir.getAbsolutePath() + File.separator + "timeregex.properties");
        if (!timeregexFile.exists())
            timeregexFile = new File(this.dir.getAbsolutePath() + File.separator + "regex.properties");
        if (!timeregexFile.exists())
            throw new FileNotFoundException("Sowohl timeregex.properties als auch regex.properties nicht gefunden in " + this.dir);
        FileReader timeregexFileReader = new FileReader(timeregexFile);
        Properties timeregexProperties = new Properties();
        timeregexProperties.load(timeregexFileReader);
        String timeregex = timeregexProperties.getProperty("regex").replaceFirst(",[a-z,A-Z]*=.*", "");
        if (timeregex != null) {
            if (timeregex.startsWith(".*"))
                timeregex = timeregex.substring(2);
            if (timeregex.endsWith(".*"))
                timeregex = timeregex.substring(0,timeregex.length()-2);
            this.timeregexPattern = Pattern.compile( timeregex );
        }
        else
            this.timeregexPattern = TIMEREGEX_DEFAULT_PATTERN;
        this.utc_pattern = timeregexProperties.getProperty("format", UTC_DEFAULT_PATTERN).replaceFirst(",[a-z,A-Z]*=.*", "");
    }
    
    // public methods
    
    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public void setImportFile(File file) {
        this.importFile = file;
    }

    public File getImportFile() {
        return importFile;
    }

    public String getLayerName() {
        return layerName;
    }

    public void setLayerName(String layerName) {
        this.layerName = layerName;
    }

    public Date getMaxDate() {
        return maxDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public int getMaxFID() {
        return maxFID;
    }

    public void setMaxFID(int maxFID) {
        this.maxFID = maxFID;
    }

    public long getSeconds() {
        return seconds;
    }

    public void setSeconds(String seconds) {
        this.seconds = Long.parseLong(seconds) * 3600 * 1000;
    }

    public File getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = new File(dir);
    }

    public boolean isDateType() {
        return dateType;
    }

    public void setDateType(String dateType) {
        this.dateType = Boolean.getBoolean(dateType);
    }

    public String getsName() {
        return sName;
    }

    public void setsName(String sName) {
        this.sName = sName;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(long maxTime) {
        this.maxTime = maxTime;
    }

    public String getTarget_date() {
        return target_date;
    }

    public void setTarget_date(String target_date) {
        this.target_date = target_date;
    }

    public String getProxyJsonpPath() {
        return proxyJsonpPath;
    }

    public void setProxyJsonpPath(String proxyJsonpPath) {
        this.proxyJsonpPath = proxyJsonpPath;
    }

    public String getLocalJsonpPath() {
        return localJsonpPath;
    }

    public void setLocalJsonpPath(String localJsonpPath) {
        this.localJsonpPath = localJsonpPath;
    }
    
    /**
     * Returns a {@link SimpleDateFormat} using the UTC_DEFAULT_PATTERN and the UTC time zone
     */
    public SimpleDateFormat getTimeFormat(String utc_pattern) {
        final SimpleDateFormat df = new SimpleDateFormat(utc_pattern);
        df.setTimeZone(UTC_TIME_ZONE);
        return df;
    }
}
