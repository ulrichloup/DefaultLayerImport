/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Properties;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
/**
 * Event-basierte Ueberwachung aller Import-Verzeichnisse einer Kategorie.
 *
 * @version 2020-03-24
 */
public class ImportObserver {

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;
    private SimpleDateFormat sdf = null;

    public static String configName = "observer.conf";
    /// z.B. "/media/data/geoserver/imp/maps/coverages/radolan/";
    public static String dataPath;
    public static String localJsonpPath;
    public static String proxyJsonpPath;
    /// z.B. /home/webadm/bin/maps/radolan/";
    public static String configPath;

    private Connection conn;
    HashMap<String, String[]> dbMap = new HashMap<String, String[]>();
    String[] tteus = {"jdbc:oracle:thin:@oratteus.dwd.de:9210:TTEUS", "prj_twebgis", "PRJ_TTEUS4us."};
    String[] teus = {"jdbc:oracle:thin:@orateus.dwd.de:9210:TEUS", "prj_webgis", "PRJ_TEUS4us."};
    String[] tlars = {"jdbc:oracle:thin:@oratlars.dwd.de:9210:TLARS", "prj_webgis", "KDnjC45!"};
    String[] lars = {"jdbc:oracle:thin:@oralars.dwd.de:9210:LARS", "prj_webgis", "JrPGibe321"};
    String[] tlarsmig = {"jdbc:oracle:thin:@oratlars.dwd.de:9210:TLARS", "prj_webgis_mig", "N9h5M4w7"};

    public ILayerImport importer;

    public String[] myLayers;

    HashMap<String, String> importerMap = new HashMap<String, String>();
    String importerName;
    HashMap<String, String> layerDBMap = new HashMap<String, String>();

    HashMap<String, Connection> connMap = new HashMap<String, Connection>();
    public HashMap<String, String[]> argMap = new HashMap<String, String[]>();
    String[] myArgs;
    HashMap<String, Boolean> afdMap = new HashMap<String, Boolean>();
    HashMap<String, String> transformationMap = new HashMap<String, String>();
    HashMap<String, String> formatMap = new HashMap<String, String>();
    HashMap<String, RenamingRule> renamingMap;
    boolean hasAfdSubdir;
    HashMap<String, String> seedMap1 = new HashMap<String, String>();
    HashMap<String, String> seedMap2 = new HashMap<String, String>();
    HashMap<String, String> fileMgmgMap = new HashMap<String, String>();
    boolean fileMgmg = false;

    /**
     * Erzeugen des WatchService Anlegen der leeren Hashmap
     *
     * @throws IOException
     */
    ImportObserver() throws IOException {

        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
        this.renamingMap = new HashMap<String, RenamingRule>();
        sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Auslesen der config-datei eines Layers.
     *
     * 1. Zeile Importer-Programm => in hashMap importerMap einfuegen 2. Zeile
     * DB-Name => in hashMap layerDBMap einfuegen 3. Zeile Argumente => String[]
     * => in hashMap argMap einfuegen, ACHTUNG "leer" als Platzhalter bei leeren
     * Argumenten 4. Zeile AFD-Subdir Kennzeichen: AFDSUBDIRON/AFDSUBDIROFF =>
     * afdMap 5. Zeile optional: Flag "TRANSFORM" fÃ¼r die Transformation der
     * Rasterdatei mit gdalwarp z.Zt bei SAT_EU_RGB_CENTRAL_CLOUD 5. Zeile seed1
     * falls caching an und gelÃ¶scht werden soll hier die Argumente ablegen =>
     * in hashMap seedMap1 einfuegen 6. Zeile seed2 falls caching an und ein
     * zweiter Bereich gelÃ¶scht werden soll => in hashMap seedMap2 einfuegen
     *
     * @param path layerName
     */
    public boolean loadLayerConfig(String path, String layerName) {

        BufferedReader br = null;
        File l = new File(path + layerName);
        String line;
        String[] sa;
        String[] sa2;
        boolean b = false;
        int i = 0;
        int len;

        try {
            // System.out.println("Lese Konfiguration von " + layerName + " in " + path);

            br = new BufferedReader(new FileReader(l));

            line = br.readLine();
            if (line == null) {
                System.out.println("Importer " + line + " zu Layer " + layerName + " leer => kein DatenImport!");
                br.close();
                return false;
            }

            // System.out.println("Importer ist " + line);
            importerMap.put(layerName, line);

            line = br.readLine();
            if (line == null) {
                System.out.println("DB " + line + " zu Layer " + layerName + " leer => kein DatenImport!");
                br.close();
                return false;
            }
            if (line.equalsIgnoreCase("TTEUS") || line.equalsIgnoreCase("TEUS") || line.equalsIgnoreCase("TLARS")
                    || line.equalsIgnoreCase("LARS") || line.equalsIgnoreCase("TLARSMIG"))
                // System.out.println("DB ist " + line);
                layerDBMap.put(layerName, line);
            else {
                System.out.println("DB " + line + " zu Layer " + layerName + " existiert nicht=> kein DatenImport!");
                br.close();
                return false;
            }

            line = br.readLine();
            if (line == null) {
                System.out.println("Parameterliste zu Layer " + layerName + " leer => kein DatenImport!");
                br.close();
                return false;
            }
            // sa = line.split(" "); mehrere Blanks stellen auch nur einen Delimiter dar!
            sa = line.trim().split("\\s+");
            // Feld 2 path ersetzen durch den datapath, der beim loadConfig aus
            // observer.conf geholt wurde
            sa[1] = dataPath;
            len = sa.length;
            sa2 = new String[len + 2];
            i = 0;
            // eventuell vorhandene "leer"-Parameter ersetzen durch einen Leerstring
            for (String par : sa) {
                if (par.equalsIgnoreCase("leer"))
                    sa2[i] = "";
                else
                    sa2[i] = sa[i];
                i++;
            }
            sa2[len] = localJsonpPath;
            sa2[len + 1] = proxyJsonpPath;

            argMap.put(layerName, sa2);

            line = br.readLine();
            // System.out.println("AFD Sub-Dir " + line);
            if (line.trim().equalsIgnoreCase("AFDSUBDIRON"))
                b = true;
            afdMap.put(layerName, b);

            line = br.readLine();
            // optionale Zeile TRANSFORM
            if (line != null && line.trim().toUpperCase().startsWith("TRANSFORM")) {
                if (line.length() > 10 && line.charAt(9) == '=') // Argument hinter TRANSFORM= angegeben

                    transformationMap.put(layerName, line.substring(10));
                else
                    transformationMap.put(layerName, "");
                line = br.readLine();
            }
            // optionale Zeile FORMAT
            if (line != null && line.trim().toUpperCase().startsWith("FORMAT")) {
                if (line.length() > 9 && line.charAt(8) == '=') // Argument hinter FORMAT= angegeben

                    formatMap.put(layerName, line.substring(9));
                else
                    formatMap.put(layerName, "");
                line = br.readLine();
            }

            // optionale Zeile RENAME
            if (line != null && line.trim().toUpperCase().startsWith("RENAME")) {
                if (line.length() > 7 && line.charAt(6) == '=') { // Argument hinter RENAME= angegeben
                    Matcher m = Pattern.compile("[^&;]*&[^&;]*&[^&;]*").matcher(line.substring(7));
                    if (m.matches()) {
                        String[] renamingRuleArray = m.group().split("&");
                        if (renamingRuleArray.length == 3)
                            this.renamingMap.put(layerName, new RenamingRule(renamingRuleArray[0], renamingRuleArray[1], renamingRuleArray[2]));
                        else if (renamingRuleArray.length == 2)
                            this.renamingMap.put(layerName, new RenamingRule(renamingRuleArray[0], renamingRuleArray[1], ""));
                    }
                }
                line = br.readLine();
            }

            if (line != null && !line.trim().isEmpty())
                seedMap1.put(layerName, line);
            line = br.readLine();
            if (line != null && !line.trim().isEmpty())
                seedMap2.put(layerName, line);

            br.close();
            return true;

        } catch (Exception e) {
            System.out.println("Fehler " + e);
            return false;
        }
    }

    /**
     * Auslesen aller Layer-Konfigurationen.
     *
     * @param path
     */
    public boolean loadLayers(String path) {
        for (String layerName : myLayers) {
            if (!loadLayerConfig(path, layerName))
                return false;
        }
        return true;
    }

    /**
     * Auslesen der Konfiguration einer Kategorie und aller Layer.
     *
     * @param path
     */
    public boolean loadConfig(String path) {

        SimpleDateFormat sdf = new SimpleDateFormat("d.M.y HH:mm");

        String line;

        // erste Zeile: alle Layernamen die ueberwacht werden sollen
        BufferedReader br = null;
        File l = new File(path + configName);

        System.out.println("Starte ImportObserver: " + sdf.format(new Date()));
        System.out.println("File " + configName + " liegt hier: " + l.getAbsolutePath());

        try {
            br = new BufferedReader(new FileReader(l));

            line = br.readLine();
            if (line == null || line.isBlank())
                throw new Exception("Unerwartete Zeile in Konfigurationsdatei: Layernamen erwartet in Zeile 1");

            myLayers = line.split(" ");

            // zweite Zeile: Import-Verzeichnis, in dem pro Layer die zu importierenden
            // Daten sind
            dataPath = br.readLine();
            if (dataPath == null || dataPath.isBlank())
                throw new Exception("Unerwartete Zeile in Konfigurationsdatei: Daten-Pfad erwartet in Zeile 2");

            // dritte Zeile: Pfad zum Ablegen der jsonp-Dateien im NAS-Bereich des Produktionsservers
            localJsonpPath = br.readLine();
            if (localJsonpPath == null || localJsonpPath.isBlank())
                localJsonpPath = "";

            // vierte Zeile: pfad zum Kopieren der jsonp-Dateien auf den Proxy
            proxyJsonpPath = br.readLine();
            if (proxyJsonpPath == null || proxyJsonpPath.isBlank())
                proxyJsonpPath = "";
            br.close();
        } catch (Exception e) {
            System.out.println("Fehler: " + e.getMessage());
            return false;
        }
        if (loadLayers(path))

            /*
			System.out.println("Ausgabe der Layer-Hashmap: ");
			for (String layerName : argMap.keySet()) {
				System.out.println("layerName " + layerName);
				for (String arg : argMap.get(layerName)) {
					System.out.println("arg " + arg);
				}
			}
			System.out.println("Ausgabe der Importer-Hashmap: ");
			for (String layerName : myLayers) {
				System.out.println("layerName " + layerName + " Importer " + importerMap.get(layerName));
			}
			**/
            return true;
        return false;
    }

    /**
     * Registrieren aller Verzeichnisse = Layer am Watchservice watcher.
     *
     * @param dir
     * @throws IOException
     */
    private void registerDirs() throws IOException {

        WatchKey key;
        Path path;
        String impdir;

        System.out.println("Registrieren der Verzeichnisse in: " + dataPath);

        for (String verz : myLayers) {
            hasAfdSubdir = (boolean) afdMap.get(verz);
            if (hasAfdSubdir)
                // System.out.println("verz " + verz + " hat afd subdir");
                impdir = dataPath + verz + "/afd";
            else
                impdir = dataPath + verz; // System.out.println("verz " + verz + " hat KEIN afd subdir");

            File file = new File(impdir);

            if (file.isDirectory()) {
                path = Paths.get(impdir);
                key = path.register(watcher, ENTRY_CREATE);
                keys.put(key, path);
                System.out.println(impdir + " registriert");
            } else
                System.out.println(impdir + " existiert nicht!");
        }
    }

    /**
     * Aufbau der verwendeten DB-Verbindung(en).
     *
     * @throws ClassNotFoundException, SQLException
     */
    private void initDB() throws ClassNotFoundException, SQLException {

        // Datenbankverbindung herstellen
        // TODO einmal oder mehrmals ausfÃ¼hren? KlÃ¤ren... !!
        Class.forName("oracle.jdbc.driver.OracleDriver");
        // Es gibt 3 DB-Schemata fuer QS und 2 fuer LS

        String dbName;
        for (String layerName : layerDBMap.keySet()) {
            dbName = layerDBMap.get(layerName);
            switch (dbName) {
                case "TTEUS":
                    if (connMap.get("TTEUS") == null) {
                        conn = DriverManager.getConnection(tteus[0], tteus[1], tteus[2]);
                        conn.setAutoCommit(false);
                        connMap.put(dbName, conn);
                        dbMap.put("TTEUS", tteus);
                    }
                    break;
                case "TEUS":
                    if (connMap.get("TEUS") == null) {
                        conn = DriverManager.getConnection(teus[0], teus[1], teus[2]);
                        conn.setAutoCommit(false);
                        connMap.put(dbName, conn);
                        dbMap.put("TEUS", teus);
                    }
                    break;
                case "TLARS":
                    if (connMap.get("TLARS") == null) {
                        conn = DriverManager.getConnection(tlars[0], tlars[1], tlars[2]);
                        conn.setAutoCommit(false);
                        connMap.put(dbName, conn);
                        dbMap.put("TLARS", tlars);
                    }
                    break;
                case "LARS":
                    if (connMap.get("LARS") == null) {
                        conn = DriverManager.getConnection(lars[0], lars[1], lars[2]);
                        conn.setAutoCommit(false);
                        connMap.put(dbName, conn);
                        dbMap.put("LARS", lars);
                    }
                    break;
                case "TLARSMIG":
                    if (connMap.get("TLARSMIG") == null) {
                        conn = DriverManager.getConnection(tlarsmig[0], tlarsmig[1], tlarsmig[2]);
                        conn.setAutoCommit(false);
                        connMap.put(dbName, conn);
                        dbMap.put("TLARSMIG", lars);
                    }
                    break;
                default:
                    System.err.printf("Unbekannte Datenbank: " + dbName + "Layer " + layerName);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    /**
     * Ausgabe eines Prozesses loggen.
     *
     * @param Process proc, String sName
     */
    public void logCache(Process proc, String sName) {
        StringBuilder sb = new StringBuilder();
        String sLine;
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            while ((sLine = in.readLine()) != null) {
                sb.append(sLine);
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (sb.indexOf("removing") == -1)
            System.out.println("- Exception beim Cache Loeschen (" + sName + "):" + sb);
        else
            System.out.println("- Alte " + sName + "-Cachedateien entfernt.");
    }

    /**
     * Ausgabe eines Prozesses loggen.
     *
     * @param proc
     * @param sName
     */
    public void logProcess(Process proc, String sName) {
        StringBuilder sb = new StringBuilder();
        String sLine;
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            while ((sLine = in.readLine()) != null) {
                sb.append(sLine);
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("- " + sb);
    }

    /**
     * Events verarbeiten.
     */
    void processEvents() {

        String importerName;
        String layerName;
        String[] dbargs;
        String[] args;
        File importFile;

        for (;;) {
            // nur bei RX Angabe layername im 6. Argument zur Erzeugung JSONP-Datei
            // String[] rxargsDB = { "RX", pfad, "nn", "YYY", "false", "RX"};
            // String[] wxargsDB = { "WX_PRODUKT", pfad, "nn", "YYY", "false", ""};
            // String[] fxargsDB = { "FX", pfad, "rad", "25", "0", "0", "2", "1", "5", ""};
            // String[] eaargsDB = { "EA_PRODUKT", pfad, "24", "", "false", ""};
            // String[] feargsDB = { "FE_PRODUKT", pfad, "24",
            // "/media/data/geoserver/imp/geoshare/date_fe.txt", "false", ""};
            // String[] sfargsDB = { "SF_PRODUKT", pfad, "72", "", "false", ""};
            // String[] sfdargsDB = { "SF_DAILY_PRODUKT", pfad, "8784", "", "true", ""};

            WatchKey key;
            String fileName;
            String[] command;
            Process proc;
            String dbName;
            String seedname1;
            String seedname2;
            boolean isAfd = false;

            try {
                // wait for keys to be signalled
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("WatchKey nicht registriert!");
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind kind = event.kind();

                // TBD - provide example of how OVERFLOW event is handled
                if (kind == OVERFLOW)
                    continue;

                // Dateinamen und Layer aus dem Pfad extrahieren
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);

                String sChild = child.toString();
                String[] sChildArray = sChild.split(File.separator);

                // bei Tests lokal unter Windows
                // String[] sChildArray = sChild.split("\\\\");
                fileName = sChildArray[sChildArray.length - 1];

                layerName = sChildArray[sChildArray.length - 2];
                if (layerName.equalsIgnoreCase("afd")) {
                    // 3 statt 2 da der event zwar im Verz afd ausgeloest wird aber der layer = Verz
                    // darueber benoetigt wird
                    layerName = sChildArray[sChildArray.length - 3];
                    isAfd = true;
                }

                if (!fileName.startsWith(".")) {
                    System.out.format("%s: %s\n", sdf.format(new Date()), "Neue Datei: " + fileName + " Layer: " + layerName);

                    long lBeg = System.currentTimeMillis();

                    try {
                        dbName = layerDBMap.get(layerName);
                        conn = connMap.get(dbName);
                        if (conn == null) {
                            System.out.println("Fehler, keine DB-Verbindung!");
                            continue;
                        }
                        if (!conn.isValid(1)) {
                            System.out.format("DB-Verbindung ungueltig. Wird neu aufgebaut...");
                            conn.close();
                            dbargs = dbMap.get(dbName);
                            conn = DriverManager.getConnection(dbargs[0], dbargs[1], dbargs[2]);
                            conn.setAutoCommit(false);
                            connMap.put(dbName, conn);
                        }

                        if (isAfd) {

                            // Eventuell bereits vorhandene identische Index-Dateien und Verzeichnisse
                            // entfernen
                            // danach Dateien von afd nach oben
                            // Fixed this line:
                            // File file = new File(dataPath + layerName + "/." + fileName);
                            File file = new File(dataPath + layerName + File.separator + fileName);

                            if (file.isDirectory()) {
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName + " ist ein Verzeichnis. Wird entfernt...");
                                try {
                                    // Process process = Runtime.getRuntime().exec("rm -rf " +
                                    // file.getAbsoluteFile());
                                    Runtime.getRuntime().exec("rm -rf " + file.getAbsoluteFile());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            } else
                                System.out.println(
                                        "File " + file.getName() + " in " + dataPath + " ist KEIN Verzeichnis !!!");

                            file = new File(dataPath + layerName + File.separator + fileName + ".gbx9");
                            if (file.exists()) {
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName
                                        + " existiert. Wird entfernt...");
                                file.delete();
                            } else
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName
                                        + " existiert NICHT !!!");

                            file = new File(dataPath + layerName + File.separator + fileName + ".gbx3");
                            if (file.exists()) {
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName
                                        + " existiert. Wird entfernt...");
                                file.delete();
                            } else
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName
                                        + " existiert NICHT !!!");

                            file = new File(dataPath + layerName + "/afd/" + fileName);

                            String importFileName = fileName;
                            if (renamingMap.containsKey(layerName))
                                importFileName = renamingMap.get(layerName).applyIn(fileName);
                            importFile = new File(dataPath + layerName + File.separator + importFileName);

                            if (transformationMap.containsKey(layerName)) {
                                String transformationCommand = transformationMap.get(layerName).replace("_sourceFile", file.getAbsolutePath());
                                if (formatMap.containsKey(layerName)) {
                                    String filenameExtension = formatMap.get(layerName);
                                    String importPath = importFile.getAbsolutePath();
                                    filenameExtension = filenameExtension.isBlank() ? "tif" : filenameExtension;
                                    Matcher mFilenameExtension = Pattern.compile("\\.[^.]*$").matcher(importPath);
                                    if (mFilenameExtension.find())
                                        importPath = importPath.substring(0, mFilenameExtension.start()) + "." + filenameExtension;
                                    else
                                        importPath = importPath + "." + filenameExtension;
                                    importFile = new File(importPath);
                                }
                                transformationCommand = transformationCommand.replace("_targetFile", importFile.getAbsolutePath());
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName + " transformieren...");
                                System.out.println(transformationCommand);
                                command = new String[]{"/bin/sh", "-c", transformationCommand};
                                proc = Runtime.getRuntime().exec(command);
                                logProcess(proc, layerName);
                                if (!importFile.exists())
                                    throw new Exception("TransformException - folgende Datei konnte nicht transformiert werden: " + file.getName());
                                file.delete();

                            } else {
                                System.out.println("File " + file.getName() + " in " + dataPath + layerName + " von AFD nach oben verschieben");
                                file.renameTo(importFile);
                            }
                        } else
                            importFile = new File(dataPath + layerName + File.separator + fileName);

                        importerName = (String) importerMap.get(layerName);
                        // System.out.println("zugehoeriger Importer " + importerName);
                        args = argMap.get(layerName);

                        try {
                            // eventuell package-Namen ergaenzen, z.b. "filescanner."
                            Class<?> cl = Class.forName(importerName);
                            importer = (ILayerImport) cl.getDeclaredConstructor().newInstance();
                            importer.setConn(conn);
                            importer.setImportFile(importFile);
                            importer.dbImport(args);
                        } catch (Exception e) {
                            System.out.println("Fehler: " + e);
                        }

                        // Caches leeren bei einigen Layern
                        seedname1 = seedMap1.get(layerName);
                        // System.out.println("Layer" + layerName + " seedname1:" + seedname1);
                        if (seedname1 != null) {
                            command = new String[]{"/bin/sh", "-c",
                                "/opt/webcc/mapproxy/bin/mapproxy-seed -f " + seedname1};
// lokal testen:							
//							command = new String[] { "/bin/sh", "-c", 
//									"/home/ckron/workspace/filescanner/src/radolan/mymapproxy-seed " + seedname1 };

                            proc = Runtime.getRuntime().exec(command);
                            logCache(proc, layerName);

                        }
                        seedname2 = seedMap2.get(layerName);
                        // System.out.println("Layer" + layerName + " seedname2:" + seedname2);
                        if (seedname2 != null) {
                            command = new String[]{"/bin/sh", "-c",
                                "/opt/webcc/mapproxy/bin/mapproxy-seed -f " + seedname2};

                            proc = Runtime.getRuntime().exec(command);
                            logCache(proc, layerName);

                        }
                    } catch (Exception e) {
                        System.out.println("Fehler beim Import: " + e.getMessage());
                    }

                    long lEnd = System.currentTimeMillis();
                    System.out.format("%s\n", "- Import von " + fileName + " nach " + (lEnd - lBeg) + "ms abgeschlossen.");
                }
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                // all directories are inaccessible
                if (keys.isEmpty())
                    break;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        
        System.out.println("Testprogram");
        System.out.println("Time ([0-9]{10}).*,format=yyMMddHHmm");
        SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmm");
        System.out.println(df);
        //System.out.println(df.parse("2004011300"));
        Properties indexProperties = new Properties();
        indexProperties.load(new FileReader(new File("indexer.properties")));
        String propertyCollectorsProperty = indexProperties.getProperty("PropertyCollectors");
        int iTimestampFileNameExtractorSPI = propertyCollectorsProperty.indexOf("TimestampFileNameExtractorSPI[") + 30;
        System.out.println(propertyCollectorsProperty);
        System.out.println(propertyCollectorsProperty.substring(iTimestampFileNameExtractorSPI, propertyCollectorsProperty.indexOf("]", iTimestampFileNameExtractorSPI)));
        
        File timeregexFile = new File("regex.properties");
        FileReader timeregexFileReader = new FileReader(timeregexFile);
        Properties timeregexProperties = new Properties();
        timeregexProperties.load(timeregexFileReader);
        
        
        System.out.println("regex=" + timeregexProperties.getProperty("regex"));
        System.out.println("format=" + timeregexProperties.getProperty("regex").split(",format=")[0]);
        System.out.println("format=" + timeregexProperties.getProperty("regex").split(",format=")[1]);
        System.out.println("format=" + timeregexProperties.getProperty("regex").split(",format=")[2]);
        System.out.println("/Testprogram");
        System.exit(0);
        
        if (args.length < 2) {
            System.out.println("Fehler beim Aufruf: Kategorie und Pfad des Konfigurationsverzeichnisses uebergeben!");
            printUsage();
            System.exit(1);
        }
        configPath = args[1];
        System.out.println();
        System.out.println("ImportObserver: " + args.length + " Argumente gelesen");
        for (int i = 0; i < args.length; i++) {
            System.out.println(i + ": " + args[i]);
        }

        ImportObserver importObserver = new ImportObserver();

        if (!importObserver.loadConfig(configPath)) {
            System.out.println("Fehler beim Auslesen der Konfiguration, " + args[0] + " -Observer wird nicht gestartet!");
            System.exit(1);
        }
        // Verzeichnisse zur Ueberwachung registrieren
        importObserver.registerDirs();
        // DB-Connections aufbauen
        importObserver.initDB();
        // neue Dateien erkennen = Events verarbeiten
        importObserver.processEvents();
    }

    private static void printUsage() {
        System.out.println();
        System.out.println("java ImportObserver <LAYER> <PFAD>");
        System.out.println("    <LAYER>: Layer-Name (zur Info)");
        System.out.println("    <PFAD>: Konfigurationsverzeichnis des Layers");
        System.out.println();
        System.out.println("Beispiel: java ImportObserver Radolan /home/webadm/bin/maps/radolan/conf/");
        System.out.println();
    }
}
