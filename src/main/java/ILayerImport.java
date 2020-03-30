/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;

/**
 *
 * @author Ulrich Loup <ulrich.loup@dwd.de>
 */
public interface ILayerImport {

    public void dbImport(String[] args) throws ClassNotFoundException, SQLException, ParseException, IOException;

    public void setConn(Connection conn);

    public void setImportFile(File file);
}
