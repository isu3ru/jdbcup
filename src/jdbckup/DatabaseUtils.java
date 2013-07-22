/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package jdbckup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 *
 * @author ISU3RU
 */
public class DatabaseUtils {

    private static Connection con;
    private static String database = "water_billing";
    
    public static Connection getConnection() throws Exception {
        if (con == null || con.isClosed()) {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + database + "?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull", "root", "123");
        }
        return con;
    }

    public static synchronized ResultSet query(String sql) throws Exception {
        return getConnection().createStatement().executeQuery(sql);
    }
}
