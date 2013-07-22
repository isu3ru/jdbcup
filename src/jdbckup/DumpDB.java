/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package jdbckup;

import com.diyatha.isuru.MySQLUtils;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.util.Date;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 *
 * @author Isuru Ranawaka - isuru@diyatha.com
 */
public class DumpDB {

    private static String dbname = "water_billing";
    
    private static String getColumnsString(String tbl) {
        String bl = "";
        try {
            ArrayList<String> colslist = new ArrayList<String>();
            ResultSet cols = DatabaseUtils.query("SHOW COLUMNS FROM `" + tbl + "`");
            while (cols.next()) {
                colslist.add(cols.getString(1));
            }
            ArrayList<String> sql_cols = new ArrayList<String>();
            for (String s : colslist) {
                sql_cols.add("`" + s + "`");
            }
            bl = Utils.implode(",", sql_cols);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bl;
    }

    private static String getSeperateInsertsFortable(Connection c, String tbl) {
        StringBuilder sb = new StringBuilder();
        try {
            ResultSet rss = c.createStatement().executeQuery("SELECT * FROM `" + tbl + "`");
            while (rss.next()) {
                int colCount = rss.getMetaData().getColumnCount();
                if (colCount > 0) {
                    sb.append("INSERT INTO ").append("`").append(tbl).append("`").append(" VALUES(");
                    for (int i = 0; i < colCount; i++) {
                        if (i > 0) {
                            sb.append(",");
                        }
                        String s = "";
                        try {
                            s += "'";
                            s += MySQLUtils.mysql_real_escape_string(c, rss.getObject(i + 1).toString());
                            s += "'";
                        } catch (Exception e) {
                            s = "''";
                        }
                        sb.append(s);
                    }
                    sb.append(");\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private static String getCombinedInsertsForTable(Connection c, String tbl, int limit) {
        StringBuilder sb = new StringBuilder();

        try {
            ResultSet rsalt = c.createStatement().executeQuery("SELECT * FROM `" + tbl + "`");
            rsalt.last();
            int numberOfRows = rsalt.getRow();
            if (numberOfRows > 0) {
                sb.append("INSERT INTO `").append(tbl).append("`(").append(getColumnsString(tbl)).append(")\n VALUES");
            }
            int colCount = rsalt.getMetaData().getColumnCount();
            rsalt.beforeFirst();
            ArrayList datacs = new ArrayList();
            while (rsalt.next()) {

                if (rsalt.getRow() % limit == 0) {
                    sb.append(Utils.implode(",", datacs)).append(";\n");
                    datacs = new ArrayList();
                    sb.append("\nINSERT INTO `").append(tbl).append("`(").append(getColumnsString(tbl)).append(")\n VALUES");
                }

                ArrayList al = new ArrayList();
                for (int i = 0; i < colCount; i++) {
                    String s = "";
                    try {
                        al.add("'" + MySQLUtils.mysql_real_escape_string(c, rsalt.getObject(i + 1).toString()) + "'");
                    } catch (Exception e) {
                        al.add("''");
                    }
                    sb.append(s);
                }

                datacs.add("(" + Utils.implode(",", al) + ")");
            }
            sb.append(Utils.implode(",", datacs)).append(";\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void dumpDatabase(String database, String Filename, boolean database_create, boolean drops, boolean seperateInserts, int limit) {
        try {
            FileWriter fw = new FileWriter(Filename);
            BufferedWriter buff = new BufferedWriter(fw);

            StringBuilder sb = new StringBuilder();
            Connection c = DatabaseUtils.getConnection();

            sb.append("--\n").append("-- -------------------------------------\n").append("-- JDBCup Database Backup Utility\n")
                    .append("-- By Isuru Ranawaka - isu3ru@gmail.com\n")
                    .append("-- Created on ")
                    .append(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a").format(new Date())).append("\n")
                    .append("-- -------------------------------------\n"
                    + "--\n\n");


            if (database_create) {
                sb.append("CREATE DATABASE IF NOT EXISTS `").append(database).append("`;\n\n");
                buff.append(sb);
                sb = new StringBuilder();
            }

            sb.append("SET FOREIGN_KEY_CHECKS=0;");
            sb.append("\n\n");

            ResultSet rs = c.createStatement().executeQuery("SHOW FULL TABLES WHERE Table_type != 'VIEW'");
            while (rs.next()) {
                String tbl = rs.getString(1);
                
                System.err.println("<-------------Outing " + tbl + "------------->\n");

                sb.append("\n");
                sb.append("-- ----------------------------\n")
                        .append("-- Table structure for `").append(tbl)
                        .append("`\n-- ----------------------------\n");

                if (drops) {
                    sb.append("DROP TABLE IF EXISTS `").append(tbl).append("`;\n");
                }

                ResultSet rs2 = c.createStatement().executeQuery("SHOW CREATE TABLE `" + tbl + "`");
                rs2.next();
                String crt = rs2.getString(2) + ";";
                sb.append(crt).append("\n");
                sb.append("\n");
                sb.append("-- ----------------------------\n").append("-- Records for `").append(tbl).append("`\n-- ----------------------------\n");

                buff.append(sb.toString());
                sb = new StringBuilder();

                if (seperateInserts) {
                    /*
                     * Outputs seperate inserts
                     */
                    //<editor-fold defaultstate="collapsed" desc="Seperate Inserts">
                    buff.append(getSeperateInsertsFortable(c, tbl));
                    //</editor-fold>
                } else {
                    /*
                     * Outputs combined inserts
                     */
                    //<editor-fold defaultstate="collapsed" desc="Combined Inserts">
                    buff.append(getCombinedInsertsForTable(c, tbl, limit));
                    //</editor-fold>
                }

            }

            System.gc();
            sb = new StringBuilder();

            ResultSet rs2 = c.createStatement().executeQuery("SHOW FULL TABLES WHERE Table_type = 'VIEW'");
            while (rs2.next()) {
                String tbl = rs2.getString(1);

                sb.append("\n");
                sb.append("-- ----------------------------\n")
                        .append("-- View structure for `").append(tbl)
                        .append("`\n-- ----------------------------\n");
                sb.append("DROP VIEW IF EXISTS `").append(tbl).append("`;\n");
                ResultSet rs3 = c.createStatement().executeQuery("SHOW CREATE VIEW `" + tbl + "`");
                rs3.next();
                String crt = rs3.getString(2) + ";";
                sb.append(crt).append("\n");

                buff.append(sb.toString());
                sb = new StringBuilder();
            }

            buff.flush();
            buff.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            
            String filename = dbname + new SimpleDateFormat("yyyy-MM-dd_hhmmssa").format(new Date());

            String zipf = "C:/" + filename + ".zip";

            File dump = File.createTempFile(filename, ".sql");
            
            System.err.println("<----- Dumping database " + dbname);
            dumpDatabase(dbname, dump.getAbsolutePath(), true, true, true, 1000);
            System.err.println("<----- Done dumping database");

            
            System.err.println("Creating zip file");
            FileOutputStream fos = new FileOutputStream(zipf);
            ZipOutputStream zos = new ZipOutputStream(fos);
            int bytesRead;
            byte[] buffer = new byte[1024];
            CRC32 crc = new CRC32();

            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(dump));
            crc.reset();
            while ((bytesRead = bis.read(buffer)) != -1) {
                crc.update(buffer, 0, bytesRead);
            }
            bis.close();
            // Reset to beginning of input stream
            bis = new BufferedInputStream(new FileInputStream(dump));
            ZipEntry entry = new ZipEntry(dump.getName());
            entry.setMethod(ZipEntry.DEFLATED);
//            entry.setCompressedSize(file.length()); //only for STORED method
            entry.setSize(dump.length());
            entry.setCrc(crc.getValue());
            zos.putNextEntry(entry);
            while ((bytesRead = bis.read(buffer)) != -1) {
                zos.write(buffer, 0, bytesRead);
            }
            bis.close();
            zos.flush();
            zos.finish();
            zos.close();
            
            System.err.println("Done creating zip file");
            System.err.println("process complete");
            
            long end = System.currentTimeMillis();
            
            long time = (end - start) / 1000;
            System.err.println("Process took " + time + " seconds.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
