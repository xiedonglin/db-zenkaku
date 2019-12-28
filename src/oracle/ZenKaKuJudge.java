/**
 * 
 */
package oracle;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author 謝東霖
 *
 */
public class ZenKaKuJudge {
    public static Logger log = Logger.getLogger(ZenKaKuJudge.class);
    Connection conn = null;
    Statement statement = null;
    ResultSet rs = null;
    CallableStatement stmt = null;

    public ZenKaKuJudge() {
        init();
    }

    public void init() {
        System.out.println("oracle jdbc test");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("driver is ok");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.3.161:1521:SCHEMA", "USERID", "PASSWORD");
            System.out.println("conection is ok");

            String sql = "SELECT A.TABLE_NAME,A.COLUMN_NAME,'VARCHAR2(' || A.DATA_LENGTH || ')' AS DATATYPE , A.DATA_LENGTH AS LEN ";
            sql = sql + "FROM USER_TAB_COLUMNS A ";
            sql = sql + "INNER JOIN DBA_TABLES B ON A.TABLE_NAME = B.TABLE_NAME ";
            sql = sql + "AND B.OWNER ='ADVUSER' AND INSTR(B.TABLE_NAME,'_WORK') = 0 ";
            sql = sql + "AND INSTR(B.TABLE_NAME,'TMP') = 0 WHERE A.DATA_TYPE = 'VARCHAR2' ";
//            sql = sql + "and b.ini_trans <> 10 and not (b.TEMPORARY = 'N' and b.global_stats ='NO')";
            sql = sql + "ORDER BY A.TABLE_NAME ";

            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            List<String> colList = new ArrayList();
            while (rs.next()) {
                colList.add(rs.getString("TABLE_NAME") + "|" + rs.getString("COLUMN_NAME") + "|" + rs.getString("LEN")
                        + "|" + rs.getString("DATATYPE"));
            }
            rs.close();
            stmt.close();

            String CHECK_SQL = "SELECT FLAG, MAX(LENB) AS LENB,MAX(LEN) AS LEN FROM (SELECT "
                    + "CASE WHEN LENGTHB(#COLUMN_NAME#) = 2 * LENGTH(#COLUMN_NAME#) THEN '0' "
                    + "WHEN LENGTHB(#COLUMN_NAME#) = LENGTH(#COLUMN_NAME#) AND TO_MULTI_BYTE(#COLUMN_NAME#) = #COLUMN_NAME# THEN '1' "
                    + "WHEN LENGTHB(#COLUMN_NAME#) = LENGTH(#COLUMN_NAME#) AND LENGTHB(TO_MULTI_BYTE(#COLUMN_NAME#)) / 2 = LENGTHB(#COLUMN_NAME#) THEN '2' "
                    + "WHEN LENGTHB(#COLUMN_NAME#) = LENGTH(#COLUMN_NAME#) AND TO_MULTI_BYTE(#COLUMN_NAME#) != #COLUMN_NAME# THEN '3' "
                    + "WHEN LENGTHB(#COLUMN_NAME#)!= LENGTH(#COLUMN_NAME#) AND TO_MULTI_BYTE(#COLUMN_NAME#) = #COLUMN_NAME# THEN '4' "
                    + "ELSE '5' END AS FLAG, LENGTHB(#COLUMN_NAME#) AS LENB, LENGTH(#COLUMN_NAME#) AS LEN "
                    + "FROM #TABLE_NAME# WHERE #COLUMN_NAME# IS NOT NULL AND ASCII(#COLUMN_NAME#) <> 0) " + "GROUP BY FLAG";
            for (String s : colList) {
                String columStatus = "";
                String columLENB = "";
                String columLEN = "";
                String columName = s.split("\\|")[1];
                String tableName = s.split("\\|")[0];
                String columType = s.split("\\|")[3];
                int columLen = Integer.valueOf(s.split("\\|")[2]);
                int len = Integer.valueOf(s.split("\\|")[2]);
                if ("ET$004B03450001".equals(tableName) || "ET$013902870001".equals(tableName) || "ET$013E009D0001".equals(tableName) || "SYS_IMPORT_TABLE_01".equals(tableName)){
                    continue;
                }
//                log.trace(CHECK_SQL.replace("#COLUMN_NAME#", columName).replace("#TABLE_NAME#", tableName));
                stmt = conn.prepareStatement(
                        CHECK_SQL.replace("#COLUMN_NAME#", columName).replace("#TABLE_NAME#", tableName));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    columStatus = columStatus + "|" + rs.getString("FLAG");
                    columLENB = columLENB + "|" + rs.getString("LENB");
                    columLEN = columLEN + "|" + rs.getString("LEN");
                }
                rs.close();
                stmt.close();
                
                if (!"".equals(columStatus)) {
                    if (columStatus.split("|").length == 2) {
                        if ("0".equals(columStatus.split("|")[1])) {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2("
                                    + (int) Math.ceil(columLen / 2) + ")" + ",全角" + "," + getColumStatus(columStatus));
                        } else if ("1".equals(columStatus.split("|")[1])) {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2(" + columLen + ")"
                                    + ",半角" + "," + getColumStatus(columStatus));
                        } else if ("2".equals(columStatus.split("|")[1])) {
//                            log.debug(tableName + "," + columName + "," + columType + ",VARCHAR(" + columLen + ")"
//                                    + ",半角" + "," + getColumStatus(columStatus));
                        } else if ("3".equals(columStatus.split("|")[1])) {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2(" + columLen + ")"
                                    + ",半角" + "," + getColumStatus(columStatus));
                        } else if ("4".equals(columStatus.split("|")[1])) {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2("
                                    + (int) Math.ceil(columLen / 2) + ")" + ",全角" + "," + getColumStatus(columStatus));
                        } else if ("5".equals(columStatus.split("|")[1])) {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2("
                                    + (int) Math.ceil(columLen / 2) + ")" + ",全角" + "," + getColumStatus(columStatus));
                        }
                    } else if (columStatus.split("|").length > 2) {
                        if (columStatus.indexOf("0") > 0 || columStatus.indexOf("4") > 0
                                || columStatus.indexOf("5") > 0) {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2("
                                    + (int) Math.ceil(columLen / 2) + ")" + ",全角" + "," + getColumStatus(columStatus));
                        } else {
                            log.debug(tableName + "," + columName + "," + columType + ",NVARCHAR2(" + columLen + ")"
                                    + ",半角" + "," + getColumStatus(columStatus));
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("close");
        }
    }

    private String getColumStatus(String flag) {
        String status = "";
        status = flag.replace("0", "【全角のみ】");
        status = status.replace("1", "【半角カラのみ】");
        status = status.replace("2", "【半角アルファ】");
        status = status.replace("3", "【半角カラ半角アルファ】");
        status = status.replace("4", "【全角半角カラ】");
        status = status.replace("5", "【全角半角アルファ】");
        status = status.replace("|", "");
        return status;
    }

    public static void main(String args[]) {
        new ZenKaKuJudge();
    }
}