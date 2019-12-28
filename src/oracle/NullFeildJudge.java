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
public class NullFeildJudge {
    public static Logger log = Logger.getLogger(NullFeildJudge.class);
    Connection conn = null;
    Statement statement = null;
    ResultSet rs = null;
    CallableStatement stmt = null;

    public NullFeildJudge() {
        init();
    }

    public void init() {
        System.out.println("oracle jdbc test");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("driver is ok");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.3.161:1521:SCHEMA", "USERID", "PASSWORD");
            System.out.println("conection is ok");

            String sql = "SELECT A.TABLE_NAME,A.COLUMN_NAME,A.Data_type,A.DATA_LENGTH AS LEN ";
            sql = sql + "FROM USER_TAB_COLUMNS A ";
            sql = sql + "INNER JOIN DBA_TABLES B ON A.TABLE_NAME = B.TABLE_NAME ";
            sql = sql + "AND B.OWNER ='SPUSER' AND INSTR(B.TABLE_NAME,'_WORK') = 0 ";
            sql = sql + "AND INSTR(B.TABLE_NAME,'TMP') = 0 ";
//            sql = sql + "and b.ini_trans <> 10 and not (b.TEMPORARY = 'N' and b.global_stats ='NO')";
            sql = sql + "ORDER BY A.TABLE_NAME ";

            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            List<String> colList = new ArrayList();
            while (rs.next()) {
                colList.add(rs.getString("TABLE_NAME") + "|" + rs.getString("COLUMN_NAME") + "|" + rs.getString("LEN")
                        + "|" + rs.getString("Data_type"));
            }
            rs.close();
            stmt.close();
            
            String CHECK_TABLE_SQL = "SELECT COUNT(1) AS CNT FROM #TABLE_NAME#";
            String CHECK_SQL = "SELECT COUNT(DISTINCT #COLUMN_NAME#) AS CNT FROM #TABLE_NAME#";
            for (String s : colList) {
                boolean isResult = false;
                String columName = s.split("\\|")[1];
                String tableName = s.split("\\|")[0];
                String columType = s.split("\\|")[3];
                if ("ET$004B03450001".equals(tableName) || "ET$013902870001".equals(tableName) || "ET$013E009D0001".equals(tableName) || "SYS_IMPORT_TABLE_01".equals(tableName)){
                    continue;
                }
                stmt = conn.prepareStatement(
                        CHECK_TABLE_SQL.replace("#TABLE_NAME#", tableName));
                rs = stmt.executeQuery();
                while (rs.next()) {
                    if (rs.getInt("CNT") > 0) {
                        isResult = true;
                    }
                }
                rs.close();
                stmt.close();

                if (isResult) {
                    stmt = conn.prepareStatement(
                            CHECK_SQL.replace("#COLUMN_NAME#", columName).replace("#TABLE_NAME#", tableName));
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        if (rs.getInt("CNT") == 0) {
                            log.debug(tableName + "," + columName + "," + columType + ",値がなし" );
                        }else if (rs.getInt("CNT") == 1){
                            log.debug(tableName + "," + columName + "," + columType + ",値が同じ" );
                        }
                    }                    
                }
                rs.close();
                stmt.close();
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
        new NullFeildJudge();
    }
}