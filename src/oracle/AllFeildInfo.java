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

import org.apache.log4j.Logger;

/**
 * @author 謝東霖
 *
 */
public class AllFeildInfo {
    public static Logger log = Logger.getLogger(AllFeildInfo.class);
    Connection conn = null;
    Statement statement = null;
    ResultSet rs = null;
    CallableStatement stmt = null;

    public AllFeildInfo() {
        init();
    }

    public void init() {
        System.out.println("oracle jdbc test");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("driver is ok");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.3.161:1521:SCHEMA", "USERID", "PASSWORD");
            System.out.println("conection is ok");

            String sql = "SELECT A.TABLE_NAME,A.COLUMN_NAME,A.Data_type, A.DATA_LENGTH AS LEN,A.DATA_PRECISION ,A.DATA_SCALE ";
            sql = sql + "FROM USER_TAB_COLUMNS A ";
            sql = sql + "INNER JOIN DBA_TABLES B ON A.TABLE_NAME = B.TABLE_NAME ";
            sql = sql + "AND B.OWNER ='ADVUSER' AND INSTR(B.TABLE_NAME,'_WORK') = 0 ";
            sql = sql + "AND INSTR(B.TABLE_NAME,'TMP') = 0 ";
//            sql = sql + "and b.ini_trans <> 10 and not (b.TEMPORARY = 'N' and b.global_stats ='NO')";
            sql = sql + "ORDER BY A.TABLE_NAME ";

            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                if ("VARCHAR2".equals(rs.getString("Data_type"))){
                    log.debug(rs.getString("TABLE_NAME") + "|" + rs.getString("COLUMN_NAME") + "|" + rs.getString("Data_type") + "(" + rs.getString("LEN") + ")");
                }else if("NUMBER".equals(rs.getString("Data_type"))){
                    log.debug(rs.getString("TABLE_NAME") + "|" + rs.getString("COLUMN_NAME") + "|" + rs.getString("Data_type") + "(" + rs.getString("DATA_PRECISION") + ", " + rs.getString("DATA_SCALE")+")");
                }else if("DATE".equals(rs.getString("Data_type"))){
                    log.debug(rs.getString("TABLE_NAME") + "|" + rs.getString("COLUMN_NAME") + "|" + rs.getString("Data_type"));
                }
            }
            rs.close();
            stmt.close();
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
        new AllFeildInfo();
    }
}