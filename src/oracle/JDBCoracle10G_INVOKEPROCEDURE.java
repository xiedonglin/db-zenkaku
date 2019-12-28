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
/**
 * @author 謝東霖
 *
 */
public class JDBCoracle10G_INVOKEPROCEDURE {
        Connection conn = null;  
        Statement statement = null;  
        ResultSet rs = null;  
        CallableStatement stmt = null;  
      
        String driver;  
        String url;  
        String user;  
        String pwd;  
        String sql;  
        String in_price;  
      
        public JDBCoracle10G_INVOKEPROCEDURE()  
        {  
            driver = "oracle.jdbc.driver.OracleDriver";  
            url = "jdbc:oracle:thin:@localhost:1521:ORCL";  
            // oracle 用户  
            user = "test";  
            // oracle 密码  
            pwd = "test";  
            init();  
            // mysid：必须为要连接机器的sid名称，否则会包以下错：  
            // java.sql.SQLException: Io 异常: Connection  
            // refused(DESCRIPTION=(TMP=)(VSNNUM=169870080)(ERR=12505)(ERROR_STACK=(ERROR=(CODE=12505)(EMFI=4))))  
            // 参考连接方式:  
            // Class.forName( "oracle.jdbc.driver.OracleDriver" );  
            // cn = DriverManager.getConnection(  
            // "jdbc:oracle:thin:@MyDbComputerNameOrIP:1521:ORCL", sUsr, sPwd );  
        }  
      
        public void init() {  
            System.out.println("oracle jdbc test");
            try {
                Class.forName(driver);  
                System.out.println("driver is ok");  
                conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.3.161:1521:SCHEMA", "USERID", "PASSWORD");
                System.out.println("conection is ok");  

                String sql = "SELECT A.TABLE_NAME,A.COLUMN_NAME FROM user_tab_columns A ";
                sql =sql+ "INNER JOIN DBA_TABLES B ON A.TABLE_NAME = B.TABLE_NAME ";
                sql =sql+ "AND B.OWNER ='SPUSER' AND INSTR(B.TABLE_NAME,'_WORK') = 0 ";
                sql =sql+ "AND INSTR(B.TABLE_NAME,'_TMP') = 0 WHERE A.DATA_TYPE = 'VARCHAR2'";

                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                List<String> list = new ArrayList();
                while (rs.next()) {  
                    list.add(rs.getString("TABLE_NAME") + "|" + rs.getString("COLUMN_NAME"));
                } 
                rs.close();
                stmt.close();
                
                CallableStatement statement=conn.prepareCall("{?=call fn_knachg(?)}");
                for (String s:list){
                    String columName = s.split("\\|")[1];
                    String tableName = s.split("\\|")[0];
                    stmt = conn.prepareStatement("SELECT DISTINCT " + columName + " FROM "+ tableName);
                    rs = stmt.executeQuery();
                    while (rs.next()) {
                        statement.registerOutParameter(1, java.sql.Types.NUMERIC);  
                        statement.setString(2, rs.getString(columName));  
                        if (!statement.execute()) {
                            if (statement.getInt(1)>0){
                                System.out.println("------------" + tableName + "------------");
                                System.out.println(columName);
                                break;
                            }
                         }
                    }
                    rs.close();
                    stmt.close();
                }

            } catch (Exception e) {  
                e.printStackTrace();  
            } finally {  
                System.out.println("close ");  
            }  
        }  
      
        public static void main(String args[])// 自己替换［］  
        {  
            new JDBCoracle10G_INVOKEPROCEDURE();  
        }  
}
