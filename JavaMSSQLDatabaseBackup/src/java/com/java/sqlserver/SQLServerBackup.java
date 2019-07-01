package com.java.sqlserver;



import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import net.sourceforge.jtds.jdbcx.JtdsDataSource;



import javax.sql.DataSource;



import java.io.FileWriter;

import java.io.PrintWriter;

import java.sql.Connection;

import java.sql.ResultSet;

import java.sql.ResultSetMetaData;

import java.sql.SQLException;

import java.sql.Statement;

import java.sql.Types;

import java.util.stream.Stream;

import java.io.*;

import java.sql.*;

import java.text.SimpleDateFormat;

public class SQLServerBackup {



    //1rItrEX PROD instance  => 1rDBSWP0608CLS

    //1rItrEX Stage instance => 1rDBSWS0458

private static final SimpleDateFormat dateFormat = 

            new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");                          

    private static final String SQLSERVER_FQDN = "1DBSWS0458";

    private static final String SQLSERVER_DB = "ORX_WFM";

    private static final int SQLSERVER_PORT = 1433;

    /**

     * The domain user account information.

     */

    private static final String SQL_DOMAIN = "MS";

    private static final String SQL_USER = "tum_re";

    private static final String SQL_PASSWORD = "FGbNJn7F";

    

    public SQLServerNTLMAuthMain() {

        super();

    }

/*

    public static void main(String[] args) throws Exception {

        Connection dbConnection = null;

         try {

                System.out.println("load driver");

                Class.forName("net.sourceforge.jtds.jdbc.Driver");

                log.info("loaded");



                String con = "jdbc:jtds:sqlserver://PNT00-PMP-SQL01:1433/iceware;domain=workgroup;userName=user;password=password";



                dbConnection = DriverManager.getConnection(con);



                log.info("got connection");



               return dbConnection;



          } catch (Exception e) {

                log.error(e.getMessage());

          }

    }

*/

    

    

    public static void main(String[] args) {

        try {

            //Stream.of(getMicrosoftDataSource()).forEach((ds) -> {

            Stream.of(getJtdsDataSource()).forEach((ds) -> {

                try (Connection conn = ds.getConnection()) {

                    conn.setAutoCommit(true);

                    try (Statement stmt = conn.createStatement()) {

                       // stmt.execute("SELECT 1");

                     

                        ResultSet rs = stmt.executeQuery("select table_name from information_schema.tables");

                        while (rs.next()) {

                            String tableName = rs.getString("table_name");

                           // System.out.println(tableName);

                            Statement stmt2 = conn.createStatement();

                            String query = "declare @vsSQL varchar(8000)\r\n" + 

                            "declare @vsTableName varchar(50)\r\n" + 

                            "select @vsTableName = '"+tableName+"'\r\n" + 

                            "\r\n" + 

                            "select @vsSQL = 'CREATE TABLE ' + @vsTableName + char(10) + '(' + char(10)\r\n" + 

                            "\r\n" + 

                            "select @vsSQL = @vsSQL + ' ' + sc.Name + ' ' +\r\n" + 

                            "st.Name +\r\n" + 

                            "case when st.Name in ('varchar','varchar','char','nchar') then '(' + cast(sc.Length as varchar) + ') ' else ' ' end +\r\n" + 

                            "case when sc.IsNullable = 1 then 'NULL' else 'NOT NULL' end + ',' + char(10)\r\n" + 

                            "from sysobjects so\r\n" + 

                            "join syscolumns sc on sc.id = so.id\r\n" + 

                            "join systypes st on st.xusertype = sc.xusertype\r\n" + 

                            "where so.name = @vsTableName\r\n" + 

                            "order by\r\n" + 

                            "sc.ColID\r\n" + 

                            "\r\n" + 

                            "select substring(@vsSQL,1,len(@vsSQL) - 2) + char(10) + ')'";

                            //ResultSet rs2 = stmt2.executeQuery("sp_help "+tableName);

                            Statement stmt3= conn.createStatement();

                            

                            ResultSet rs2 = stmt2.executeQuery(query);

                            String createQuery = null;

                            while (rs2.next()) {

                            createQuery = rs2.getString(1);

                            System.out.println(createQuery);

                            }

                            

                            

                            generateInsertStatements(conn,tableName,createQuery,false);

                           // String columnName = rs.getString("column_name");

                           // System.out.println(tableName + "\t" + columnName);

                        }

                    //String strSelect = "BACKUP DATABASE ORX_WFM TO DISK = '/Users/skarthee/myFile' ";

                        //ResultSet rset = stmt.executeQuery(strSelect);

                    

                    } catch (Exception e) {

// TODO Auto-generated catch block

e.printStackTrace();

}

                } catch (SQLException ex) {

                    throw new RuntimeException(ex);

                }

            });

            System.out.println("PASS");

        } catch (RuntimeException ex) {

            System.out.println("FAIL");

            ex.printStackTrace();

        }

    }

    private static void generateInsertStatements(Connection conn, String tableName,String createQuery,boolean sameFile) 

            throws Exception {

System.out.println("Generating Insert statements for: " + tableName);

Statement stmt = conn.createStatement();

ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName); 

ResultSetMetaData rsmd = rs.getMetaData();

int numColumns = rsmd.getColumnCount();

int[] columnTypes = new int[numColumns];

String columnNames = "";

for (int i = 0; i < numColumns; i++) {

columnTypes[i] = rsmd.getColumnType(i + 1);

if (i != 0) {

    columnNames += ",";

}

columnNames += rsmd.getColumnName(i + 1);

}



java.util.Date d = null;

PrintWriter p = null;

if(sameFile==true)

p= new PrintWriter(new FileWriter( "ddl_create_insert.sql"));

else

 p= new PrintWriter(new FileWriter(tableName + "_insert.sql"));

p.println(createQuery);

p.println("set sqlt off");

p.println("set sqlblanklines on");

p.println("set define off");

while (rs.next()) {

String columnValues = "";

for (int i = 0; i < numColumns; i++) {

    if (i != 0) {

        columnValues += ",";

    }



    switch (columnTypes[i]) {

        case Types.BIGINT:

        case Types.BIT:

        case Types.BOOLEAN:

        case Types.DECIMAL:

        case Types.DOUBLE:

        case Types.FLOAT:

        case Types.INTEGER:

        case Types.SMALLINT:

        case Types.TINYINT:

            String v = rs.getString(i + 1);

            columnValues += v;

            break;



        case Types.DATE:

            d = rs.getDate(i + 1); 

        case Types.TIME:

            if (d == null) d = rs.getTime(i + 1);

        case Types.TIMESTAMP:

            if (d == null) d = rs.getTimestamp(i + 1);



            if (d == null) {

                columnValues += "null";

            }

            else {

                columnValues += "TO_DATE('"

                          + dateFormat.format(d)

                          + "', 'YYYY/MM/DD HH24:MI:SS')";

            }

            break;



        default:

            v = rs.getString(i + 1);

            if (v != null) {

                columnValues += "'" + v.replaceAll("'", "''") + "'";

            }

            else {

                columnValues += "null";

            }

            break;

    }

}

p.println(String.format("INSERT INTO %s (%s) values (%s)\n/", 

                        tableName,

                        columnNames,

                        columnValues));

}

p.close();

}

    private static DataSource getJtdsDataSource() {

        JtdsDataSource ds = new JtdsDataSource();

        ds.setServerName(SQLSERVER_FQDN);

        ds.setPortNumber(1433);

        ds.setAppName(SQLServerNTLMAuthMain.class.getSimpleName());

        ds.setDatabaseName(SQLSERVER_DB);

        ds.setDomain(SQL_DOMAIN);

        ds.setUser(SQL_USER);

        ds.setPassword(SQL_PASSWORD);

        ds.setUseNTLMV2(true);



        return ds;

    }



    /**

     * This is supposed to configure an equivalent DataSource as in the above {@link #getJtdsDataSource()} method.

     * Note that I have also tried using user@domain format in the setUser() method, to no avail. Also, this needs

     * to function cross-platform (that is, it can't depend on sqljdbc_auth.dll being present to function correctly).

     */

    private static DataSource getMicrosoftDataSource() {

        SQLServerDataSource ds = new SQLServerDataSource();

        ds.setServerName(SQLSERVER_FQDN);

        ds.setPortNumber(SQLSERVER_PORT);

        ds.setApplicationName(SQLServerNTLMAuthMain.class.getSimpleName());

        ds.setDatabaseName(SQLSERVER_DB);

        ds.setUser(SQL_DOMAIN + "\\" + SQL_USER);

        ds.setPassword(SQL_PASSWORD);

        ds.setAuthentication("ActiveDirectoryPassword");



        return ds;

    }

}

