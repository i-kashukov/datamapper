import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MysqlCachingStringMapper implements CachingDataMapper<String> {

    private final String userName;
    private final String password;
    private final String serverName;
    private final String dbName;
    private final String tableName;
    private final int serverPort = 3306;
    private static final int MAX_LENGTH = 100;
    private static final String CONNECTION_ARGS = "useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false";
    private Connection connection;

    private Map<Long,String> localCache = new ConcurrentHashMap<>();
    private long nextFreeId = 0;


    public MysqlCachingStringMapper(String serverName, String dbName, String tableName, String userName, String password) {
        this.serverName = serverName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.userName = userName;
        this.password = password;
        try {
            Properties connectionProps = new Properties();
            connectionProps.put("user", this.userName);
            connectionProps.put("password", this.password);
            String connectionString = String.format("jdbc:mysql://%s:%d/%s?%s",
                    this.serverName,this.serverPort,dbName,CONNECTION_ARGS);
            this.connection = DriverManager.getConnection(connectionString,
                    connectionProps);
            this.resetTable();
        } catch (SQLException ex) {
            System.out.println("SQLException:\n" + ex.toString());
            ex.printStackTrace();
        }
    }

    private void resetTable() throws SQLException {
        Statement st = connection.createStatement();
        st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        st.executeUpdate("CREATE TABLE " +  tableName +
                " (id INT NOT NULL, " +
                "  value VARCHAR(" + MAX_LENGTH + ") NULL, " +
                "  PRIMARY KEY (id));");
        st.close();
        localCache.clear();
    }


    @Override
    public Map<Long, String> getAll() {
        try(PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM " + tableName)) {
            ResultSet resultSet = ps.getResultSet();
            while(resultSet.next()) {
                Long id = resultSet.getLong(1);
                String value = resultSet.getString(2);
                localCache.put(id,value);
                // теперь локальный кэш содержит полную версию БД
                return Collections.unmodifiableMap(localCache);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getById(long id) {
        if(localCache.containsKey(id))
            return localCache.get(id);
        try(PreparedStatement ps = connection.prepareStatement("SELECT * FROM " + tableName + " WHERE id = ?;")) {
            ps.setLong(1,id);
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                String ret = rs.getString(1);
                // обновляем кэш
                localCache.put(id,ret);
                return ret;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public long persist(String s) {
        try(
        PreparedStatement ps = connection.prepareStatement(String.format(
                "INSERT INTO %s (`id`, `value`) VALUES (?, ?);",tableName));
        ) {
            ps.setLong(1, nextFreeId);
            ps.setString(2,s);
            ps.execute();
            localCache.put(nextFreeId, s);
            return nextFreeId++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }


    @Override
    public void delete(String s) {
        if(s == null)
            return;
        try(
                PreparedStatement ps = connection.prepareStatement(String.format(
                        "DELETE FROM %s WHERE value=?;",tableName));
        ) {
            ps.setString(1,s);
            ps.execute();
            localCache.entrySet().removeIf(e -> e.getValue().equals(s));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int size() {
        try(
                PreparedStatement ps = connection.prepareStatement(String.format(
                        "SELECT COUNT(*) FROM %s;",tableName));
        ) {
            ResultSet resultSet = ps.executeQuery();
            if(resultSet.next())
                return resultSet.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
