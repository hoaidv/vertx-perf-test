package vn.tiki;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.extern.log4j.Log4j2;

import java.sql.SQLException;

/**
 * Hello world!
 */
@Log4j2
public class App {

    private static HikariDataSource dataSource;
    private static Vertx vertx;

    public static void main(String[] args) {
        System.out.println("Hello World!");

        int deploymentSize = 15;

        dataSource = setupConnectionPool(deploymentSize);
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40))
                .exceptionHandler(App::vertxExceptionHandler);

        for (int i = 0; i < deploymentSize; i++) {
            vertx.deployVerticle(new VoucherVerticle(vertx, () -> {
                try {
                    return dataSource.getConnection();
                } catch (SQLException e) {
                    throw new RuntimeException("Cannot get connection");
                }
            }));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vertx.close();
            dataSource.close();
        }));
    }

    private static void vertxExceptionHandler(Throwable t) {
        log.warn("Unhandled exception", t);
    }

    private static HikariDataSource setupConnectionPool(int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" +
                "localhost:3306/" +
                "test_db?" +
                "allowMultiQueries=true&" +
                "autoReconnect=true&" +
                "autoReconnectForPools=true&" +
                "useJDBCCompliantTimezoneShift=true&" +
                "useLegacyDatetimeCode=false&" +
                "serverTimezone=UTC&" +
                "zeroDateTimeBehavior=convertToNull&" +
                "tinyInt1isBit=false&" +
                "useSSL=false"
        );

        config.setMaximumPoolSize(maxPoolSize);

        config.setUsername("test_user");
        config.setPassword("pipi");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }
}
