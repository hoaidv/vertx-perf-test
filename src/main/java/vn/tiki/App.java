package vn.tiki;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import lombok.extern.log4j.Log4j2;

import java.sql.SQLException;

@Log4j2
public class App {

    private static HikariDataSource dataSource;
    private static Vertx vertx;

    public static void main(String[] args) {
        int deploymentSize = CpuCoreSensor.availableProcessors();

        log.info("Available logical processors = Deployment size = {}", deploymentSize);

        dataSource = setupConnectionPool(deploymentSize);
        vertx = Vertx.vertx(new VertxOptions())
                .exceptionHandler(App::vertxExceptionHandler);

        vertx.deployVerticle(
                App::getVoucherVerticle,
                new DeploymentOptions().setInstances(deploymentSize));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vertx.close();
            dataSource.close();
        }));
    }

    private static Verticle getVoucherVerticle() {
        try {
            return new VoucherVerticle(dataSource.getConnection());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
                "autoReconnectForPools=true"
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
