package vn.tiki;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class App {

    private static MySQLPool mySQLPool;
    private static Vertx vertx;

    public static void main(String[] args) {
        int deploymentSize = CpuCoreSensor.availableProcessors();

        log.info("Available logical processors = Deployment size = {}", deploymentSize);

        mySQLPool = setupConnectionPool(deploymentSize);
        vertx = Vertx.vertx(new VertxOptions())
                .exceptionHandler(App::vertxExceptionHandler);

        vertx.deployVerticle(
                () -> new VoucherVerticle(mySQLPool),
                new DeploymentOptions().setInstances(deploymentSize));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vertx.close();
            mySQLPool.close();
        }));
    }

    private static void vertxExceptionHandler(Throwable t) {
        log.warn("Unhandled exception", t);
    }

    private static MySQLPool setupConnectionPool(int maxPoolSize) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setHost("localhost").setPort(3306).setDatabase("test_db")
                .setUser("test_user").setPassword("pipi")
                .setReconnectAttempts(5);

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(maxPoolSize);

        return MySQLPool.pool(connectOptions, poolOptions);
    }
}
