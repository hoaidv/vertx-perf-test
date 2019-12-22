package vn.tiki;

import com.github.jasync.sql.db.Connection;
import com.github.jasync.sql.db.mysql.MySQLConnectionBuilder;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

@Log4j2
public class App {

    private static Vertx vertx;

    public static void main(String[] args) {
        int deploymentSize = CpuCoreSensor.availableProcessors();
        List<Connection> connections = new ArrayList<>();

        log.info("Available logical processors = Deployment size = {}", deploymentSize);

        vertx = Vertx.vertx(new VertxOptions())
                .exceptionHandler(App::vertxExceptionHandler);

        vertx.deployVerticle(
                () -> {
                    var connection = createConnectionPool();
                    connections.add(connection);
                    return new VoucherVerticle(connection);
                },
                new DeploymentOptions().setInstances(deploymentSize));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vertx.close();
            connections.forEach(Connection::disconnect);
        }));
    }

    private static void vertxExceptionHandler(Throwable t) {
        log.warn("Unhandled exception", t);
    }

    private static Connection createConnectionPool() {
        return MySQLConnectionBuilder.createConnectionPool(
                "jdbc:mysql://" +
                        "0.0.0.0:" +
                        "3306/" +
                        "test_db?" +
                        "user=test_user&" +
                        "password=pipi");
    }
}
