package vn.tiki;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

@Log4j2
public class VoucherVerticle extends AbstractVerticle {

    private final DslJson<Object> json = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());
    private Vertx vertx;
    private Supplier<Connection> connectionProvider;
    private Connection connection;

    public VoucherVerticle(Vertx vertx, Supplier<Connection> connectionProvider) {
        this.vertx = vertx;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void start() {
        this.connection = connectionProvider.get();
        Router router = Router.router(vertx);
        router.route("/vouchers/:id")
                .handler(this::voucherHandler)
                .failureHandler(rtx -> log.error(rtx.failure()));

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(8000, res -> {
                    if (res.succeeded()) {
                        log.info("Successfully started http server {}", this);
                    } else {
                        log.error("Failed to start http server", res.cause());
                    }
                });
    }

    @Override
    public void stop() throws Exception {
        connection.close();
    }

    private void voucherHandler(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        String rawVoucherId = routingContext.pathParam("id");

        if (rawVoucherId == null || rawVoucherId.isBlank()) {
            response.end(errorToBuffer(0x0001, "Missing voucher id."));
            return;
        }

        int voucherId = Integer.parseInt(rawVoucherId);
        response.end(findVoucher(voucherId));
    }

    private Buffer findVoucher(int voucherId) {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM voucher WHERE id = ?")) {
            statement.setInt(1, voucherId);
            statement.execute();
            return readFindVoucherResult(statement);
        } catch (SQLException | IOException e) {
            log.error(e);
            return errorToBuffer(0x0004, e.toString());
        }
    }

    private Buffer readFindVoucherResult(PreparedStatement statement) throws SQLException, IOException {
        try (ResultSet resultSet = statement.getResultSet()) {
            if (resultSet.next()) {
                return Buffer.buffer(resultSetToVoucher(resultSet));
            }
            return errorToBuffer(0x0003, "No voucher found.");
        }
    }

    private byte[] resultSetToVoucher(ResultSet resultSet) throws SQLException, IOException {
        Voucher voucher = new Voucher(
                resultSet.getInt("id"),
                resultSet.getString("code"),
                resultSet.getInt("quantity")
        );

        ByteArrayOutputStream outputStream =  new ByteArrayOutputStream();
        json.serialize(voucher, outputStream);
        return outputStream.toByteArray();
    }

    private Buffer errorToBuffer(int code, String error) {
        try {
            ByteArrayOutputStream outputStream =  new ByteArrayOutputStream();
            json.serialize(new Error(code, error), outputStream);
            return Buffer.buffer(outputStream.toByteArray());
        } catch (IOException e) {
            return Buffer.buffer(e.toString());
        }
    }
}
