package vn.tiki;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.http.HttpServerResponse;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.sqlclient.*;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Log4j2
public class VoucherVerticle extends AbstractVerticle {

    private final DslJson<Object> json = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());
    private MySQLPool connectionPool;
    private Disposable disposable;

    public VoucherVerticle(MySQLPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);

        Flowable<RoutingContext> flowableRoutingContexts = Flowable.create(flowableEmitter -> {
            router.route("/vouchers/:id")
                    .handler(flowableEmitter::onNext)
                    .failureHandler(rtx -> log.error(rtx.failure()));
        }, BackpressureStrategy.BUFFER);

        disposable = flowableRoutingContexts
                .buffer(2, TimeUnit.MILLISECONDS)
                .subscribe(this::handleBatchedRequests);

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
    public void stop() {
        if (!disposable.isDisposed()) {
            disposable.dispose();
        }
    }

    private void handleBatchedRequests(List<RoutingContext> requests) {
        if (requests.isEmpty()) return;

        for (RoutingContext request : requests) {
            HttpServerResponse response = request.response();
            String rawVoucherId = request.pathParam("id");

            if (rawVoucherId == null || rawVoucherId.isBlank()) {
                response.end(errorToBuffer(0x0001, "Missing voucher id."));
                continue;
            }

            int voucherId = Integer.parseInt(rawVoucherId);

            findVoucher(response, voucherId);
        }
    }

    private void findVoucher(HttpServerResponse response, int voucherId) {
        connectionPool.getConnection(connectionResult -> {
            if (connectionResult.succeeded()) {
                SqlConnection connection = connectionResult.result();
                connection.preparedQuery("SELECT * FROM voucher WHERE id = ?", Tuple.of(voucherId), queryResult -> {
                    if (queryResult.succeeded()) {
                        RowSet<Row> rows = queryResult.result();
                        RowIterator<Row> iterator = rows.iterator();
                        if (iterator.hasNext()) {
                            response.end(readFindVoucherResult(iterator.next()));
                        } else {
                            response.end(errorToBuffer(0x0002, "No voucher found."));
                        }
                    } else {
                        response.end(errorToBuffer(0x0003, queryResult.cause().toString()));
                    }
                    connection.close();
                });
            } else {
                response.end(errorToBuffer(0x0004, connectionResult.cause().toString()));
            }
        });
    }

    private Buffer readFindVoucherResult(Row row) {
        try {
            Voucher voucher = new Voucher(
                    row.getInteger("id"),
                    row.getBuffer("code").toString(),
                    row.getInteger("quantity")
            );
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            json.serialize(voucher, outputStream);
            return Buffer.buffer(outputStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize data", e);
        }
    }

    private Buffer errorToBuffer(int code, String error) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            json.serialize(new Error(code, error), outputStream);
            return Buffer.buffer(outputStream.toByteArray());
        } catch (IOException e) {
            return Buffer.buffer(e.toString());
        }
    }
}
