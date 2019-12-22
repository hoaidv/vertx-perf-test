package vn.tiki;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.sqlclient.*;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.joining;

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
                .buffer(1, TimeUnit.MILLISECONDS)
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

        Map<Integer, RoutingContext> voucherIdToRequest = new HashMap<>();
        List<RoutingContext> invalidRequests = new ArrayList<>();

        for (RoutingContext request: requests) {
            String rawVoucherId = request.pathParam("id");
            try {
                int voucherId = Integer.parseInt(rawVoucherId);
                voucherIdToRequest.put(voucherId, request);
            } catch (NumberFormatException ignored) {
                invalidRequests.add(request);
            }
        }

        for (RoutingContext request: invalidRequests) {
            request.response().end(errorTextToBuffer(0x0001, "Invalid voucher id."));
        }

        findVouchers(voucherIdToRequest.keySet()).handleAsync((voucherMap, throwable) -> {
            if (throwable != null) {
                var errorText = printStackTrace(throwable);
                for (RoutingContext request: voucherIdToRequest.values()) {
                    request.response().end(errorTextToBuffer(0x0003, errorText));
                }
            } else {
                for (Map.Entry<Integer, RoutingContext> requestEntry : voucherIdToRequest.entrySet()) {
                    int voucherId = requestEntry.getKey();
                    RoutingContext request = requestEntry.getValue();
                    Voucher voucher = voucherMap.get(voucherId);
                    if (voucher != null) {
                        request.response().end(pojoToBuffer(voucher));
                    } else {
                        request.response().end(errorTextToBuffer(0x0002, "Voucher not found"));
                    }
                }
            }
            return null;
        });
    }

    private CompletableFuture<Map<Integer, Voucher>> findVouchers(Collection<Integer> voucherIds) {
        var futureVoucherMap = new CompletableFuture<Map<Integer, Voucher>>();

        connectionPool.getConnection(connectionResult -> {
            if (connectionResult.succeeded()) {
                SqlConnection connection = connectionResult.result();
                String idsList = voucherIds.stream().map(Objects::toString).collect(joining(", "));
                connection.preparedQuery("SELECT * FROM voucher WHERE id IN (?)", Tuple.of(idsList), queryResult -> {
                    if (queryResult.succeeded()) {
                        RowIterator<Row> iterator = queryResult.result().iterator();
                        Map<Integer, Voucher> voucherMap = new HashMap<>();

                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            Voucher voucher = new Voucher(
                                    row.getInteger("id"),
                                    row.getBuffer("code").toString(),
                                    row.getInteger("quantity")
                            );
                            voucherMap.put(voucher.id, voucher);
                        }

                        futureVoucherMap.complete(voucherMap);
                    } else {
                        futureVoucherMap.completeExceptionally(queryResult.cause());
                    }
                    connection.close();
                });
            } else {
                futureVoucherMap.completeExceptionally(connectionResult.cause());
            }
        });

        return futureVoucherMap;
    }

    private Buffer pojoToBuffer(Object pojo) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            json.serialize(pojo, outputStream);
            return Buffer.buffer(outputStream.toByteArray());
        } catch (IOException e) {
            return throwableToBuffer(0x0004, e);
        }
    }

    private Buffer errorTextToBuffer(int code, String error) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            json.serialize(new Error(code, error), outputStream);
            return Buffer.buffer(outputStream.toByteArray());
        } catch (IOException e) {
            return throwableToBuffer(0x0004, e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private Buffer throwableToBuffer(int code, Throwable throwable) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Error error = new Error(code, printStackTrace(throwable));
            json.serialize(error, outputStream);
            return Buffer.buffer(outputStream.toByteArray());
        } catch (IOException e) {
            return Buffer.buffer(e.toString());
        }
    }

    private static String printStackTrace(Throwable throwable) {
        StringWriter errorWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(errorWriter));
        return errorWriter.toString();
    }
}
