package vn.tiki;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.runtime.Settings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@Log4j2
public class VoucherVerticle extends AbstractVerticle {

    private final DslJson<Object> json = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());
    private Connection connection;
    private Disposable disposable;

    public VoucherVerticle(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);

        Flowable<RoutingContext> flowableRoutingContexts = Flowable.create(
                flowableEmitter -> router
                        .route("/vouchers/:id")
                        .handler(flowableEmitter::onNext)
                        .failureHandler(rtx -> log.error(rtx.failure())),
                BackpressureStrategy.BUFFER
        );

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

        for (RoutingContext request : requests) {
            String rawVoucherId = request.pathParam("id");
            try {
                int voucherId = Integer.parseInt(rawVoucherId);
                voucherIdToRequest.put(voucherId, request);
            } catch (NumberFormatException ignored) {
                invalidRequests.add(request);
            }
        }

        for (RoutingContext request : invalidRequests) {
            request.response().end(errorTextToBuffer(0x0001, "Invalid voucher id."));
        }

        findVouchers(voucherIdToRequest.keySet()).handleAsync((voucherMap, throwable) -> {
            if (throwable != null) {
                var errorText = printStackTrace(throwable);
                for (RoutingContext request : voucherIdToRequest.values()) {
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
        return CompletableFuture.supplyAsync(() -> {
            try (Statement statement = connection.createStatement()) {
                String idsList = voucherIds.stream().map(Objects::toString).collect(joining(", "));
                String findVouchersSql = format("SELECT * FROM voucher WHERE id IN (%s)", idsList);
                statement.execute(findVouchersSql);

                try (ResultSet resultSet = statement.getResultSet()) {
                    Map<Integer, Voucher> voucherMap = new HashMap<>();
                    while (resultSet.next()) {
                        Voucher voucher = new Voucher(
                                resultSet.getInt("id"),
                                resultSet.getString("code"),
                                resultSet.getInt("quantity")
                        );
                        voucherMap.put(voucher.id, voucher);
                    }
                    return voucherMap;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static ThreadLocal<ByteArrayOutputStream> outputStreams = new ThreadLocal<>();

    private Buffer pojoToBuffer(Object pojo) {
        try {
            ByteArrayOutputStream outputStream = outputStreams.get();

            if (outputStream == null) {
                outputStream = new ByteArrayOutputStream();
                outputStreams.set(outputStream);
            }

            outputStream.reset();

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
