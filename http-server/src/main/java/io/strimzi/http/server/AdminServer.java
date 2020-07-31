package io.strimzi.http.server;

import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.execution.instrumentation.tracing.TracingInstrumentation;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.VertxDataFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Triplet;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

public class AdminServer extends AbstractVerticle {
    private static final Logger LOGGER = LogManager.getLogger(AdminServer.class);
    @Override
    public void start(final Promise<Void> startServer) {

        loadRoutes()
            .onSuccess(routes -> {
                HttpServerOptions httpServerOptions = new HttpServerOptions()
                    .setWebSocketSubProtocols(Collections.singletonList("graphql-ws"));

                final HttpServer server = vertx.createHttpServer(httpServerOptions);


                final Router router = Router.router(vertx);
                router.mountSubRouter("/", routes);   // Mount the sub router containing the module routes
                Map<String, String> map = new HashMap<>();
                vertx.executeBlocking(promise -> {
                        final SchemaParser schemaParser = new SchemaParser();
                        final InputStreamReader userInputStream = new InputStreamReader(
                            Objects.requireNonNull(
                                getClass()
                                    .getClassLoader()
                                    .getResourceAsStream("graphql-schema/test.graphql")),
                            StandardCharsets.UTF_8);

                        final TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(userInputStream);

                        promise.complete(typeDefinitionRegistry);

                    }, ar -> {
                        final TypeDefinitionRegistry baseSchema = (TypeDefinitionRegistry) ar.result();
                        final GraphQL graphQL = setupGraphQL(baseSchema);
                        router.route("/graphql").handler(routingContext -> {
                            if (routingContext.request().getHeader("Authorization") != null) {
                                map.put("token", routingContext.request().getHeader("Authorization"));
                                routingContext.next();
                            } else {
                                routingContext.response().setStatusCode(511).end();
                            }
                        });
                        router.route("/graphql").handler(ApolloWSHandler.create(graphQL).connectionHandler(webSocket -> {
                            if (webSocket.headers().get("Authorization") != null) {
                                map.put(webSocket.textHandlerID(), map.get("token"));
                            }
                        }).queryContext(context -> map.get(context.serverWebSocket().textHandlerID())));
                    }
                );
                server.requestHandler(router).listen(8080);
                LOGGER.info("AdminServer is listening on port 8080");
            })
            .onFailure(throwable -> LOGGER.atFatal().withThrowable(throwable).log("Loading of routes was unsuccessful."));
    }

    private Future<Router> loadRoutes() {

        final Router router = Router.router(vertx);
        final ServiceLoader<RestService> loader = ServiceLoader.load(RestService.class);
        final List<Future<Triplet<String, String, Router>>> modules = new ArrayList<>();

        loader.forEach(restService -> modules.add(restService.registerRoutes(vertx)));

        return CompositeFuture.all(new ArrayList<>(modules))
            .onSuccess(cf -> modules.forEach(future -> {
                final String moduleName = future.result().getValue0();
                final String mountPoint = future.result().getValue1();
                final Router subRouter = future.result().getValue2();

                router.mountSubRouter(mountPoint, subRouter);

                LOGGER.info("Module {} mounted on path {}.", moduleName, mountPoint);

            })).map(router);
    }

    private GraphQL setupGraphQL(TypeDefinitionRegistry baseSchema) {
        VertxDataFetcher<String> tokenDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            getToken(environment, future);
        });

        RuntimeWiring runtimeWiring = newRuntimeWiring()
            .type("Query", builder -> builder.dataFetcher("getToken", tokenDataFetcher))
            .build();

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(baseSchema, runtimeWiring);

        GraphQL build = GraphQL.newGraphQL(graphQLSchema)
            .instrumentation(new TracingInstrumentation())
            .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
            .build();

        return build;
    }

    private void getToken(DataFetchingEnvironment env, Handler<AsyncResult<String>> completionHandler) {
        LOGGER.info("token " + env.getContext());
        completionHandler.handle(Future.succeededFuture(env.getContext()));
    }
}
