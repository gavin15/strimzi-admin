package io.strimzi.http.server;

import graphql.GraphQL;
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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.VertxDataFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Triplet;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;
import static java.util.stream.Collectors.toList;

public class AdminServer extends AbstractVerticle {
    private static final Logger LOGGER = LogManager.getLogger(AdminServer.class);

    List<Book> books = Arrays.asList(
        new Book("Harry Potter and the Sorcerer's stone","J.K. Rowling", "123456"),
        new Book("Jurassic Park","Michael Crichton", "123457"),
        new Book("Book 3","Michael Crichton", "123458"),
        new Book("Book 4","Michael Crichton", "123459"),
        new Book("Book 5","Michael Crichton", "123460"),
        new Book("Book 6","Michael Crichton", "123461"),
        new Book("Book 7","Michael Crichton", "123462")
        );

    @Override
    public void start(final Promise<Void> startServer) {

        loadRoutes()
            .onSuccess(routes -> {
                HttpServerOptions httpServerOptions = new HttpServerOptions()
                    .addWebSocketSubProtocol("graphql-ws");

                final HttpServer server = vertx.createHttpServer(httpServerOptions);


                final Router router = Router.router(vertx);
                router.mountSubRouter("/", routes);   // Mount the sub router containing the module routes

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
//                    router.route("/graphql").handler(ApolloWSHandler.create(graphQL));
                    router.route("/graphql").handler(GraphQLHandler.create(graphQL));
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
        VertxDataFetcher<List<Book>> booksDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            books(environment, future);
        });

        VertxDataFetcher<Book> addBookDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            addBook(environment, future);
        });

        VertxDataFetcher<Book> deleteBookDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            deleteBook(environment, future);
        });

        RuntimeWiring runtimeWiring = newRuntimeWiring()
            .type("Query", builder -> builder.dataFetcher("books", booksDataFetcher))
            .type("Mutation", builder -> builder.dataFetcher("addBook", addBookDataFetcher)
            .dataFetcher("deleteBook", deleteBookDataFetcher))
            .build();

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(baseSchema, runtimeWiring);

        GraphQL build = GraphQL.newGraphQL(graphQLSchema)
            .instrumentation(new TracingInstrumentation()).build();

        return build;
    }

    private void addBook(DataFetchingEnvironment env, Handler<AsyncResult<Book>> completionHandler) {
        String title = env.getArgument("title");
        String author = env.getArgument("author");
        Book book = new Book(title, author, "467234");
        List<Book> newList = new ArrayList<>(books);
        newList.add(book);
        books = newList;

        completionHandler.handle(Future.succeededFuture(book));
    }

    private void deleteBook(DataFetchingEnvironment env, Handler<AsyncResult<Book>> completionHandler) {
        String createdAt = env.getArgument("createdAt");
        Book bookToDelete = books.stream().filter(book -> book.createdAt.equals(createdAt)).findFirst().get();
        books = books.stream().filter(book -> !book.createdAt.equals(createdAt)).collect(toList());

         completionHandler.handle(Future.succeededFuture(bookToDelete));
    }

    private void books(DataFetchingEnvironment env, Handler<AsyncResult<List<Book>>> completionHandler) {
        completionHandler.handle(Future.succeededFuture(books));
    }

    private void bookFeed(DataFetchingEnvironment env, Handler<AsyncResult<List<Book>>> completionHandler) {
        LocalDateTime before = env.getArgument("before");
        LocalDateTime after = env.getArgument("after");
        String limit = env.getArgument("limit");
        List<Book> books = new ArrayList<>();
        if (before != null && after != null) {
            for (Book book : books
            ) {
                LocalDateTime date = LocalDateTime.parse(book.createdAt);
                if (date.isBefore(before) && date.isAfter(after)) {
                    books.add(book);
                }
            }
            completionHandler.handle(Future.succeededFuture(books));
        }
        else {
            completionHandler.handle(Future.succeededFuture(books));
        }
    }
}
