package io.strimzi.http.server;

import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.execution.instrumentation.tracing.TracingInstrumentation;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.reactivex.BackpressureStrategy;
import io.reactivex.subjects.PublishSubject;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.VertxDataFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Triplet;
import org.reactivestreams.Publisher;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;
import static java.util.stream.Collectors.toList;

public class AdminServer extends AbstractVerticle {
    private static final Logger LOGGER = LogManager.getLogger(AdminServer.class);

    List<Book> newList = Arrays.asList(
        new Book("Harry Potter and the Sorcerer's stone", "J.K. Rowling", "123456"),
        new Book("Jurassic Park", "Michael Crichton", "123457"),
        new Book("Book 3", "Michael Crichton", "123458"),
        new Book("Book 4", "Michael Crichton", "123459"),
        new Book("Book 5", "Michael Crichton", "123460"),
        new Book("Book 6", "Michael Crichton", "123461"),
        new Book("Book 7", "Michael Crichton", "123462")
    );

    PublishSubject<Book> bookAdded = PublishSubject.create();
    PublishSubject<Book> bookRemoved = PublishSubject.create();

    List<Book> books = new ArrayList<>(newList);

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
                                    .getResourceAsStream("graphql-schema/book.graphql")),
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
        VertxDataFetcher<List<Book>> booksDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            books(environment, future);
        });

        VertxDataFetcher<String> tokenDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            getToken(environment, future);
        });

        VertxDataFetcher<BookFeed> bookFeedDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            bookFeed(environment, future);
        });

        VertxDataFetcher<Book> addBookDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            addBook(environment, future);
        });

        VertxDataFetcher<Book> deleteBookDataFetcher = new VertxDataFetcher<>((environment, future) -> {
            deleteBook(environment, future);
        });

        DataFetcher<Publisher<Book>> bookAddedPublisherDataFetcher = environment -> {
            return bookAdded.toFlowable(BackpressureStrategy.BUFFER);
        };

        DataFetcher<Publisher<Book>> bookRemovedPublisherDataFetcher = environment -> {
            return bookRemoved.toFlowable(BackpressureStrategy.BUFFER);
        };

        RuntimeWiring runtimeWiring = newRuntimeWiring()
            .type("Query", builder -> builder.dataFetcher("books", booksDataFetcher)
                .dataFetcher("bookFeed", bookFeedDataFetcher)
                .dataFetcher("getToken", tokenDataFetcher))
            .type("Mutation", builder -> builder.dataFetcher("addBook", addBookDataFetcher)
                .dataFetcher("deleteBook", deleteBookDataFetcher))
            .type("Subscription", builder -> builder.dataFetcher("bookAdded", bookAddedPublisherDataFetcher))
            .type("Subscription", builder -> builder.dataFetcher("bookRemoved", bookRemovedPublisherDataFetcher))
            .build();

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(baseSchema, runtimeWiring);

        GraphQL build = GraphQL.newGraphQL(graphQLSchema)
            .instrumentation(new TracingInstrumentation())
            .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
            .build();

        return build;
    }

    private void addBook(DataFetchingEnvironment env, Handler<AsyncResult<Book>> completionHandler) {
        String title = env.getArgument("title");
        String author = env.getArgument("author");
        Book book = new Book(title, author, "467234");
        books.add(book);
        bookAdded.onNext(book);

        completionHandler.handle(Future.succeededFuture(book));
    }

    private void deleteBook(DataFetchingEnvironment env, Handler<AsyncResult<Book>> completionHandler) {
        String createdAt = env.getArgument("createdAt");
        Book bookToDelete = books.stream().filter(book -> book.createdAt.equals(createdAt)).findFirst().get();
        books = books.stream().filter(book -> !book.createdAt.equals(createdAt)).collect(toList());

        bookRemoved.onNext(bookToDelete);
        completionHandler.handle(Future.succeededFuture(bookToDelete));
    }

    private void books(DataFetchingEnvironment env, Handler<AsyncResult<List<Book>>> completionHandler) {
        completionHandler.handle(Future.succeededFuture(books));
    }

    private void getToken(DataFetchingEnvironment env, Handler<AsyncResult<String>> completionHandler) {
        completionHandler.handle(Future.succeededFuture(env.getContext()));
    }


    private void bookFeed(DataFetchingEnvironment env, Handler<AsyncResult<BookFeed>> completionHandler) {
        String before = env.getArgument("before");
        String after = env.getArgument("after");
        int limit = env.getArgument("limit");
        BookFeed bookFeed = new BookFeed();
        List<Book> newBooks = new ArrayList<>();
        String cursor;
        if (before == null && after == null) {
            cursor = books.get(0).createdAt;
            System.out.println("cursor " + cursor);
        } else {
            cursor = before != null ? before : after;
            System.out.println("cursor " + cursor);
        }

//        int cursorInt = Integer.parseInt(cursor);

        int bookIndex = books.indexOf(books.stream().filter(b -> b.createdAt.equals(cursor)).findFirst().get());

        System.out.println("bookIndex " + bookIndex);

        if (before != null) {
            int startIndex = bookIndex - limit > 0 ? bookIndex - limit : 0;

            System.out.println("startIndex " + startIndex);
            PageInfo pageInfo = new PageInfo();
            pageInfo.hasNextPage = startIndex != books.size() - 1;
            pageInfo.hasPrevPage = startIndex != 0;
            pageInfo.startCursor = books.get(startIndex).createdAt;
            pageInfo.endCursor = books.get(bookIndex).createdAt;

            newBooks = books.subList(startIndex, bookIndex);
            bookFeed.books = newBooks;
            bookFeed.pageInfo = pageInfo;
        } else {
            boolean last = bookIndex + limit >= books.size() - 1;
            int endIndex = last ? books.size() - 1 : bookIndex + limit;
            System.out.println("endIndex " + endIndex);
            PageInfo pageInfo = new PageInfo();
            pageInfo.hasNextPage = endIndex != books.size() - 1;
            pageInfo.hasPrevPage = bookIndex != 0;
            pageInfo.startCursor = books.get(bookIndex).createdAt;
            pageInfo.endCursor = books.get(endIndex).createdAt;

            newBooks = books.subList(bookIndex, last && endIndex - bookIndex != limit ? endIndex + 1 : endIndex);
            bookFeed.books = newBooks;
            bookFeed.pageInfo = pageInfo;
        }

        completionHandler.handle(Future.succeededFuture(bookFeed));
    }
}
