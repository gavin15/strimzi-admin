package io.strimzi.http.server;

import java.time.LocalDateTime;

public class Book {
    public String title;
    public String author;
    public String createdAt;

    public Book(String title, String author, String createdAt) {
        this.title = title;
        this.author = author;
        this.createdAt = createdAt;
    }
}
