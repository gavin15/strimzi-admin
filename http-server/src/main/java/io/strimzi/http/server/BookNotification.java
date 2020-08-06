package io.strimzi.http.server;

import java.time.LocalDateTime;

public class BookNotification extends Book {
    public String subscribedBy;

    public BookNotification(String title, String author, String createdAt, String createdBy, String subscribedBy) {
        super(title, author, createdAt, createdBy);
        this.subscribedBy = subscribedBy;
    }
}
