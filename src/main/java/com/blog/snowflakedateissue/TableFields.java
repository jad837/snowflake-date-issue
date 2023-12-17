package com.blog.snowflakedateissue;

public enum TableFields {

    PURCHASES("purchased_on"),
    REGISTRATIONS("registered_on");
    final String fieldName;

    TableFields(String fieldName) {
        this.fieldName = fieldName;
    }

}
