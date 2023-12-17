package com.blog.snowflakedateissue;

import org.jooq.impl.DSL;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

@RestController
public class Example {

    @GetMapping(path = "/getData")
    public LocalDate getData(@RequestParam List<String> dates, String tableName) throws SQLException {
        var log = LoggerFactory.getLogger(Example.class);
        log.debug("Fetching max date for table: {} & dates : {}", tableName, dates.toString());
        try (var con = dbConnection()) {
            var statement = con.prepareStatement("SELECT max(purchased_on) AS max_date FROM PURCHASES where purchased_on IN ('2023-01-01', '2022-12-31) limit 1");
            var result = statement.executeQuery();
            result.next();
            var maxDate = result.getDate("max_date");


            var jooqMaxDate = getDataFromJooqQuery(con, dates, TableFields.valueOf(tableName));
            log.debug("jooqmaxdate : {}", jooqMaxDate.toString());
            log.debug("queryMaxDate : {}", maxDate.toString());

            return LocalDate.ofInstant(maxDate.toInstant(), ZoneId.of("America/New_York"));
        } catch (Exception e) {
            return null;
        }
    }

    private Connection dbConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:snowflake://--accountname--.snowflakecomputing.com");
    }

    @GetMapping(path = "/getJooqData")
    public LocalDate getDataFromJooqQuery(Connection connection, List<String> dates, TableFields tableFields ) {
        var context = DSL.using(connection);
        var maxDateField = DSL.field(tableFields.fieldName).as("max_date");
        var registerdDateField = DSL.field(tableFields.fieldName);

        var select = context.select(maxDateField).from(tableFields.name()).where(registerdDateField.in(dates)).limit(1);
        var maxdate = context.fetchOne(select).into(LocalDate[].class);

        return maxdate[0];
    }

}
