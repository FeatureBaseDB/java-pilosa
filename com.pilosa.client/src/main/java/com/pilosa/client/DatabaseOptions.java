package com.pilosa.client;

public class DatabaseOptions {
        private String columnLabel = "profileID";

        private DatabaseOptions() {}

        public static DatabaseOptions withDefaults() {
            return new DatabaseOptions();
        }

        public static DatabaseOptions withColumnLabel(String columnLabel) {
            Validator.ensureValidLabel(columnLabel);
            DatabaseOptions options = new DatabaseOptions();
            options.columnLabel = columnLabel;
            return options;
        }

        public String getColumnLabel() {
            return this.columnLabel;
    }
}
