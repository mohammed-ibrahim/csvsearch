package org.tool.csearch.common;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import au.com.bytecode.opencsv.CSVReader;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class CsvColumnAnalyzer {

    private List<String> columnNames;

    private List<Integer> selectedColumnIndexes;

    private List<String> selectedColumnNames;

    public void compile(Path filePath, String columnExpression) {
        List<String> columnNamesInCsvFile = Arrays.asList(getColumnNamesOfCsv(filePath));
        this.columnNames =  columnNamesInCsvFile;

        //If expression is not present simply return all column indexes;
        if (columnExpression == null || columnExpression.trim().isEmpty()) {

            this.selectedColumnIndexes = columnNamesInCsvFile
                    .stream()
                    .map(i -> columnNamesInCsvFile.indexOf(i))
                    .collect(Collectors.toList());

            return;
        }

        List<String> columnExpressions = Arrays.asList(columnExpression.split(","))
                .stream()
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());

        List<Integer> integerExpressions = columnExpressions.stream()
                .filter(a -> safeParseInteger(a).isPresent())
                .map(b -> Integer.parseInt(b))
                .collect(Collectors.toList());

        //If all of the expressions are column indexes, then simply return the indexes;
        if (integerExpressions.size() == columnExpressions.size()) {

            this.selectedColumnIndexes = integerExpressions;
            return;
        }

        List<Integer> finalColumns = new ArrayList<Integer>();
        columnExpressions.forEach(expression -> {

            List<Integer> foundColumns = findColumns(expression, columnNamesInCsvFile);

            if (foundColumns != null && foundColumns.size() > 0) {
                finalColumns.addAll(foundColumns);
            }
        });

        if (finalColumns.size() < 1) {
            throw new RuntimeException("Couldn't find any columns in csv");
        }

        StringBuilder sb = new StringBuilder();
        finalColumns.forEach(a -> {
            sb.append(columnNamesInCsvFile.get(a));
            sb.append(",");
        });

        System.out.println("Selected columns: " + sb.toString());
        System.out.println("Final Columns: " + finalColumns.toString());

        this.selectedColumnIndexes = finalColumns;
        this.selectedColumnNames = new ArrayList<String>();

        this.selectedColumnIndexes.forEach(index -> {
            this.selectedColumnNames.add(this.columnNames.get(index));
        });
    }

    public List<Integer> findColumns(String expression, List<String> columnNamesInCsvFile) {
        if (expression.contains("*")) {

            System.out.println("Search for regexp: " + expression);
            List<Integer> matchingColumnsIndexes = columnNamesInCsvFile
                    .stream()
                    .filter(columnNameInCsv -> matches(expression, columnNameInCsv))
                    .map(i -> columnNamesInCsvFile.indexOf(i))
                    .collect(Collectors.toList());

            if (matchingColumnsIndexes.size() < 1) {

                log.info("Expression: {} didn't match any column.", expression);
                return null;
            }

            return matchingColumnsIndexes;
        } else {

            System.out.println("Search for full string: " + expression);
            if (columnNamesInCsvFile.contains(expression)) {
                return Arrays.asList(columnNamesInCsvFile.indexOf(expression));
            }

            log.info("Column: {} not found in csv.", expression);
            return null;
        }
    }

    public boolean matches(String pattern, String value) {
        String derivedPattern = pattern.replace("*", ".*");
        Pattern compiler = Pattern.compile(derivedPattern);
        Matcher matcher = compiler.matcher(value);

        return matcher.find();
    }

    public String[] getColumnNamesOfCsv(Path path) {

        String[] columnNamesInsideCsvFile = null;

        try (CSVReader reader = new CSVReader(new FileReader(path.toFile()))) {

            columnNamesInsideCsvFile = reader.readNext();
        } catch (Exception e) {

            throw new RuntimeException(e);
        }

        return columnNamesInsideCsvFile;
    }

    public static Optional<Integer> safeParseInteger(String input) {
        try {

            Integer parsedValue = Integer.parseInt(input);
            return Optional.of(parsedValue);
        } catch (Exception e) {

            return Optional.empty();
        }
    }
}

