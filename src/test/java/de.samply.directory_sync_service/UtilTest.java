package de.samply.directory_sync_service;

import static org.junit.jupiter.api.Assertions.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.hl7.fhir.r4.model.OperationOutcome;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

class UtilTest {

    // ---------- convertListOfStringMapsToTable ----------

    @Test
    void convertListOfStringMapsToTable_nullOrEmpty_returnsEmptyString() {
        assertEquals("", Util.convertListOfStringMapsToTable(null, ";", List.of("a")));
        assertEquals("", Util.convertListOfStringMapsToTable(List.of(), ";", List.of("a")));
    }

    @Test
    void convertListOfStringMapsToTable_respectsColumnOrder_thenAppendsAlphabetical() {
        List<Map<String, String>> data = List.of(
                new HashMap<>(Map.of("b","2","a","1","z","9")),
                new HashMap<>(Map.of("a","x","c","y"))
        );
        String table = Util.convertListOfStringMapsToTable(data, ";", List.of("z","a"));

        String[] lines = table.split("\\R");
        assertArrayEquals(new String[]{"z","a","b","c"}, lines[0].split(";"));

        // rows are sorted alphabetically as strings (post-join), so assert both rows exist
        assertEquals(3, lines.length);
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.equals("9;1;2;")));
        assertTrue(Arrays.stream(lines).anyMatch(l -> l.equals(";x;;y")));
    }

    @Test
    void convertListOfStringMapsToTable_skipsNullOrEmptyRowMaps() {
        List<Map<String, String>> data = new ArrayList<>();
        data.add(null);
        data.add(Map.of()); // empty
        data.add(Map.of("a", "1"));
        String table = Util.convertListOfStringMapsToTable(data, ",", List.of("a"));
        String[] lines = table.split("\\R");
        assertEquals("a", lines[0]);
        assertEquals("1", lines[1]);
        assertEquals(2, lines.length);
    }

    // ---------- convertListOfMapsToTable (indirectly tests formatValue) ----------

    @Test
    void convertListOfMapsToTable_formatsValues_string_int_map_listOfMaps_and_unknown() {
        Map<String,Object> row1 = new HashMap<>();
        row1.put("s", "str");
        row1.put("i", 42);
        row1.put("m1", Map.of("name", "Name1", "id", "ID1"));
        row1.put("m2", Map.of("id", "OnlyId"));
        row1.put("lm", List.of(Map.of("name", "N1"), Map.of("name", "N2")));
        row1.put("u", 3.14d); // unknown → toString()

        String table = Util.convertListOfMapsToTable(List.of(row1), "|", List.of("s","i","m1","m2","lm","u"));
        String[] lines = table.split("\\R");

        assertEquals("s|i|m1|m2|lm|u", lines[0]);
        assertEquals("str|42|Name1|OnlyId|N1,N2|3.14", lines[1]);
    }

    @Test
    void convertListOfMapsToTable_nullOrEmpty_returnsEmptyString() {
        assertEquals("", Util.convertListOfMapsToTable(null, ";", List.of("a")));
        assertEquals("", Util.convertListOfMapsToTable(List.of(), ";", List.of("a")));
    }

    @Test
    void convertListOfMapsToTable_rowsAreAlphabeticallySorted_afterJoin() {
        Map<String,Object> r1 = Map.of("a","alpha","b","beta");
        Map<String,Object> r2 = Map.of("a","alpha","b","aaa");
        String table = Util.convertListOfMapsToTable(List.of(r1, r2), ",", List.of("a","b"));
        String[] lines = table.split("\\R");
        assertEquals("a,b", lines[0]);
        // Expect "alpha,aaa" line to appear before "alpha,beta"
        assertEquals("alpha,aaa", lines[1]);
        assertEquals("alpha,beta", lines[2]);
    }

    // ---------- traceFromException ----------

    @Test
    void traceFromException_containsExceptionTypeAndMessage() {
        Exception ex = new IllegalArgumentException("bad arg");
        String trace = Util.traceFromException(ex);
        assertTrue(trace.contains("IllegalArgumentException"));
        assertTrue(trace.contains("bad arg"));
        assertTrue(trace.contains("at ")); // stack frame
    }

    // ---------- getErrorMessageFromOperationOutcome ----------

    @Test
    void getErrorMessageFromOperationOutcome_collectsOnlyErrorAndFatalWithNewlines() {
        OperationOutcome oo = new OperationOutcome();

        OperationOutcome.OperationOutcomeIssueComponent info =
                new OperationOutcome.OperationOutcomeIssueComponent();
        info.setSeverity(OperationOutcome.IssueSeverity.INFORMATION);
        info.setDiagnostics("ignore me");

        OperationOutcome.OperationOutcomeIssueComponent error =
                new OperationOutcome.OperationOutcomeIssueComponent();
        error.setSeverity(OperationOutcome.IssueSeverity.ERROR);
        error.setDiagnostics("first error");

        OperationOutcome.OperationOutcomeIssueComponent fatal =
                new OperationOutcome.OperationOutcomeIssueComponent();
        fatal.setSeverity(OperationOutcome.IssueSeverity.FATAL);
        fatal.setDiagnostics("fatal error");

        oo.setIssue(List.of(info, error, fatal));

        String msg = Util.getErrorMessageFromOperationOutcome(oo);
        // Ends with newline per implementation
        assertEquals("first error\nfatal error\n", msg);
    }

    // ---------- jsonStringFomObject (note: method name has a small typo) ----------

    @Test
    void jsonStringFomObject_serializesPrettyPrinted() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("x", 1);
        payload.put("y", "z");

        String json = Util.jsonStringFomObject(payload);

        com.google.gson.JsonObject obj =
                com.google.gson.JsonParser.parseString(json).getAsJsonObject();

        assertEquals(1, obj.get("x").getAsInt());
        assertEquals("z", obj.get("y").getAsString());
        assertTrue(json.contains("\n"));
    }

    @Test
    void jsonStringFomObject_null_returnsNull() {
        assertNull(Util.jsonStringFomObject(null));
    }

    // ---------- orderedKeylistFromMap ----------

    @Test
    void orderedKeylistFromMap_returnsAlphabeticallySortedCommaSeparatedKeys() {
        Map<String,String> m = new HashMap<>();
        m.put("b", "B");
        m.put("a", "A");
        m.put("c", "C");
        assertEquals("a,b,c", Util.orderedKeylistFromMap(m));
    }

    @Test
    void orderedKeylistFromMap_null_returnsEmptyString() {
        assertEquals("", Util.orderedKeylistFromMap(null));
    }

    // ---------- printAsTable (capture stdout) ----------

    @Test
    void printAsTable_printsSemicolonSeparatedAlignedTable() {
        List<Map<String,String>> data = List.of(
                Map.of("name","Alice","age","9"),
                Map.of("age","10","name","Bob")
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos, true, StandardCharsets.UTF_8));
            Util.printAsTable(data);
        } finally {
            System.setOut(original);
        }

        String out = baos.toString(StandardCharsets.UTF_8);
        String[] lines = out.split("\\R");
        assertTrue(lines.length >= 3, "Expected header + 2 rows");

        // Parse header → column index map (trim to ignore padding)
        String[] headerCols = Arrays.stream(lines[0].split(";"))
                .map(String::trim)
                .toArray(String[]::new);

        Map<String,Integer> idx = new HashMap<>();
        for (int i = 0; i < headerCols.length; i++) {
            idx.put(headerCols[i], i);
        }
        assertTrue(idx.keySet().containsAll(List.of("name","age")),
                "Header must contain 'name' and 'age'");

        // Helper to get trimmed cell by column name
        java.util.function.BiFunction<String,String,String> cell =
                (line, col) -> {
                    String[] parts = Arrays.stream(line.split(";"))
                            .map(String::trim)
                            .toArray(String[]::new);
                    return idx.get(col) < parts.length ? parts[idx.get(col)] : "";
                };

        // Collect parsed rows (skip header)
        List<Map<String,String>> rows = Arrays.stream(lines)
                .skip(1)
                .map(l -> Map.of(
                        "name", cell.apply(l, "name"),
                        "age",  cell.apply(l, "age")
                ))
                .collect(Collectors.toList());

        // Assert rows regardless of order
        assertTrue(rows.stream().anyMatch(r -> r.get("name").equals("Alice") && r.get("age").equals("9")));
        assertTrue(rows.stream().anyMatch(r -> r.get("name").equals("Bob")   && r.get("age").equals("10")));
    }

    @Test
    void printAsTable_empty_printsNoDataMessage() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(baos, true, StandardCharsets.UTF_8));
            Util.printAsTable(List.of());
        } finally {
            System.setOut(original);
        }
        assertTrue(baos.toString(StandardCharsets.UTF_8).contains("No data available."));
    }

    // ---------- serializeToFile ----------

    @TempDir
    File tmpDir;

    @Test
    void serializeToFile_writesPrettyJsonToFile() throws Exception {
        File f = new File(tmpDir, "out.json");
        Map<String,Object> payload = Map.of("a", 1, "b", "c");
        Util.serializeToFile(payload, f.getAbsolutePath());

        assertTrue(f.exists());
        String content = Files.readString(f.toPath());
        assertTrue(content.contains("\"a\": 1"));
        assertTrue(content.contains("\"b\": \"c\""));
    }

    @Test
    void serializeToFile_nullObjectOrPath_isNoop() {
        // Should not throw; file should not be created
        Util.serializeToFile(null, new File(tmpDir, "x.json").getAbsolutePath());
        Util.serializeToFile(Map.of("x", 1), null);
        Util.serializeToFile(Map.of("x", 1), "");
        // nothing to assert besides "no exception"
    }
}
