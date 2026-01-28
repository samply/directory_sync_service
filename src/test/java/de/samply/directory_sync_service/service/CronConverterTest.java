package de.samply.directory_sync_service.service;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CronConverterTest {

    @Nested
    class HappyPath {

        @Test
        void allWildcards_everyMinute() {
            // UNIX:  * * * * *
            // Quartz: 0 * * ? * *
            assertEquals("0 * * ? * *", CronConverter.unixToQuartz("* * * * *"));
        }

        @Test
        void dailyAtSpecificTime() {
            // UNIX:  15 2 * * *  →  02:15 every day
            assertEquals("0 15 2 ? * *", CronConverter.unixToQuartz("15 2 * * *"));
        }

        @Test
        void monthlyOnGivenDayOfMonth() {
            // UNIX:  0 0 1 * *  →  midnight on the 1st every month
            assertEquals("0 0 0 1 * *", CronConverter.unixToQuartz("0 0 1 * *"));
        }

        @Test
        void weekdaysOnly_byDow() {
            // UNIX:  0 0 * * 1-5  →  every day-of-week 1..5 at 00:00
            assertEquals("0 0 0 ? * 1-5", CronConverter.unixToQuartz("0 0 * * 1-5"));
        }

        @Test
        void preservesListAndWhitespace() {
            // extra spaces are fine; DOW comma list preserved
            assertEquals("0 5 4 ? * 1,2", CronConverter.unixToQuartz("  5  4   *   *   1,2  "));
        }
    }

    @Nested
    class DowMapping {

        @Test
        void zeroOrSevenMapToOne_singleValue() {
            // 0 → 1 (Sunday)
            assertEquals("0 0 12 ? * 1", CronConverter.unixToQuartz("0 12 * * 0"));
            // 7 → 1 (Sunday)
            assertEquals("0 0 12 ? * 1", CronConverter.unixToQuartz("0 12 * * 7"));
        }

        @Test
        void rangeMapping_rewritesEndpoints() {
            // 0-2 -> 1-2 ; 6-0 -> 6-1
            assertEquals("0 0 0 ? * 1-2", CronConverter.unixToQuartz("0 0 * * 0-2"));
            assertEquals("0 0 0 ? * 6-1", CronConverter.unixToQuartz("0 0 * * 6-0"));
        }

        @Test
        void listMapping_rewritesEachElement() {
            // 0,7,1 -> 1,1,1 (duplicates kept as-is)
            assertEquals("0 0 0 ? * 1,1,1", CronConverter.unixToQuartz("0 0 * * 0,7,1"));
        }
    }

    @Nested
    class ConflictRule {

        @Test
        void bothDomAndDowRestricted_prefersDowAndWildcardsDom() {
            // UNIX: minute=5 hour=4 DOM=13 month=10 DOW=3
            // Rule: when both DOM and DOW restricted, Quartz can't do OR -> use DOW, set DOM='?'
            String quartz = CronConverter.unixToQuartz("5 4 13 10 3");
            assertEquals("0 5 4 ? 10 3", quartz);
        }
    }

    @Nested
    class Validation {

        @Test
        void throwsWhenFieldCountIsNotFive() {
            assertThrows(IllegalArgumentException.class, () -> CronConverter.unixToQuartz("*/5 * * *"));      // 4 fields
            assertThrows(IllegalArgumentException.class, () -> CronConverter.unixToQuartz("* * * * * *"));    // 6 fields
        }
    }
}
