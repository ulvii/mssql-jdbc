/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;

//Constants relating to the historically accepted Julian-Gregorian calendar cutover date (October 15, 1582).
//
//Used in processing SQL Server temporal data types whose date component may precede that date.
//
//Scoping these constants to a class defers their initialization to first use.
class GregorianChange {
 // Cutover date for a pure Gregorian calendar - that is, a proleptic Gregorian calendar with
 // Gregorian leap year behavior throughout its entire range. This is the cutover date is used
 // with temporal server values, which are represented in terms of number of days relative to a
 // base date.
 static final java.util.Date PURE_CHANGE_DATE = new java.util.Date(Long.MIN_VALUE);

 // The standard Julian to Gregorian cutover date (October 15, 1582) that the JDBC temporal
 // classes (Time, Date, Timestamp) assume when converting to and from their UTC milliseconds
 // representations.
 static final java.util.Date STANDARD_CHANGE_DATE = (new GregorianCalendar(Locale.US)).getGregorianChange();

 // A hint as to the number of days since 1/1/0001, past which we do not need to
 // not rationalize the difference between SQL Server behavior (pure Gregorian)
 // and Java behavior (standard Gregorian).
 //
 // Not having to rationalize the difference has a substantial (measured) performance benefit
 // for temporal getters.
 //
 // The hint does not need to be exact, as long as it's later than the actual change date.
 static final int DAYS_SINCE_BASE_DATE_HINT = DDC.daysSinceBaseDate(1583, 1, 1);

 // Extra days that need to added to a pure gregorian date, post the gergorian
 // cut over date, to match the default julian-gregorain calendar date of java.
 static final int EXTRA_DAYS_TO_BE_ADDED;

 static {
     // This issue refers to the following bugs in java(same issue).
     // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7109480
     // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6459836
     // The issue is fixed in JRE 1.7
     // and exists in all the older versions.
     // Due to the above bug, in older JVM versions(1.6 and before),
     // the date calculation is incorrect at the Gregorian cut over date.
     // i.e. the next date after Oct 4th 1582 is Oct 17th 1582, where as
     // it should have been Oct 15th 1582.
     // We intentionally do not make a check based on JRE version.
     // If we do so, our code would break if the bug is fixed in a later update
     // to an older JRE. So, we check for the existence of the bug instead.

     GregorianCalendar cal = new GregorianCalendar(Locale.US);
     cal.clear();
     cal.set(1, Calendar.FEBRUARY, 577738, 0, 0, 0);// 577738 = 1+577737(no of days since epoch that brings us to oct 15th 1582)
     if (cal.get(Calendar.DAY_OF_MONTH) == 15) {
         // If the date calculation is correct(the above bug is fixed),
         // post the default gregorian cut over date, the pure gregorian date
         // falls short by two days for all dates compared to julian-gregorian date.
         // so, we add two extra days for functional correctness.
         // Note: other ways, in which this issue can be fixed instead of
         // trying to detect the JVM bug is
         // a) use unoptimized code path in the function convertTemporalToObject
         // b) use cal.add api instead of cal.set api in the current optimized code path
         // In both the above approaches, the code is about 6-8 times slower,
         // resulting in an overall perf regression of about (10-30)% for perf test cases
         EXTRA_DAYS_TO_BE_ADDED = 2;
     }
     else
         EXTRA_DAYS_TO_BE_ADDED = 0;
 }

 private GregorianChange() {
 }
}

