/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.ryft.elasticsearch.converter.ryftdsl;

import com.ryft.elasticsearch.converter.ElasticConversionException;
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionRange.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Utility class which contains methods specific to datetime queries
 */
public class RyftQueryDateTimeUtil {

    public static RyftQuery buildSimpleDateQuery(Date date,
                                                 String format,
                                                 RyftOperator ryftOperator,
                                                 String fieldName,
                                                 RyftOperatorCompare operatorCompare) throws ElasticConversionException {
        RyftExpression ryftExpressionDate = new RyftExpressionDate(date, operatorCompare, format);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpressionDate);
    }

    public static RyftQuery buildSimpleTimeQuery(Date date,
                                                 String format,
                                                 RyftOperator ryftOperator,
                                                 String fieldName,
                                                 RyftOperatorCompare operatorCompare) throws ElasticConversionException {
        RyftExpression ryftExpressionDate = new RyftExpressionTime(date, operatorCompare, format);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpressionDate);
    }

    /**
     * Builds datetime range query for cases where only either date or time are specified.
     */
    public static RyftQuery buildSimpleRangeQuery(Map<RyftOperatorCompare, String> lowerBound,
                                                  Map<RyftOperatorCompare, String> upperBound,
                                                  String format,
                                                  RyftOperator ryftOperator,
                                                  String fieldName,
                                                  boolean isTime) throws ElasticConversionException, ParseException {
        Optional<RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            Date dateLower = sdf.parse(lowerBound.get(operatorCompareLower.get()));
            Date dateUpper = sdf.parse(upperBound.get(operatorCompareUpper.get()));

            //Resulting expression is of format "a < x < b". So, we must reverse operatorCompareLower in order for the comparison logic to be preserved
            RyftExpression ryftExpression;
            if (isTime) {
                ryftExpression = new RyftExpressionTime(dateLower,
                        RyftOperatorCompare.getOppositeValue(operatorCompareLower.get()),
                        operatorCompareUpper.get(), dateUpper, format);
            } else {
                ryftExpression = new RyftExpressionDate(dateLower,
                        RyftOperatorCompare.getOppositeValue(operatorCompareLower.get()),
                        operatorCompareUpper.get(), dateUpper, format);
            }

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareLower.isPresent() && !operatorCompareUpper.isPresent()) {
            Date dateA = sdf.parse(lowerBound.get(operatorCompareLower.get()));

            RyftExpression ryftExpression;
            if (isTime) {
                ryftExpression = new RyftExpressionTime(dateA, operatorCompareLower.get(), format);
            } else {
                ryftExpression = new RyftExpressionDate(dateA, operatorCompareLower.get(), format);
            }

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareUpper.isPresent() && !operatorCompareLower.isPresent()) {
            Date dateB = sdf.parse(upperBound.get(operatorCompareUpper.get()));

            RyftExpression ryftExpression;
            if (isTime) {
                ryftExpression = new RyftExpressionTime(dateB, operatorCompareUpper.get(), format);
            } else {
                ryftExpression = new RyftExpressionDate(dateB, operatorCompareUpper.get(), format);
            }

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else {
            throw new ElasticConversionException("Could not parse datetime format: " + format);
        }
    }

    /**
     * Builds datetime range query for cases where both date and time are specified.
     */
    public static RyftQuery buildFullRangeQuery(Map<RyftOperatorCompare, String> lowerBound,
                                                Map<RyftOperatorCompare, String> upperBound,
                                                String format,
                                                RyftOperator ryftOperator,
                                                String fieldName, boolean isMillis) throws ElasticConversionException, ParseException {
        Optional<RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            Date lowerDate;
            Date upperDate;
            if(isMillis) {
                lowerDate = new Date(Long.valueOf(lowerBound.get(operatorCompareLower.get())));
                upperDate = new Date(Long.valueOf(upperBound.get(operatorCompareUpper.get())));
            } else  {
                lowerDate = sdf.parse(lowerBound.get(operatorCompareLower.get()));
                upperDate = sdf.parse(upperBound.get(operatorCompareUpper.get()));
            }
            //Special case - if the first date is the same as the last, we can create a simplified expression
            SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
            if (fmt.format(lowerDate).equals(fmt.format(upperDate))) {
                RyftQuery dayQuery = buildSimpleDateQuery(lowerDate, format, RyftOperator.CONTAINS, fieldName, RyftOperatorCompare.EQ);
                RyftQuery timeQuery = buildSimpleRangeQuery(lowerBound, upperBound, format, RyftOperator.CONTAINS, fieldName, true);

                List<RyftQuery> allQueries = Arrays.asList(dayQuery, timeQuery);
                return new RyftQueryComplex(RyftQueryComplex.RyftLogicalOperator.AND, allQueries);
            }

            RyftQuery lowerDayQuery = buildBoundaryDayQuery(lowerDate, format,
                    ryftOperator, fieldName, operatorCompareLower.get());

            RyftExpression ryftExpressionMiddle = new RyftExpressionDate(lowerDate,
                    RyftOperatorCompare.LT, RyftOperatorCompare.LT, upperDate, format);
            RyftQuery middleDayQuery = new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                    ryftOperator, ryftExpressionMiddle);

            RyftQuery upperDayQuery = buildBoundaryDayQuery(upperDate, format, ryftOperator,
                    fieldName, operatorCompareUpper.get());

            List<RyftQuery> finalQueries = new ArrayList<>();
            finalQueries.add(lowerDayQuery);

            finalQueries.add(middleDayQuery);
            finalQueries.add(upperDayQuery);

            return new RyftQueryComplex(RyftQueryComplex.RyftLogicalOperator.OR, finalQueries);
        } else if (operatorCompareLower.isPresent() && !operatorCompareUpper.isPresent()) {
            Date dateLower = sdf.parse(lowerBound.get(operatorCompareLower.get()));
            RyftQuery lowerDayQuery = buildBoundaryDayQuery(dateLower, format, ryftOperator,
                    fieldName, operatorCompareLower.get());

            RyftExpression ryftExpressionRemainingDays = new RyftExpressionDate(dateLower, RyftOperatorCompare.GT, format);
            RyftQuery remainingDaysQuery = new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                    ryftOperator, ryftExpressionRemainingDays);

            List<RyftQuery> finalQueries = new ArrayList<>();
            finalQueries.add(lowerDayQuery);
            finalQueries.add(remainingDaysQuery);

            return new RyftQueryComplex(RyftQueryComplex.RyftLogicalOperator.OR, finalQueries);
        } else if (operatorCompareUpper.isPresent() && !operatorCompareLower.isPresent()) {
            Date dateUpper = sdf.parse(upperBound.get(operatorCompareUpper.get()));
            RyftQuery upperDayQuery = buildBoundaryDayQuery(dateUpper, format, ryftOperator,
                    fieldName, operatorCompareUpper.get());

            RyftExpression ryftExpressionRemainingDays = new RyftExpressionDate(dateUpper, RyftOperatorCompare.LT, format);
            RyftQuery remainingDaysQuery = new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                    ryftOperator, ryftExpressionRemainingDays);

            List<RyftQuery> finalQueries = new ArrayList<>();
            finalQueries.add(upperDayQuery);
            finalQueries.add(remainingDaysQuery);

            return new RyftQueryComplex(RyftQueryComplex.RyftLogicalOperator.OR, finalQueries);
        } else {
            throw new ElasticConversionException("Could not parse datetime format " + format);
        }
    }

    private static RyftQuery buildBoundaryDayQuery(Date date,
                                                   String format,
                                                   RyftOperator ryftOperator,
                                                   String fieldName,
                                                   RyftOperatorCompare operatorCompare) throws ElasticConversionException {
        RyftQuery dateQuery = buildSimpleDateQuery(date, format, ryftOperator,
                fieldName, RyftOperatorCompare.EQ);

        RyftQuery timeQuery = buildSimpleTimeQuery(date, format, ryftOperator,
                fieldName, operatorCompare);

        List<RyftQuery> queries = Arrays.asList(dateQuery, timeQuery);

        return new RyftQueryComplex(RyftQueryComplex.RyftLogicalOperator.AND, queries);
    }
}
