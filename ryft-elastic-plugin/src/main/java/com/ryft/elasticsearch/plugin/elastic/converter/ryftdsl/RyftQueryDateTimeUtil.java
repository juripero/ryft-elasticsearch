package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionRange.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
                                                String fieldName) throws ElasticConversionException, ParseException {
        Optional<RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            Date lowerDate = sdf.parse(lowerBound.get(operatorCompareLower.get()));
            Date upperDate = sdf.parse(upperBound.get(operatorCompareUpper.get()));

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

            //Special case - if the first is the same as the last, we do not need an extra expression to retrieve the
            //days that fall between them
            SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
            if (!fmt.format(lowerDate).equals(fmt.format(upperDate))) {
                finalQueries.add(middleDayQuery);
            }
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
