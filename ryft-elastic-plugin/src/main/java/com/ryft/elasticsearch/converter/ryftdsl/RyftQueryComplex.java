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

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class RyftQueryComplex implements RyftQuery {

    public static enum RyftLogicalOperator implements RyftDslToken {
        AND, OR, XOR;

        @Override
        public String buildRyftString() {
            return name();
        }
    }

    private final Collection<RyftQuery> operands;
    private final RyftLogicalOperator operator;

    public RyftQueryComplex(RyftQuery operand1, RyftLogicalOperator operator, RyftQuery operand2) {
        this(operator, operand1, operand2);
    }

    public RyftQueryComplex(RyftLogicalOperator operator, RyftQuery... operands) {
        this(operator, Arrays.asList(operands));
    }

    public RyftQueryComplex(RyftLogicalOperator operator, Collection<RyftQuery> operands) {
        this.operands = operands;
        this.operator = operator;
    }

    @Override
    public String buildRyftString() {
        if (operands.size() == 1) {
            return operands.iterator().next().buildRyftString();
        } else {
            String queryString = operands.stream()
                    .map(operand -> operand.buildRyftString())
                    .collect(Collectors.joining(" " + operator.buildRyftString() + " "));
            return String.format("(%s)", queryString);
        }
    }

    @Override
    public RyftQuery toRawTextQuery() {
        //For raw_text search, there is an extra constraint: if the operator is AND, LINE must be true. Check and apply
        //transformations if required.
        if (operator.equals(RyftLogicalOperator.AND)) {
            return new RyftQueryComplex(operator, operands.stream().map(ryftQuery -> ryftQuery.toRawTextQuery().toLineQuery()).collect(Collectors.toList()));
        } else {
            return new RyftQueryComplex(operator, operands.stream().map(RyftQuery::toRawTextQuery).collect(Collectors.toList()));
        }
    }

    @Override
    public RyftQuery toLineQuery() {
        if (operator.equals(RyftLogicalOperator.AND)) {
            return new RyftQueryComplex(operator, operands.stream().map(RyftQuery::toLineQuery).collect(Collectors.toList()));
        }
        return this;
    }

    @Override
    public RyftQuery toWidthQuery(Integer width) {
        return new RyftQueryComplex(operator, operands.stream().map(ryftQuery -> ryftQuery.toWidthQuery(width)).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return "RyftQueryComplex{" + buildRyftString() + '}';
    }

}
