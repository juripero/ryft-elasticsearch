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

import java.util.Optional;

public abstract class RyftExpressionRange extends RyftExpression {

    public static enum RyftOperatorCompare implements RyftDslToken {
        EQ("="), NE("!="), GT(">"), LT("<"), GTE(">="), LTE("<=");

        private final String ryftValue;

        private RyftOperatorCompare(String ryftValue) {
            this.ryftValue = ryftValue;
        }

        public static RyftOperatorCompare getOppositeValue(RyftOperatorCompare base) {
            switch (base) {
                case EQ:
                    return NE;
                case NE:
                    return EQ;
                case GT:
                    return LT;
                case LT:
                    return GT;
                case GTE:
                    return LTE;
                case LTE:
                    return GTE;
                default:
                    return base;
            }
        }

        @Override
        public String buildRyftString() {
            return ryftValue;
        }
    }

    protected String valueA;
    protected RyftOperatorCompare operatorA;
    protected Optional<String> valueB = Optional.empty();
    protected Optional<RyftOperatorCompare> operatorB = Optional.empty();
    protected String variableName;

    public RyftExpressionRange(String valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, String valueB, String expressionName, String variableName) {
        this(valueA, operatorA, expressionName, variableName);
        this.valueB = Optional.ofNullable(valueB);
        this.operatorB = Optional.ofNullable(operatorB);
        constructValue();
    }

    public RyftExpressionRange(String valueA, RyftOperatorCompare operatorA, String expressionName, String variableName) {
        this(expressionName);
        this.valueA = valueA;
        this.operatorA = operatorA;
        this.variableName = variableName;
        constructValue();
    }

    protected RyftExpressionRange(String expressionName) {
        super(expressionName);
    }

    protected final void constructValue() {
        if (valueB.isPresent() && operatorB.isPresent()) {
            value = String.format("%s %s %s %s %s", valueA, operatorA.buildRyftString(), variableName, operatorB.get().buildRyftString(), valueB.get());
        } else {
            value = String.format("%s %s %s", variableName, operatorA.buildRyftString(), valueA);
        }
    }
}
