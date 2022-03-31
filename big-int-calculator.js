const DEFAULT_DECIMAL_PRECISION = 4;

class BigIntCalculator {
  constructor(options) {
    options = options || {};
    this.decimalPrecision = options.decimalPrecision == null ?
      DEFAULT_DECIMAL_PRECISION : options.decimalPrecision;
    this.decimalPrecisionFactor = 10 ** this.decimalPrecision;
  }

  multiplyBigIntByDecimal(bigIntValue, decimalValue) {
    return bigIntValue * BigInt(Math.round(decimalValue * this.decimalPrecisionFactor)) / BigInt(this.decimalPrecisionFactor);
  }

  divideBigIntByDecimal(bigIntValue, decimalValue) {
    if (decimalValue === 0) {
      return NaN;
    }
    return bigIntValue * BigInt(this.decimalPrecisionFactor) / BigInt(Math.round(decimalValue * this.decimalPrecisionFactor));
  }

  divideBigIntByBigInt(bigIntValueA, bigIntValueB) {
    if (bigIntValueB === 0n) {
      return NaN;
    }
    return Number(bigIntValueA * BigInt(this.decimalPrecisionFactor) / bigIntValueB) / this.decimalPrecisionFactor;
  }
}

module.exports = BigIntCalculator;
