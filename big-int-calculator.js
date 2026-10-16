const DEFAULT_DECIMAL_PRECISION = 4;

class BigIntCalculator {
  constructor(options) {
    options = options || {};
    this.decimalPrecision = options.decimalPrecision == null ?
      DEFAULT_DECIMAL_PRECISION : options.decimalPrecision;
    this.decimalPrecisionFactor = 10 ** this.decimalPrecision;
  }

  // A decimalValue which is not a finite number cannot be converted to a BigInt. Such a
  // value yields NaN, consistent with division by zero below, so that a bad decimal
  // surfaces as a failed comparison instead of as a RangeError thrown from deep inside
  // block processing.
  multiplyBigIntByDecimal(bigIntValue, decimalValue) {
    let decimalUnits = Math.round(decimalValue * this.decimalPrecisionFactor);
    if (!Number.isFinite(decimalUnits)) {
      return NaN;
    }
    return bigIntValue * BigInt(decimalUnits) / BigInt(this.decimalPrecisionFactor);
  }

  divideBigIntByDecimal(bigIntValue, decimalValue) {
    if (decimalValue === 0) {
      return NaN;
    }
    let decimalUnits = Math.round(decimalValue * this.decimalPrecisionFactor);
    // A decimal which rounds down to 0 units would divide by zero.
    if (!Number.isFinite(decimalUnits) || decimalUnits === 0) {
      return NaN;
    }
    return bigIntValue * BigInt(this.decimalPrecisionFactor) / BigInt(decimalUnits);
  }

  divideBigIntByBigInt(bigIntValueA, bigIntValueB) {
    if (bigIntValueB === 0n) {
      return NaN;
    }
    return Math.round(Number(bigIntValueA * BigInt(this.decimalPrecisionFactor * 10) / bigIntValueB) / 10) / this.decimalPrecisionFactor;
  }
}

module.exports = BigIntCalculator;
