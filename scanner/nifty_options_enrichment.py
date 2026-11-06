"""NIFTY options historical BSM IV and Greeks enrichment primitives.

File orchestration lives in 08_enrich_nifty_options.py. This module is
independent of the broker API and the existing DuckDB research database.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Optional, Tuple


CALCULATION_VERSION = "nifty-options-enrichment-v1.1.0"
PRICING_MODEL = "BLACK_SCHOLES_MERTON_CONTINUOUS_YIELD"
INDIA_TZ = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
SUCCESS_STATUSES = frozenset(("OK", "MISSING_VIX"))
NIFTY_LOT_SIZE = 65


class ModelPriceOutOfBounds(ValueError):
    """The observed option price violates theoretical BSM price bounds."""


class IVSolverError(ValueError):
    """A finite implied volatility could not be found within solver bounds."""


class NonFiniteResultError(ValueError):
    """A model input or computed result is not finite."""


@dataclass(frozen=True)
class ModelAssumptions:
    """Inputs that must be supplied explicitly for every real-data run."""

    risk_free_rate: float
    dividend_yield: float
    expiry_time: time
    year_fraction_basis: str
    assumptions_as_of_date: date
    assumption_mode: str = "historical"
    assumptions_provisional: bool = True
    sigma_min: float = 1e-6
    sigma_max: float = 5.0
    solver_tolerance: float = 1e-8
    solver_max_iterations: int = 100

    def validate(self) -> None:
        values = (self.risk_free_rate, self.dividend_yield, self.sigma_min,
                  self.sigma_max, self.solver_tolerance)
        if not all(math.isfinite(float(value)) for value in values):
            raise ValueError("Model assumptions must be finite numbers")
        if self.year_fraction_basis not in ("ACT/365", "ACT/365.25"):
            raise ValueError("year_fraction_basis must be ACT/365 or ACT/365.25")
        if self.assumption_mode not in ("historical", "scenario"):
            raise ValueError("assumption_mode must be historical or scenario")
        if not isinstance(self.assumptions_as_of_date, date) or isinstance(
            self.assumptions_as_of_date, datetime
        ):
            raise ValueError("assumptions_as_of_date must be an explicit calendar date")
        if self.sigma_min <= 0 or self.sigma_max <= self.sigma_min:
            raise ValueError("Require 0 < sigma_min < sigma_max")
        if self.solver_tolerance <= 0:
            raise ValueError("solver_tolerance must be positive")
        if self.solver_max_iterations <= 0:
            raise ValueError("solver_max_iterations must be positive")
        if self.expiry_time.tzinfo is not None:
            raise ValueError("expiry_time must be a naive India wall-clock time")


@dataclass(frozen=True)
class CalculationResult:
    spot_price: Optional[float]
    india_vix: Optional[float]
    implied_volatility: Optional[float]
    delta: Optional[float]
    gamma: Optional[float]
    vega: Optional[float]
    theta: Optional[float]
    spot_timestamp: Optional[datetime]
    vix_timestamp: Optional[datetime]
    time_to_expiry_years: Optional[float]
    assumptions_as_of_date: date
    assumption_timing_status: str
    scenario_only: bool
    risk_free_rate: float
    dividend_yield: float
    pricing_model: str
    calculation_status: str
    calculation_reason: str
    calculation_version: str
    spot_match_status: str
    vix_match_status: str
    assumptions_provisional: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "spot_price": self.spot_price,
            "india_vix": self.india_vix,
            "implied_volatility": self.implied_volatility,
            "delta": self.delta,
            "gamma": self.gamma,
            "vega": self.vega,
            "theta": self.theta,
            "spot_timestamp": self.spot_timestamp,
            "vix_timestamp": self.vix_timestamp,
            "time_to_expiry_years": self.time_to_expiry_years,
            "assumptions_as_of_date": self.assumptions_as_of_date,
            "assumption_timing_status": self.assumption_timing_status,
            "scenario_only": self.scenario_only,
            "risk_free_rate": self.risk_free_rate,
            "dividend_yield": self.dividend_yield,
            "pricing_model": self.pricing_model,
            "calculation_status": self.calculation_status,
            "calculation_reason": self.calculation_reason,
            "calculation_version": self.calculation_version,
            "spot_match_status": self.spot_match_status,
            "vix_match_status": self.vix_match_status,
            "assumptions_provisional": self.assumptions_provisional,
        }


def _finite_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _as_india_datetime(value: Any) -> Optional[datetime]:
    if value is None or not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=INDIA_TZ)
    return value.astimezone(INDIA_TZ)


def _expiry_as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def time_to_expiry_years(
    observation_timestamp: Any,
    expiry: Any,
    expiry_time: time,
    year_fraction_basis: str,
) -> Optional[float]:
    """Return actual/365 or actual/365.25 seconds to configured India expiry."""
    observation = _as_india_datetime(observation_timestamp)
    expiry_date = _expiry_as_date(expiry)
    if observation is None or expiry_date is None:
        return None
    if year_fraction_basis == "ACT/365":
        days_per_year = 365.0
    elif year_fraction_basis == "ACT/365.25":
        days_per_year = 365.25
    else:
        raise ValueError("year_fraction_basis must be ACT/365 or ACT/365.25")
    expiry_timestamp = datetime.combine(expiry_date, expiry_time).replace(tzinfo=INDIA_TZ)
    return (expiry_timestamp - observation).total_seconds() / (days_per_year * 86400.0)


def _normal_cdf(value: float) -> float:
    return 0.5 * math.erfc(-value / math.sqrt(2.0))


def _normal_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def bsm_price(
    option_type: str, spot: float, strike: float, time_years: float,
    risk_free_rate: float, dividend_yield: float, volatility: float,
) -> float:
    """Black–Scholes–Merton European call/put price with continuous yield."""
    if time_years <= 0 or volatility <= 0 or spot <= 0 or strike <= 0:
        raise ValueError("spot, strike, time and volatility must be positive")
    root_time = math.sqrt(time_years)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate - dividend_yield + 0.5 * volatility * volatility) * time_years
    ) / (volatility * root_time)
    d2 = d1 - volatility * root_time
    discounted_spot = spot * math.exp(-dividend_yield * time_years)
    discounted_strike = strike * math.exp(-risk_free_rate * time_years)
    kind = option_type.strip().upper()
    if kind == "CE":
        value = discounted_spot * _normal_cdf(d1) - discounted_strike * _normal_cdf(d2)
    elif kind == "PE":
        value = discounted_strike * _normal_cdf(-d2) - discounted_spot * _normal_cdf(-d1)
    else:
        raise ValueError("option_type must be CE or PE")
    if not math.isfinite(value):
        raise NonFiniteResultError("BSM price is not finite")
    return value


def bsm_price_bounds(
    option_type: str, spot: float, strike: float, time_years: float,
    risk_free_rate: float, dividend_yield: float,
) -> Tuple[float, float]:
    """Return lower and upper European BSM price bounds."""
    discounted_spot = spot * math.exp(-dividend_yield * time_years)
    discounted_strike = strike * math.exp(-risk_free_rate * time_years)
    kind = option_type.strip().upper()
    if kind == "CE":
        return max(0.0, discounted_spot - discounted_strike), discounted_spot
    if kind == "PE":
        return max(0.0, discounted_strike - discounted_spot), discounted_strike
    raise ValueError("option_type must be CE or PE")


def solve_implied_volatility(
    option_type: str, market_price: float, spot: float, strike: float,
    time_years: float, risk_free_rate: float, dividend_yield: float,
    sigma_min: float = 1e-6, sigma_max: float = 5.0,
    tolerance: float = 1e-8, max_iterations: int = 100,
) -> float:
    """Solve BSM IV with bounded bisection and no external solver."""
    inputs = (market_price, spot, strike, time_years, risk_free_rate,
              dividend_yield, sigma_min, sigma_max, tolerance)
    if not all(math.isfinite(float(value)) for value in inputs):
        raise NonFiniteResultError("IV solver received a non-finite input")
    if market_price <= 0 or spot <= 0 or strike <= 0 or time_years <= 0:
        raise ValueError("market price, spot, strike and time must be positive")
    if sigma_min <= 0 or sigma_max <= sigma_min or tolerance <= 0 or max_iterations <= 0:
        raise ValueError("invalid IV solver bounds or tolerance")

    lower_bound, upper_bound = bsm_price_bounds(
        option_type, spot, strike, time_years, risk_free_rate, dividend_yield
    )
    if not math.isfinite(lower_bound) or not math.isfinite(upper_bound):
        raise NonFiniteResultError("Theoretical option-price bounds are not finite")
    if market_price < lower_bound - tolerance or market_price > upper_bound + tolerance:
        raise ModelPriceOutOfBounds("Observed close is outside theoretical BSM price bounds")

    low = sigma_min
    high = sigma_max
    low_price = bsm_price(
        option_type, spot, strike, time_years, risk_free_rate, dividend_yield, low
    )
    high_price = bsm_price(
        option_type, spot, strike, time_years, risk_free_rate, dividend_yield, high
    )
    if abs(market_price - low_price) <= tolerance:
        return low
    if abs(market_price - high_price) <= tolerance:
        return high
    if market_price < low_price or market_price > high_price:
        raise IVSolverError("Observed close is not bracketed by configured volatility bounds")

    for _ in range(max_iterations):
        middle = (low + high) * 0.5
        middle_price = bsm_price(
            option_type, spot, strike, time_years,
            risk_free_rate, dividend_yield, middle
        )
        error = middle_price - market_price
        if abs(error) <= tolerance:
            return middle
        if error < 0:
            low = middle
        else:
            high = middle
        if high - low <= max(1e-14, tolerance * 1e-4):
            candidate = (low + high) * 0.5
            if abs(bsm_price(
                option_type, spot, strike, time_years,
                risk_free_rate, dividend_yield, candidate
            ) - market_price) <= tolerance:
                return candidate
            break
    raise IVSolverError("IV bisection did not reach the configured price tolerance")


def bsm_greeks(
    option_type: str, spot: float, strike: float, time_years: float,
    risk_free_rate: float, dividend_yield: float, volatility: float,
) -> Dict[str, float]:
    """Return Delta/Gamma per underlying unit and Theta/Vega per 65-unit lot."""
    if time_years <= 0 or volatility <= 0 or spot <= 0 or strike <= 0:
        raise ValueError("spot, strike, time and volatility must be positive")
    root_time = math.sqrt(time_years)
    d1 = (
        math.log(spot / strike)
        + (risk_free_rate - dividend_yield + 0.5 * volatility * volatility) * time_years
    ) / (volatility * root_time)
    d2 = d1 - volatility * root_time
    discounted_spot = math.exp(-dividend_yield * time_years)
    discounted_strike = math.exp(-risk_free_rate * time_years)
    density = _normal_pdf(d1)
    kind = option_type.strip().upper()
    if kind == "CE":
        delta = discounted_spot * _normal_cdf(d1)
        annual_theta = (
            -spot * discounted_spot * density * volatility / (2.0 * root_time)
            - risk_free_rate * strike * discounted_strike * _normal_cdf(d2)
            + dividend_yield * spot * discounted_spot * _normal_cdf(d1)
        )
    elif kind == "PE":
        delta = discounted_spot * (_normal_cdf(d1) - 1.0)
        annual_theta = (
            -spot * discounted_spot * density * volatility / (2.0 * root_time)
            + risk_free_rate * strike * discounted_strike * _normal_cdf(-d2)
            - dividend_yield * spot * discounted_spot * _normal_cdf(-d1)
        )
    else:
        raise ValueError("option_type must be CE or PE")
    values = {
        "delta": delta,
        "gamma": discounted_spot * density / (spot * volatility * root_time),
        "vega": spot * discounted_spot * density * root_time / 100.0 * NIFTY_LOT_SIZE,
        "theta": annual_theta / 365.0 * NIFTY_LOT_SIZE,
    }
    if not all(math.isfinite(value) for value in values.values()):
        raise NonFiniteResultError("One or more BSM Greeks are not finite")
    return values


def _empty_values() -> Dict[str, Optional[float]]:
    return {
        "implied_volatility": None, "delta": None, "gamma": None,
        "vega": None, "theta": None,
    }


def enrich_option_row(
    option: Dict[str, Any], spot_timestamp: Any, spot_close: Any,
    vix_timestamp: Any, vix_close: Any, assumptions: ModelAssumptions,
) -> CalculationResult:
    """Enrich one observation; missing VIX is non-blocking, missing spot blocks."""
    assumptions.validate()
    observation = _as_india_datetime(option.get("date"))
    candidate_spot_ts = _as_india_datetime(spot_timestamp)
    candidate_vix_ts = _as_india_datetime(vix_timestamp)
    # The file pipeline performs this equality in DuckDB too. Keep the
    # calculation function defensive so a caller cannot pass a nearby quote.
    spot_ts = candidate_spot_ts if observation is not None and candidate_spot_ts == observation else None
    vix_ts = candidate_vix_ts if observation is not None and candidate_vix_ts == observation else None
    spot_match = "MATCHED" if spot_ts is not None else "MISSING_SPOT"
    vix_match = "MATCHED" if vix_ts is not None else "MISSING_VIX"
    spot_value = _finite_number(spot_close) if spot_ts is not None else None
    vix_value = _finite_number(vix_close) if vix_ts is not None else None

    expiry = _expiry_as_date(option.get("expiry"))
    time_years = None
    if expiry is not None and observation is not None:
        time_years = time_to_expiry_years(
            observation, expiry, assumptions.expiry_time,
            assumptions.year_fraction_basis
        )
    raw_close = _finite_number(option.get("close"))
    strike = _finite_number(option.get("strike"))
    raw_option_type = option.get("option_type")
    option_type = raw_option_type.strip().upper() if isinstance(raw_option_type, str) else ""

    if observation is None:
        assumption_timing_status = "UNKNOWN_OBSERVATION_TIME"
    elif observation.date() < assumptions.assumptions_as_of_date:
        assumption_timing_status = "FUTURE_ASSUMPTION_INPUT"
    elif observation.date() == assumptions.assumptions_as_of_date:
        assumption_timing_status = "SAME_DAY_AS_OF_UNVERIFIED"
    else:
        assumption_timing_status = "AS_OF_BEFORE_OBSERVATION"
    scenario_only = assumption_timing_status != "AS_OF_BEFORE_OBSERVATION"

    issues = []
    if (
        assumption_timing_status == "FUTURE_ASSUMPTION_INPUT"
        and assumptions.assumption_mode == "historical"
    ):
        issues.append((
            "FUTURE_ASSUMPTION_INPUT",
            "Rate/yield assumption as-of date is later than this option observation",
        ))
    if spot_ts is None:
        issues.append(("MISSING_SPOT", "No exact NIFTY_50 timestamp match"))
    elif spot_value is None or spot_value <= 0:
        issues.append(("INVALID_SPOT_PRICE", "Matched NIFTY spot close is not finite and positive"))
    if raw_close is None or raw_close <= 0:
        issues.append(("INVALID_OPTION_PRICE", "Option close is not finite and positive"))
    if option.get("strike") is None:
        issues.append(("MISSING_STRIKE", "Strike metadata is missing"))
    elif strike is None or strike <= 0:
        issues.append(("INVALID_STRIKE", "Strike must be finite and positive"))
    if option.get("expiry") is None:
        issues.append(("MISSING_EXPIRY", "Expiry metadata is missing"))
    elif expiry is None:
        issues.append(("MISSING_EXPIRY", "Expiry metadata is not a valid date"))
    if option_type not in ("CE", "PE"):
        issues.append(("INVALID_OPTION_TYPE", "Option type must be CE or PE"))
    if observation is None or (expiry is not None and (time_years is None or time_years <= 0)):
        issues.append(("INVALID_TIME_TO_EXPIRY", "Observation is at or after configured expiry"))

    if issues:
        status = issues[0][0]
        reason = "; ".join(message for _, message in issues)
        values = _empty_values()
    else:
        try:
            iv = solve_implied_volatility(
                option_type, raw_close, spot_value, strike, time_years,
                assumptions.risk_free_rate, assumptions.dividend_yield,
                assumptions.sigma_min, assumptions.sigma_max,
                assumptions.solver_tolerance, assumptions.solver_max_iterations
            )
            greeks = bsm_greeks(
                option_type, spot_value, strike, time_years,
                assumptions.risk_free_rate, assumptions.dividend_yield, iv
            )
            values = {
                "implied_volatility": iv, "delta": greeks["delta"],
                "gamma": greeks["gamma"], "vega": greeks["vega"],
                "theta": greeks["theta"],
            }
            if not all(math.isfinite(value) for value in values.values()):
                raise NonFiniteResultError("IV or Greek result is not finite")
            if vix_ts is None:
                status = "MISSING_VIX"
                reason = "India VIX has no exact timestamp match; IV and Greeks were calculated"
            else:
                status, reason = "OK", ""
        except ModelPriceOutOfBounds as exc:
            status, reason, values = "PRICE_OUT_OF_MODEL_BOUNDS", str(exc), _empty_values()
        except IVSolverError as exc:
            status, reason, values = "IV_SOLVER_FAILED", str(exc), _empty_values()
        except NonFiniteResultError as exc:
            status, reason, values = "NON_FINITE_RESULT", str(exc), _empty_values()
        except (OverflowError, ValueError, ZeroDivisionError) as exc:
            status, reason, values = "NON_FINITE_RESULT", str(exc), _empty_values()

    return CalculationResult(
        spot_price=spot_value, india_vix=vix_value,
        implied_volatility=values["implied_volatility"], delta=values["delta"],
        gamma=values["gamma"], vega=values["vega"], theta=values["theta"],
        spot_timestamp=spot_ts, vix_timestamp=vix_ts,
        time_to_expiry_years=time_years,
        assumptions_as_of_date=assumptions.assumptions_as_of_date,
        assumption_timing_status=assumption_timing_status,
        scenario_only=scenario_only,
        risk_free_rate=float(assumptions.risk_free_rate),
        dividend_yield=float(assumptions.dividend_yield),
        pricing_model=PRICING_MODEL, calculation_status=status,
        calculation_reason=reason, calculation_version=CALCULATION_VERSION,
        spot_match_status=spot_match, vix_match_status=vix_match,
        assumptions_provisional=bool(assumptions.assumptions_provisional),
    )

