"""
Unit tests for Silver layer transformation logic.
These tests validate the cleaning rules WITHOUT requiring a Spark cluster.
They test pure Python logic that mirrors the Silver transformations.
"""


def normalize_country(country: str) -> str:
    """Mirrors the country standardization logic from transform_silver.py"""
    if country is None:
        return None
    country_lower = country.strip().lower()
    if country_lower in ("usa", "u.s.a.", "united states"):
        return "USA"
    elif country_lower in ("uk", "united kingdom"):
        return "UK"
    elif country_lower in ("in", "india"):
        return "India"
    else:
        return country.strip().title()


def normalize_status(status: str) -> str:
    """Mirrors the status standardization logic from transform_silver.py"""
    if status is None:
        return None
    status_lower = status.strip().lower()
    valid = ["cancelled", "delivered", "shipped", "pending", "returned"]
    if status_lower in valid:
        return status_lower
    return status_lower


def normalize_payment_method(method: str) -> str:
    """Mirrors the payment method standardization logic from transform_silver.py"""
    if method is None:
        return None
    method_lower = method.strip().lower()
    if method_lower in ("cod", "cash on delivery"):
        return "COD"
    elif method_lower == "credit card":
        return "Credit Card"
    elif method_lower == "debit card":
        return "Debit Card"
    elif method_lower == "paypal":
        return "PayPal"
    elif method_lower == "upi":
        return "UPI"
    return method.strip()


def clean_price(price_str: str) -> float:
    """Mirrors the price cleaning logic from transform_silver.py"""
    if price_str is None:
        return None
    cleaned = price_str.replace("$", "").strip()
    if cleaned.lower() in ("n/a", ""):
        return None
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def clean_age(age) -> int:
    """Mirrors the age cleaning logic from transform_silver.py"""
    if age is None:
        return None
    try:
        age_int = int(age)
        if age_int < 0 or age_int > 120:
            return None
        return age_int
    except (ValueError, TypeError):
        return None


def normalize_category(category: str) -> str:
    """Mirrors the category standardization logic from transform_silver.py"""
    if category is None or category.strip() == "":
        return None
    cat_lower = category.strip().lower()
    if "electronic" in cat_lower:
        return "Electronics"
    elif "cloth" in cat_lower:
        return "Clothing"
    elif "home" in cat_lower or "kitchen" in cat_lower:
        return "Home & Kitchen"
    elif "book" in cat_lower:
        return "Books"
    elif "toy" in cat_lower:
        return "Toys"
    elif "sport" in cat_lower:
        return "Sports"
    elif "beauty" in cat_lower:
        return "Beauty"
    elif "grocery" in cat_lower:
        return "Grocery"
    return category.strip().title()


# ────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────

class TestCountryNormalization:
    def test_usa_variants(self):
        assert normalize_country("usa") == "USA"
        assert normalize_country("U.S.A.") == "USA"
        assert normalize_country("United States") == "USA"

    def test_uk_variants(self):
        assert normalize_country("uk") == "UK"
        assert normalize_country("United Kingdom") == "UK"

    def test_india_variants(self):
        assert normalize_country("IN") == "India"
        assert normalize_country("India") == "India"

    def test_other_countries(self):
        assert normalize_country("France") == "France"
        assert normalize_country("  canada  ") == "Canada"
        assert normalize_country("germany") == "Germany"

    def test_none(self):
        assert normalize_country(None) is None


class TestStatusNormalization:
    def test_valid_statuses(self):
        assert normalize_status("SHIPPED") == "shipped"
        assert normalize_status("Delivered") == "delivered"
        assert normalize_status("pending") == "pending"
        assert normalize_status("Cancelled") == "cancelled"
        assert normalize_status("Returned") == "returned"

    def test_whitespace(self):
        assert normalize_status("  shipped  ") == "shipped"

    def test_none(self):
        assert normalize_status(None) is None


class TestPaymentMethodNormalization:
    def test_cod_variants(self):
        assert normalize_payment_method("COD") == "COD"
        assert normalize_payment_method("Cash on Delivery") == "COD"

    def test_card_types(self):
        assert normalize_payment_method("credit card") == "Credit Card"
        assert normalize_payment_method("Debit Card") == "Debit Card"

    def test_other_methods(self):
        assert normalize_payment_method("paypal") == "PayPal"
        assert normalize_payment_method("UPI") == "UPI"

    def test_none(self):
        assert normalize_payment_method(None) is None


class TestPriceCleaning:
    def test_normal_price(self):
        assert clean_price("599.00") == 599.00

    def test_dollar_sign(self):
        assert clean_price("$165.3") == 165.3

    def test_with_spaces(self):
        assert clean_price("420.78 ") == 420.78

    def test_na(self):
        assert clean_price("N/A") is None

    def test_empty(self):
        assert clean_price("") is None

    def test_negative(self):
        assert clean_price("-20.0") is None

    def test_none(self):
        assert clean_price(None) is None


class TestAgeCleaning:
    def test_valid_age(self):
        assert clean_age(30) == 30
        assert clean_age(19) == 19

    def test_negative_age(self):
        assert clean_age(-5) is None

    def test_impossible_age(self):
        assert clean_age(150) is None

    def test_none(self):
        assert clean_age(None) is None


class TestCategoryNormalization:
    def test_electronics(self):
        assert normalize_category("Electronics") == "Electronics"
        assert normalize_category("electronics") == "Electronics"
        assert normalize_category("Electronic ") == "Electronics"

    def test_clothing(self):
        assert normalize_category("Clothing") == "Clothing"
        assert normalize_category("clothing") == "Clothing"

    def test_home_kitchen(self):
        assert normalize_category("Home & Kitchen") == "Home & Kitchen"
        assert normalize_category("Home and Kitchen") == "Home & Kitchen"

    def test_books(self):
        assert normalize_category("Books") == "Books"
        assert normalize_category("BOOKS") == "Books"

    def test_empty_and_none(self):
        assert normalize_category("") is None
        assert normalize_category(None) is None
