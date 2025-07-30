def not_empty(value):
    return value is not None and value != ""

def not_null(value):
    return value is not None

VALIDATION_FUNCTIONS = {
    "notEmpty": not_empty,
    "notNull": not_null
}