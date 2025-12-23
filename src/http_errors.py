# http_errors.py

HTTP_ERROR_MESSAGES = {
    400: {
        "title": "400 Bad Request",
        "detail": "Validate store configuration (StoreNumber, approved orders, permissions)."
    },
    401: {
        "title": "401 Unauthorized",
        "detail": "Please validate API credentials and BaseUrl in config.ini."
    },
    403: {
        "title": "403 Forbidden",
        "detail": "Access denied. Validate API permissions for this store."
    },
    404: {
        "title": "404 Not Found",
        "detail": "API endpoint not found. Validate BaseUrl in config.ini."
    },
    500: {
        "title": "500 Server Error",
        "detail": "Upshop API internal error. Try again later or contact vendor."
    },
}
