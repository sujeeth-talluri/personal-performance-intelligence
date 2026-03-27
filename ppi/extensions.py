from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect

db = SQLAlchemy()
csrf = CSRFProtect()

# In-memory rate limiter — swap storage_uri to Redis in production for
# multi-worker consistency: RATELIMIT_STORAGE_URI=redis://localhost:6379/0
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],          # No global limit — applied per-route only
    storage_uri="memory://",
)
