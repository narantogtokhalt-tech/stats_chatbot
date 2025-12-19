# auth_utils.py
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, Any

from jose import jwt
from jwt import PyJWTError
from fastapi import Request, HTTPException

import bcrypt  # ✅ passlib биш, шууд bcrypt

JWT_SECRET = os.getenv("JWT_SECRET", "dev-change-me")
JWT_ALG = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "12"))

BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))  # 12 default

def hash_password(password: str) -> str:
    pw = password.encode("utf-8")
    salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)  # => $2b$12$...
    return bcrypt.hashpw(pw, salt).decode("utf-8")  # length ~ 60

def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(
            password.encode("utf-8"),
            password_hash.encode("utf-8"),
        )
    except Exception:
        return False

def create_access_token(*, sub: str, role: str, email: str) -> str:
    now = datetime.utcnow()
    exp = now + timedelta(hours=JWT_EXPIRE_HOURS)
    payload = {
        "sub": sub,
        "role": role,
        "email": email,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

def decode_token(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_bearer_token(request: Request) -> str:
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return auth.split(" ", 1)[1].strip()

async def require_jwt_user(request: Request) -> Dict[str, Any]:
    token = get_bearer_token(request)
    return decode_token(token)

async def require_jwt_admin(request: Request) -> Dict[str, Any]:
    payload = await require_jwt_user(request)
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return payload