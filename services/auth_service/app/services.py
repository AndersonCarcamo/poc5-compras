import httpx
from jose import jwt
from datetime import datetime, timedelta
from services.auth_service.app.schemas import TokenData, UserAuth
from typing import Optional

SECRET_KEY = "tu_clave_secreta_super_segura"  # En producción, usa variables de entorno
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
USER_SERVICE_URL = "http://localhost:8001"  # URL del User Service

async def authenticate_user(username: str, password: str) -> Optional[UserAuth]:
    """Autentica un usuario consultando al User Service"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{USER_SERVICE_URL}/users/authenticate",
                json={"username": username, "password": password}
            )
            
            if response.status_code == 200:
                return UserAuth(**response.json())
            return None
    except Exception as e:
        print(f"Error al autenticar usuario: {str(e)}")
        return None

async def get_user_by_username(username: str) -> Optional[UserAuth]:
    """Obtiene información de un usuario por su username desde el User Service"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{USER_SERVICE_URL}/users/by-username/{username}")
            
            if response.status_code == 200:
                return UserAuth(**response.json())
            return None
    except Exception as e:
        print(f"Error al obtener usuario: {str(e)}")
        return None

def create_access_token(data: dict, expires_delta: timedelta = None):
    """Crea un token JWT de acceso"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def verify_token(token: str) -> Optional[TokenData]:
    """Verifica un token JWT y retorna los datos del token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        
        if username is None or user_id is None:
            return None
            
        # Verificar que el usuario exista
        user = await get_user_by_username(username)
        if user is None:
            return None
            
        return TokenData(username=username, user_id=user_id)
    except Exception:
        return None