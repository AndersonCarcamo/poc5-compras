from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from services.auth_service.app.schemas import Token, TokenData, UserAuth
from services.auth_service.app.services import authenticate_user, create_access_token, verify_token,get_user_by_username
from datetime import timedelta

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_token_from_cookie(request: Request):
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No autenticado"
        )
    
    # Remover el prefijo "Bearer " si está presente
    if token.startswith("Bearer "):
        token = token[7:]
        
    return token

@router.post("/token")
async def login_for_access_token(
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends()
):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=30)
    access_token = create_access_token(
        data={"sub": user.username, "user_id": user.id},
        expires_delta=access_token_expires
    )
    
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,          # No accesible por JavaScript
        secure=False,            # Solo se envía por HTTPS
        samesite="lax",         # Protección contra CSRF
        max_age=1800            # 30 minutos en segundos
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

async def get_current_token_data(token: str = Depends(get_token_from_cookie)) -> TokenData:
    """Obtiene los datos del token actual"""
    token_data = await verify_token(token)
    if token_data is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token_data

async def get_current_user(token_data: TokenData = Depends(get_current_token_data)) -> UserAuth:
    """Obtiene el usuario actual basado en el token"""
    user = await get_user_by_username(token_data.username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario no encontrado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

@router.get("/users/me", response_model=UserAuth)
async def read_users_me(current_user: UserAuth = Depends(get_current_user)):
    return current_user

@router.post("/verify-token")
async def verify_token_endpoint(token: str = Depends(get_token_from_cookie)):
    """Endpoint para que otros servicios verifiquen tokens"""
    token_data = await verify_token(token)
    if token_data is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")
        
    return {
        "valid": True,
        "user_id": token_data.user_id,
        "username": token_data.username
    }