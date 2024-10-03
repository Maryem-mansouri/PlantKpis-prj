from tkinter import N
from flask import request
from jose import jwt, JWTError
 
# Secret key et algorithme utilisés pour décoder le token
SECRET_KEY = '1b284e3de1a96576a25a3f5d6c4946cd9c5a7d2e30ac838c9dc09a4976a7cf58'
ALGORITHM = "HS256"
 
def get_logged_in_user_info():
    try:
        # Récupérer le token à partir des cookies
        token = request.cookies.get('access_token')
        if not token:
            return None, None, None , None # Retourner None si le token est manquant
 
        # Décoder le token pour obtenir les informations utilisateur
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload['sub']  # User ID
        email = payload.get('email')  # Email
        department = payload.get('department')  # Role
        role=payload.get('Role')
 
        return user_id, email, department,role
   
    except JWTError as e:
        print("JWT Error:", str(e))
        return None, None, None,None  # Retourner None si le token est invalide ou expiréPP