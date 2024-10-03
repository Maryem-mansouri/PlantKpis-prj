import pandas as pd
from sqlalchemy import MetaData, Table, select, and_
from config.db import establish_connectionAuth
from flask import request  # Assurez-vous que Flask est bien importé ici
from components.ManageUsers.currentUser import get_logged_in_user_info
from sqlalchemy import update, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from dash import Dash, dcc, html, dash_table
connection, engine = establish_connectionAuth()
metadata = MetaData()
 
 
def get_users_in_same_department_but_not_current():
    # Appeler get_logged_in_user_info ici, lorsque le contexte de requête est actif
    user_id, email, user_department,role = get_logged_in_user_info()
    print(user_id, email, user_department,role)
    if not engine or user_id is None or user_department is None:
        return pd.DataFrame()
 
    # Charger les tables des utilisateurs et des rôles
    users_table = Table('user', metadata, autoload_with=engine)
    roles_table = Table('role', metadata, autoload_with=engine)
 
    # Requête pour sélectionner les utilisateurs du même département que l'utilisateur connecté, sans inclure l'utilisateur actuel
    query = select(
        users_table.c.user_id,
        users_table.c.email,
        users_table.c.business_unit,
        users_table.c.active,
        roles_table.c.name # Join with the roles table to get the role name
    ).select_from(
        users_table.join(roles_table, users_table.c.role_id == roles_table.c.id)
    ).where(
        and_(
            users_table.c.department == user_department,
            users_table.c.user_id != user_id,
        )
    )
   
    with engine.connect() as connection:
        result = connection.execute(query)
        
        df = pd.DataFrame(result.fetchall(), columns=['User_id', 'Email', 'Business Unit', 'Status', 'Role Name'])
    print(df)
    return df
 
 
 
def update_user_status(user_id, status, role_name):
    # Establish connection to the database
    connection, engine = establish_connectionAuth()
   
    if engine is None:
        return "Database connection could not be established.", True
 
    metadata = MetaData()
 
    # Load the users and roles tables
    try:
        users_table = Table('user', metadata, autoload_with=engine)
        roles_table = Table('role', metadata, autoload_with=engine)
    except Exception as e:
        return f"Error loading tables: {e}", True
 
    try:
        with engine.connect() as conn:
            # Fetch the role_id corresponding to the role_name
            role_query = select(roles_table.c.id).where(roles_table.c.name == role_name)
            role_id_result = conn.execute(role_query).fetchone()
 
            if not role_id_result:
                return f"Role '{role_name}' not found.", True
 
            role_id = role_id_result[0]
            print(f"Role ID for '{role_name}': {role_id}")
 
            try:
                # Prepare the update statement to update both status and role_id
                stmt = update(users_table).where(
                    users_table.c.user_id == user_id
                ).values(
                    active=status,
                    role_id=role_id  # Update with the correct role_id
                )
 
                # Print the statement for debugging
                print(f"Executing SQL: {stmt}")
 
                # Execute the update statement
                result = conn.execute(stmt)
                print(f"Update successful. Rows affected: {result.rowcount}")
 
                # Commit the transaction if needed
                conn.commit()
 
                return [
                    html.I(className="bi bi-database-check me-2"),  # Bootstrap icon
                    "Changes saved successfully!"
                ], True
 
            except SQLAlchemyError as e:
                return f"Error saving changes: {e}", True
 
    except SQLAlchemyError as e:
        return f"Error establishing connection: {e}", True
 
 
 
 
def delete_user_from_database(user_id):
    # Establish connection to the database
    connection, engine = establish_connectionAuth()
   
    if engine is None:
        return "Database connection could not be established.", True
 
    metadata = MetaData()
 
    # Load the users table
    try:
        users_table = Table('user', metadata, autoload_with=engine)
    except Exception as e:
        return f"Error loading users table: {e}", True
 
    try:
        with engine.connect() as conn:
            # Begin a transaction
            with conn.begin():
                stmt = users_table.delete().where(users_table.c.user_id == user_id)
                result = conn.execute(stmt)
                if result.rowcount > 0:
                    return "User deleted successfully.", False
                else:
                    return "User not found.", True
    except SQLAlchemyError as e:
        return f"Error during deletion: {e}", True