"""
Authentication module - Gestione OAuth Databricks e connessione Lakebase
"""

import os
import logging
import requests
import psycopg2
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class DatabricksAuth:
    """Gestisce autenticazione OAuth con Databricks"""
    
    def __init__(self, databricks_host: str, token_lifetime: int = 3600):
        self.databricks_host = databricks_host
        self.token_lifetime = token_lifetime
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        
        # Get Databricks token from env
        self.databricks_token = os.getenv('DATABRICKS_TOKEN')
        if not self.databricks_token:
            raise ValueError(
                "DATABRICKS_TOKEN not found in environment. "
                "Set it with: export DATABRICKS_TOKEN='dapi...'"
            )
    
    def get_oauth_token(self, force_refresh: bool = False) -> str:
        """
        Ottiene OAuth token per Lakebase
        
        Args:
            force_refresh: Forza refresh anche se token valido
            
        Returns:
            OAuth token string
        """
        # Check se token esistente è ancora valido
        if not force_refresh and self._token and self._token_expiry:
            # Refresh 5 minuti prima della scadenza
            refresh_threshold = timedelta(minutes=5)
            if datetime.now() < (self._token_expiry - refresh_threshold):
                logger.debug("Using cached OAuth token")
                return self._token
        
        logger.info("Requesting new OAuth token from Databricks...")
        
        try:
            response = requests.post(
                f"{self.databricks_host}/api/2.0/token/generate",
                headers={"Authorization": f"Bearer {self.databricks_token}"},
                json={
                    "lifetime_seconds": self.token_lifetime,
                    "comment": "Airflow Metadata Manager"
                },
                timeout=10
            )
            
            response.raise_for_status()
            
            self._token = response.json()["token_value"]
            self._token_expiry = datetime.now() + timedelta(seconds=self.token_lifetime)
            
            logger.info("✅ OAuth token obtained successfully")
            logger.debug(f"Token expires at: {self._token_expiry}")
            
            return self._token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get OAuth token: {e}")
            raise Exception(f"OAuth token request failed: {e}")
    
    def invalidate_token(self):
        """Invalida token cached (forza refresh al prossimo uso)"""
        logger.debug("Invalidating cached OAuth token")
        self._token = None
        self._token_expiry = None


class LakebaseConnection:
    """Gestisce connessione a Databricks Lakebase (PostgreSQL)"""
    
    def __init__(self, config: Dict, auth: DatabricksAuth):
        """
        Args:
            config: Lakebase config dict (host, port, database, user, schema, sslmode)
            auth: DatabricksAuth instance
        """
        self.config = config
        self.auth = auth
        
        logger.debug(f"LakebaseConnection initialized for {config['host']}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager per connessione PostgreSQL
        
        Yields:
            psycopg2.connection
            
        Example:
            with lakebase.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM pipelines")
        """
        token = self.auth.get_oauth_token()
        
        logger.debug(f"Connecting to Lakebase: {self.config['host']}")
        
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=token,
                sslmode=self.config['sslmode'],
                options=f"-c search_path={self.config['schema']},public",
                connect_timeout=10
            )
            
            logger.debug("Connection established")
            
            try:
                yield conn
                conn.commit()
                logger.debug("Transaction committed")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Transaction rolled back: {e}")
                raise
                
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise Exception(f"Failed to connect to Lakebase: {e}")
            
        finally:
            if conn:
                conn.close()
                logger.debug("Connection closed")
    
    def test_connection(self) -> bool:
        """
        Testa connessione a Lakebase
        
        Returns:
            True se connessione OK, False altrimenti
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
            logger.info("✅ Connection test successful")
            return result[0] == 1
            
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False