"""
Database module - Query CRUD per metadata pipelines
"""

import json
import logging
import psycopg2.extras
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)


class PipelineRepository:
    """Repository per operazioni CRUD su pipelines"""
    
    def __init__(self, lakebase_connection):
        """
        Args:
            lakebase_connection: LakebaseConnection instance
        """
        self.conn = lakebase_connection
    
    def create(self, pipeline: Dict[str, Any]) -> int:
        """
        Crea nuova pipeline
        
        Args:
            pipeline: Dict con campi (name, type, schedule, enabled, config, owner, description)
            
        Returns:
            Pipeline ID
        """
        logger.info(f"Creating pipeline: {pipeline['name']}")
        
        query = """
            INSERT INTO pipelines (name, type, schedule, enabled, config, owner, description)
            VALUES (%(name)s, %(type)s, %(schedule)s, %(enabled)s, %(config)s::jsonb, %(owner)s, %(description)s)
            RETURNING id
        """
        
        # Ensure config is JSON string
        if isinstance(pipeline.get('config'), dict):
            pipeline['config'] = json.dumps(pipeline['config'])
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, pipeline)
            pipeline_id = cursor.fetchone()[0]
        
        logger.info(f"✅ Pipeline created with ID: {pipeline_id}")
        return pipeline_id
    
    def get_by_id(self, pipeline_id: int) -> Optional[Dict]:
        """
        Recupera pipeline per ID
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline dict o None se non trovata
        """
        logger.debug(f"Fetching pipeline by ID: {pipeline_id}")
        
        query = "SELECT * FROM pipelines WHERE id = %s"
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, (pipeline_id,))
            result = cursor.fetchone()
        
        return dict(result) if result else None
    
    def get_by_name(self, name: str) -> Optional[Dict]:
        """
        Recupera pipeline per nome
        
        Args:
            name: Pipeline name
            
        Returns:
            Pipeline dict o None se non trovata
        """
        logger.debug(f"Fetching pipeline by name: {name}")
        
        query = "SELECT * FROM pipelines WHERE name = %s"
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, (name,))
            result = cursor.fetchone()
        
        return dict(result) if result else None
    
    def list_all(self, enabled_only: bool = False) -> List[Dict]:
        """
        Lista tutte le pipeline
        
        Args:
            enabled_only: Se True, ritorna solo pipeline abilitate
            
        Returns:
            Lista di pipeline dict
        """
        logger.debug(f"Listing pipelines (enabled_only={enabled_only})")
        
        query = "SELECT * FROM pipelines"
        if enabled_only:
            query += " WHERE enabled = TRUE"
        query += " ORDER BY name"
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query)
            results = cursor.fetchall()
        
        pipelines = [dict(row) for row in results]
        logger.debug(f"Found {len(pipelines)} pipelines")
        
        return pipelines
    
    def update(self, name: str, field: str, value: Any) -> bool:
        """
        Aggiorna campo di una pipeline
        
        Args:
            name: Pipeline name
            field: Campo da aggiornare
            value: Nuovo valore
            
        Returns:
            True se aggiornamento OK, False se pipeline non trovata
        """
        logger.info(f"Updating pipeline '{name}': {field} = {value}")
        
        allowed_fields = ['schedule', 'enabled', 'config', 'owner', 'description', 'type']
        if field not in allowed_fields:
            raise ValueError(f"Invalid field '{field}'. Allowed: {allowed_fields}")
        
        # Handle config JSON
        if field == 'config' and isinstance(value, dict):
            value = json.dumps(value)
        
        query = f"UPDATE pipelines SET {field} = %s WHERE name = %s"
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (value, name))
            rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            logger.info(f"✅ Pipeline '{name}' updated")
            return True
        else:
            logger.warning(f"Pipeline '{name}' not found")
            return False
    
    def enable(self, name: str) -> bool:
        """Abilita pipeline"""
        return self.update(name, 'enabled', True)
    
    def disable(self, name: str) -> bool:
        """Disabilita pipeline"""
        return self.update(name, 'enabled', False)
    
    def delete(self, name: str) -> bool:
        """
        Elimina pipeline
        
        Args:
            name: Pipeline name
            
        Returns:
            True se eliminazione OK, False se pipeline non trovata
        """
        logger.warning(f"Deleting pipeline: {name}")
        
        query = "DELETE FROM pipelines WHERE name = %s"
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (name,))
            rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            logger.info(f"✅ Pipeline '{name}' deleted")
            return True
        else:
            logger.warning(f"Pipeline '{name}' not found")
            return False
    
    def bulk_upsert(self, pipelines: List[Dict]) -> int:
        """
        Insert o update multipli (per seed da YAML)
        
        Args:
            pipelines: Lista di pipeline dict
            
        Returns:
            Numero di pipeline processate
        """
        logger.info(f"Bulk upserting {len(pipelines)} pipelines")
        
        query = """
            INSERT INTO pipelines (name, type, schedule, enabled, config, owner, description)
            VALUES (%(name)s, %(type)s, %(schedule)s, %(enabled)s, %(config)s::jsonb, %(owner)s, %(description)s)
            ON CONFLICT (name) DO UPDATE SET
                type = EXCLUDED.type,
                schedule = EXCLUDED.schedule,
                enabled = EXCLUDED.enabled,
                config = EXCLUDED.config,
                owner = EXCLUDED.owner,
                description = EXCLUDED.description
        """
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            
            for p in pipelines:
                # Ensure config is JSON string
                if isinstance(p.get('config'), dict):
                    p['config'] = json.dumps(p['config'])
                
                cursor.execute(query, p)
                logger.debug(f"  Upserted: {p['name']}")
        
        logger.info(f"✅ Bulk upsert completed: {len(pipelines)} pipelines")
        return len(pipelines)


class SchemaManager:
    """Gestisce operazioni su schema (init, reset)"""
    
    def __init__(self, lakebase_connection):
        self.conn = lakebase_connection
    
    def execute_sql_file(self, filepath: str):
        """
        Esegue file SQL
        
        Args:
            filepath: Path al file SQL
        """
        logger.info(f"Executing SQL file: {filepath}")
        
        with open(filepath, 'r') as f:
            sql = f.read()
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
        
        logger.info("✅ SQL file executed successfully")
    
    def drop_schema(self):
        """Drop schema metadata (DANGEROUS!)"""
        logger.warning("⚠️  Dropping metadata schema...")
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DROP SCHEMA IF EXISTS metadata CASCADE;")
        
        logger.info("✅ Schema dropped")
    
    def schema_exists(self) -> bool:
        """
        Verifica se schema metadata esiste
        
        Returns:
            True se esiste, False altrimenti
        """
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'metadata'
            )
        """
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            exists = cursor.fetchone()[0]
        
        logger.debug(f"Schema exists: {exists}")
        return exists
    
    def get_pipeline_count(self) -> int:
        """Ritorna numero di pipeline nel DB"""
        query = "SELECT COUNT(*) FROM pipelines"
        
        with self.conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
        
        return count