#!/usr/bin/env python3
"""
Metadata Runner - Setup, CRUD, Reset per Lakebase
Uso: python runner.py <command> [args]
"""

import sys
import json
import yaml
import logging
from pathlib import Path

# Import utils
from utils import DatabricksAuth, LakebaseConnection, PipelineRepository, SchemaManager

# ============================================================================
# SETUP
# ============================================================================

SCRIPT_DIR = Path(__file__).parent

def load_config():
    """Carica config.yaml locale"""
    config_path = SCRIPT_DIR / "config.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# Setup logging
logging.basicConfig(
    level=getattr(logging, CONFIG['logging']['level']),
    format=CONFIG['logging']['format']
)
logger = logging.getLogger('metadata-runner')

# Initialize auth and connection
try:
    auth = DatabricksAuth(
        databricks_host=CONFIG['databricks']['host'],
        token_lifetime=CONFIG['oauth']['token_lifetime_seconds']
    )
    lakebase = LakebaseConnection(CONFIG['lakebase'], auth)
    pipeline_repo = PipelineRepository(lakebase)
    schema_manager = SchemaManager(lakebase)
    
except ValueError as e:
    logger.error(str(e))
    sys.exit(1)

# ============================================================================
# COMMANDS
# ============================================================================

def cmd_init(args):
    """Inizializza schema"""
    logger.info("Initializing metadata schema...")
    
    if schema_manager.schema_exists():
        logger.warning("Schema already exists")
        response = input("Recreate? (y/n): ")
        if response.lower() != 'y':
            logger.info("Init cancelled")
            return
        schema_manager.drop_schema()
    
    schema_file = SCRIPT_DIR / "schema.sql"
    schema_manager.execute_sql_file(str(schema_file))
    
    logger.info("✅ Schema initialized successfully")
    
    # Opzionale: carica esempi
    examples_file = SCRIPT_DIR / "examples.yaml"
    if examples_file.exists():
        response = input("\n📦 Load example pipelines? (y/n): ")
        if response.lower() == 'y':
            cmd_seed([str(examples_file)])

def cmd_reset(args):
    """Reset completo"""
    logger.warning("⚠️  RESET will DELETE ALL metadata!")
    confirm = input("Continue? (type 'yes'): ")
    
    if confirm != 'yes':
        logger.info("Reset cancelled")
        return
    
    schema_manager.drop_schema()
    logger.info("Reinitializing schema...")
    cmd_init([])

def cmd_seed(args):
    """Carica pipeline da YAML"""
    yaml_file = Path(args[0]) if args else SCRIPT_DIR / "examples.yaml"
    
    if not yaml_file.exists():
        logger.error(f"YAML file not found: {yaml_file}")
        sys.exit(1)
    
    logger.info(f"Loading pipelines from {yaml_file.name}...")
    
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f)
    
    pipelines = data.get('pipelines', [])
    
    if not pipelines:
        logger.warning("No pipelines found in YAML")
        return
    
    count = pipeline_repo.bulk_upsert(pipelines)
    logger.info(f"✅ Successfully loaded {count} pipelines")

def cmd_list(args):
    """Lista pipeline"""
    pipelines = pipeline_repo.list_all()
    
    if not pipelines:
        logger.warning("No pipelines found")
        return
    
    logger.info(f"Found {len(pipelines)} pipelines")
    
    # Print table
    print(f"\n{'ID':<5} {'Name':<30} {'Type':<12} {'Schedule':<15} {'Status':<8} {'Owner':<20}")
    print("-" * 95)
    
    for p in pipelines:
        status = "✅ ON" if p['enabled'] else "❌ OFF"
        schedule = p['schedule'] or 'manual'
        print(f"{p['id']:<5} {p['name']:<30} {p['type']:<12} {schedule:<15} {status:<8} {p['owner'] or '-':<20}")
    
    print(f"\nTotal: {len(pipelines)} pipelines\n")

def cmd_show(args):
    """Mostra dettagli pipeline"""
    if not args:
        logger.error("Missing pipeline name")
        sys.exit(1)
    
    pipeline = pipeline_repo.get_by_name(args[0])
    
    if not pipeline:
        logger.error(f"Pipeline '{args[0]}' not found")
        sys.exit(1)
    
    logger.info(f"Pipeline details: {pipeline['name']}")
    
    print(f"\n📋 Pipeline: {pipeline['name']}")
    print(f"   ID: {pipeline['id']}")
    print(f"   Type: {pipeline['type']}")
    print(f"   Schedule: {pipeline['schedule'] or 'manual'}")
    print(f"   Enabled: {'✅ YES' if pipeline['enabled'] else '❌ NO'}")
    print(f"   Owner: {pipeline['owner'] or '-'}")
    print(f"   Description: {pipeline['description'] or '-'}")
    print(f"   Config:")
    print(f"      {json.dumps(pipeline['config'], indent=6)}")
    print(f"   Created: {pipeline['created_at']}")
    print(f"   Updated: {pipeline['updated_at']}\n")

def cmd_create(args):
    """Crea pipeline"""
    if len(args) >= 3:
        pipeline = {
            'name': args[0],
            'type': args[1],
            'schedule': args[2] if args[2] != 'null' else None,
            'owner': args[3] if len(args) > 3 else None,
            'description': args[4] if len(args) > 4 else None,
            'config': json.loads(args[5]) if len(args) > 5 else {},
            'enabled': True
        }
    else:
        # Interactive
        logger.info("Interactive pipeline creation")
        print("\n📝 Create new pipeline")
        pipeline = {
            'name': input("Name: "),
            'type': input("Type (databricks/dbt/mixed): "),
            'schedule': input("Schedule (cron or 'null'): ") or None,
            'owner': input("Owner (optional): ") or None,
            'description': input("Description (optional): ") or None,
            'config': json.loads(input("Config JSON (default {}): ") or "{}"),
            'enabled': True
        }
        
        if pipeline['schedule'] == 'null':
            pipeline['schedule'] = None
    
    pipeline_id = pipeline_repo.create(pipeline)
    logger.info(f"✅ Pipeline created: {pipeline['name']} (ID: {pipeline_id})")

def cmd_update(args):
    """Aggiorna pipeline"""
    if len(args) < 3:
        logger.error("Usage: python runner.py update <name> <field> <value>")
        sys.exit(1)
    
    name, field, value = args[0], args[1], args[2]
    
    # Parse value
    if field == 'enabled':
        value = value.lower() in ['true', '1', 'yes']
    elif field == 'config':
        value = json.loads(value)
    
    if pipeline_repo.update(name, field, value):
        logger.info(f"✅ Pipeline '{name}' updated")
    else:
        logger.error(f"Pipeline '{name}' not found")
        sys.exit(1)

def cmd_enable(args):
    """Abilita pipeline"""
    if not args:
        logger.error("Missing pipeline name")
        sys.exit(1)
    
    if pipeline_repo.enable(args[0]):
        logger.info(f"✅ Pipeline '{args[0]}' enabled")
    else:
        sys.exit(1)

def cmd_disable(args):
    """Disabilita pipeline"""
    if not args:
        logger.error("Missing pipeline name")
        sys.exit(1)
    
    if pipeline_repo.disable(args[0]):
        logger.info(f"✅ Pipeline '{args[0]}' disabled")
    else:
        sys.exit(1)

def cmd_delete(args):
    """Elimina pipeline"""
    if not args:
        logger.error("Missing pipeline name")
        sys.exit(1)
    
    name = args[0]
    confirm = input(f"⚠️  Delete pipeline '{name}'? (yes/no): ")
    
    if confirm != 'yes':
        logger.info("Delete cancelled")
        return
    
    if pipeline_repo.delete(name):
        logger.info(f"✅ Pipeline '{name}' deleted")
    else:
        sys.exit(1)

def cmd_export(args):
    """Esporta in YAML"""
    output_file = args[0] if args else "pipelines_export.yaml"
    
    pipelines = pipeline_repo.list_all()
    
    export_data = {
        'pipelines': [
            {
                'name': p['name'],
                'type': p['type'],
                'schedule': p['schedule'],
                'enabled': p['enabled'],
                'config': p['config'],
                'owner': p['owner'],
                'description': p['description']
            }
            for p in pipelines
        ]
    }
    
    with open(output_file, 'w') as f:
        yaml.dump(export_data, f, default_flow_style=False, sort_keys=False)
    
    logger.info(f"✅ Exported {len(pipelines)} pipelines to {output_file}")

def cmd_test(args):
    """Testa connessione"""
    logger.info("Testing connection to Lakebase...")
    
    if lakebase.test_connection():
        count = schema_manager.get_pipeline_count()
        logger.info(f"✅ Connection OK - Found {count} pipelines")
    else:
        logger.error("❌ Connection failed")
        sys.exit(1)

def cmd_help(args):
    """Help"""
    print("""
Metadata Runner - Commands:

Setup:
  init                          Initialize schema
  reset                         Drop and recreate schema
  seed [yaml_file]              Load pipelines from YAML
  test                          Test connection

List & View:
  list                          List all pipelines
  show <name>                   Show pipeline details
  export [output.yaml]          Export pipelines to YAML

CRUD:
  create [args...]              Create pipeline
  update <name> <field> <value> Update pipeline
  enable <name>                 Enable pipeline
  disable <name>                Disable pipeline
  delete <name>                 Delete pipeline

Examples:
  python runner.py test
  python runner.py init
  python runner.py list
  python runner.py create my_etl databricks "0 2 * * *"
""")

# ============================================================================
# MAIN
# ============================================================================

COMMANDS = {
    'init': cmd_init,
    'reset': cmd_reset,
    'seed': cmd_seed,
    'list': cmd_list,
    'show': cmd_show,
    'create': cmd_create,
    'update': cmd_update,
    'enable': cmd_enable,
    'disable': cmd_disable,
    'delete': cmd_delete,
    'export': cmd_export,
    'test': cmd_test,
    'help': cmd_help,
}

def main():
    if len(sys.argv) < 2:
        cmd_help([])
        sys.exit(1)
    
    command = sys.argv[1]
    args = sys.argv[2:]
    
    if command not in COMMANDS:
        logger.error(f"Unknown command: {command}")
        cmd_help([])
        sys.exit(1)
    
    try:
        COMMANDS[command](args)
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()