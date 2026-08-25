"""pipeline orchestrator

sequences the 

run_pipeline(config):
    
    result_1  = ingestion_stage.run(config)
    result_2  = schema_validation_stage.run(config, result_1)
    result_3  = data_quality_stage.run(config, result_2)
    result_4  = processing_stage.run(config, result_3)
    result_5  = feature_engineering_stage.run(config, result_4)
    result_6  = splitting_stage.run(config, result_5)
    result_7  = training_stage.run(config, result_6)
    result_8  = evaluation_stage.run(config, result_7)
    
    
    
    result_9  = model_validation_stage.run(config, result_8)
    result_10 = registration_stage.run(config, result_9)
    result_11 = deployment_stage.run(config, result_10)
    return final_result


"""
from src.ingestion.ingestion import run_ingestion_stage, IngestionStageFailed

def get_required(config: dict, key: str):
    if key not in config:
        raise ValueError(f"Missing required config key: {key!r}")
    return config[key]

primary_url
def run_pipeline(config_path: Path, ):
    #Extacting config from config_path
    
    value_primary_url = get_required(config, "primary_url")
    
    value_backup_url = get_required(config, "backup_url")
    #calling run_ingestion_stage

    #make sure ingestionstagefailed error propagates

    #returning ingestionResult to main
