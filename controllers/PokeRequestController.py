import json 
import logging

from fastapi import HTTPException
from models.PokeRequest import PokemonRequest
from utils.database import execute_query_json
from utils.AQueue import AQueue, AQueueDelete
from utils.ABlob import ABlob
from fastapi.responses import JSONResponse


# configurar el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def select_pokemon_request( id: int ) -> dict:
    try:
        query = "select * from pokeqdb.requests where id = ?"
        params = (id,)
        result = await execute_query_json( query , params )
        result_dict = json.loads(result)
        return result_dict
    except Exception as e:
        logger.error( f"Error selecting report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )


async def update_pokemon_request( pokemon_request: PokemonRequest) -> dict:
    try:
        query = " exec pokeqdb.update_poke_request ?, ?, ? "
        if not pokemon_request.url:
            pokemon_request.url = "";

        params = ( pokemon_request.id, pokemon_request.status, pokemon_request.url  )
        result = await execute_query_json( query , params, True )
        result_dict = json.loads(result)
        return result_dict
    except Exception as e:
        logger.error( f"Error updating report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )


async def insert_pokemon_request( pokemon_request: PokemonRequest) -> dict:
    try:
        query = " exec pokeqdb.create_poke_request ?, ?"
        if not pokemon_request.sample_size:
            pokemon_request.sample_size=0
        params = ( pokemon_request.pokemon_type, pokemon_request.sample_size )
        result = await execute_query_json( query , params, True )
        result_dict = json.loads(result)

        await AQueue().insert_message_on_queue( result )

        return result_dict
    except Exception as e:
        logger.error( f"Error inserting report reques {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )


async def get_all_request() -> dict:
    query = """
        select 
            r.id as ReportId
            , s.description as Status
            , r.type as PokemonType
            , r.url 
            , r.created 
            , r.updated
        from pokeqdb.requests r 
        inner join pokeqdb.status s 
        on r.id_status = s.id 
    """
    result = await execute_query_json( query  )
    result_dict = json.loads(result)
    blob = ABlob()
    for record in result_dict:
        id = record['ReportId']
        record['url'] = f"{record['url']}?{blob.generate_sas(id)}"
    return result_dict


async def delete_pokemon_request( id:int):
    try:
        query_check = "SELECT id AS id FROM pokeqdb.requests WHERE id = ?"
        query_delete = "DELETE FROM pokeqdb.requests WHERE id = ?"
        params = (id,)

        # Verificar si existe el registro
        result = await execute_query_json(query_check, params)
        result_dict = json.loads(result)
        backup = {
            'id':id
        }
        print(f"akjskdnamsd+++++++++++: {result}")
        print(f"akjskdnamsd+++++++++++: {result_dict}")
        if not result_dict:
            await AQueueDelete().insert_delete_message(f"[{backup}]")
            raise HTTPException(status_code=404, detail="Report not found")

        # Eliminar el registro si existe
        await execute_query_json(query_delete, params, True)
        await AQueueDelete().insert_delete_message(result)

        # Retornar mensaje de éxito
        return {
            "message": f"Blob file poke_report_{id}.csv and DB record deleted"
        }

    except HTTPException:
        raise  # Re-lanzar excepciones HTTP si ya fueron lanzadas arriba
    except Exception as e:
        logger.error(f"Error deleting report request: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")