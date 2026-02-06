import logging

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.uri_parser import parse_uri

from argo_config import ArgoConfig
from argo_web_api import ArgoWebApi

logger = logging.getLogger(__name__)


def init_mongo(
    config: ArgoConfig,
    tenant_id: str,
    tenant_name: str,
    connection_string: str,
    db_name: str,
) -> bool:
    """Initialise indexes in mongodb"""

    # Connect to MongoDB and get the database
    client: MongoClient = MongoClient(connection_string)
    db = client[db_name]

    # types of indexing
    index_desc_dateint_id = [("date_integer", DESCENDING), ("id", ASCENDING)]
    index_desc_dateint_name = [("date_integer", DESCENDING), ("name", ASCENDING)]
    index_dateint_report = [("date_integer", ASCENDING), ("report", ASCENDING)]
    index_dateint_host = [("date_integer", ASCENDING), ("host", ASCENDING)]
    index_report_dateint = [("report", ASCENDING), ("date_integer", ASCENDING)]
    index_report_date = [("report", ASCENDING), ("date", ASCENDING)]

    # list of indexes to be created - collection and type
    indexes = [
        ("aggregation_profiles", index_desc_dateint_id),
        ("downtimes", index_desc_dateint_id),
        ("endpoint_group_ar", index_report_date),
        ("metric_profiles", index_desc_dateint_id),
        ("operations_profiles", index_desc_dateint_id),
        ("service_ar", index_report_date),
        ("status_endpoint_groups", index_report_dateint),
        ("status_endpoints", index_report_dateint),
        ("status_metrics", index_report_dateint),
        ("status_metrics", index_dateint_report),
        ("status_metrics", index_dateint_host),
        ("status_services", index_report_dateint),
        ("threshold_profiles", index_desc_dateint_id),
        ("weights", index_desc_dateint_id),
        ("topology_endpoints", index_desc_dateint_id),
        ("topology_groups", index_desc_dateint_id),
        ("topology_service_types", index_desc_dateint_name),
    ]

    for collection_name, index_type in indexes:
        try:
            collection = db[collection_name]
            index_name = collection.create_index(index_type)
            logger.debug(
                f"db: {db_name}: created index {index_name} on {collection_name}"
            )
        except Exception as e:
            logger.error(
                f"db: {db_name} failed to create index on {collection_name}: {e}"
            )
            return False
    # if mongo was initialised correctly - update the status in argo-web-api tenant

    web_api = ArgoWebApi(config)
    parsed = parse_uri(connection_string)
    mongo_host, mongo_port = parsed["nodelist"][0]
    web_api.update_tenant_db_info(
        tenant_id, tenant_name, "ar", mongo_host, mongo_port, db_name
    )

    return True
