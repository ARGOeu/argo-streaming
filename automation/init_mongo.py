import logging

from pymongo import ASCENDING, DESCENDING, MongoClient

logger = logging.getLogger(__name__)


def init_mongo(connection_string: str, db_name: str) -> bool:
    """Initialise indexes in mongodb"""

    # Connect to MongoDB and get the database
    client = MongoClient(connection_string)
    db = client[db_name]

    # types of indexing
    index_desc_dateint_id = [("date_integer", DESCENDING), ("id", ASCENDING)]
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
    return True
